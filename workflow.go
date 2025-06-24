package starflow

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/emcfarlane/starlarkproto"
	"github.com/lithammer/shortuuid/v4"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.starlark.net/lib/json"
	"go.starlark.net/lib/math"
	starlarktime "go.starlark.net/lib/time"
	"go.starlark.net/starlark"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// ErrYield is returned by workflow functions to indicate that execution should pause until an external signal is received.
var ErrYield = errors.New("workflow yielded")

// yieldError wraps ErrYield with a correlation ID so it can be persisted and later signalled.
type yieldError struct {
	cid string
}

func (y yieldError) Error() string { return ErrYield.Error() }

// Is allows errors.Is(err, ErrYield) to work.
func (y yieldError) Is(target error) bool {
	return target == ErrYield
}

// CorrelationID returns the associated correlation id.
func (y yieldError) CorrelationID() string { return y.cid }

// Yield creates a new correlation id and returns an error sentinel that callers can return
// from their registered function to pause the workflow.
// Usage:
//
//	cid, err := starflow.Yield()
//	return nil, err
func Yield() (string, error) {
	cid := shortuuid.New()
	return cid, yieldError{cid: cid}
}

type registeredFn struct {
	fn          func(ctx context.Context, req proto.Message) (proto.Message, error)
	reqType     proto.Message
	resType     proto.Message
	name        string
	retryPolicy backoff.BackOff
}

// Option configures behaviour of a registered function.
type Option func(*registeredFn)

// WithName overrides the automatically derived name for the function.
func WithName(name string) Option {
	return func(rf *registeredFn) {
		rf.name = name
	}
}

// WithRetryPolicy specifies a retry policy for the function.
func WithRetryPolicy(b backoff.BackOff) Option {
	return func(rf *registeredFn) {
		rf.retryPolicy = b
	}
}

// Workflow executes and manages durable Starlark workflows.
type Workflow[Input proto.Message, Output proto.Message] struct {
	store    Store
	tracer   trace.Tracer
	registry map[string]registeredFn
	types    map[string]proto.Message
}

// New creates a new Workflow executor with the specified input and output types.
func New[Input proto.Message, Output proto.Message](store Store) *Workflow[Input, Output] {
	return &Workflow[Input, Output]{
		store:    store,
		tracer:   otel.Tracer("starflow"),
		registry: make(map[string]registeredFn),
		types:    make(map[string]proto.Message),
	}
}

// Register registers a Go function to be callable from Starlark using generics and reflection.
// The function must have the signature: func(ctx context.Context, req ReqType) (ResType, error)
// where ReqType and ResType implement proto.Message.
func Register[Input proto.Message, Output proto.Message, Req proto.Message, Res proto.Message](w *Workflow[Input, Output], fn func(ctx context.Context, req Req) (Res, error), opts ...Option) {
	// Use reflection to get function name
	funcValue := reflect.ValueOf(fn)
	funcName := runtime.FuncForPC(funcValue.Pointer()).Name()

	// Extract just the function name (remove package path)
	parts := strings.Split(funcName, ".")
	defaultName := parts[len(parts)-1]

	// Create instances to get the types (not zero values)
	var req Req
	var res Res

	// Use reflection to create actual instances
	reqType := reflect.TypeOf(req).Elem()
	resType := reflect.TypeOf(res).Elem()

	reqInstance := reflect.New(reqType).Interface().(proto.Message)
	resInstance := reflect.New(resType).Interface().(proto.Message)

	// Wrap the typed function to match the expected signature
	wrappedFn := func(ctx context.Context, reqMsg proto.Message) (proto.Message, error) {
		// Cast the proto.Message to the specific type
		typedReq, ok := reqMsg.(Req)
		if !ok {
			return nil, fmt.Errorf("invalid request type: expected %T, got %T", req, reqMsg)
		}

		return fn(ctx, typedReq)
	}

	reg := registeredFn{
		fn:      wrappedFn,
		reqType: reqInstance,
		resType: resInstance,
		name:    defaultName,
	}

	// Apply functional options
	for _, o := range opts {
		o(&reg)
	}

	finalName := reg.name
	if finalName == "" {
		finalName = defaultName
		reg.name = finalName
	}

	w.registry[finalName] = reg
	w.types[finalName] = reqInstance
	w.types[finalName+"_response"] = resInstance
}

// createInputInstance creates a new instance of the Input type using reflection
func (w *Workflow[Input, Output]) createInputInstance() Input {
	var zero Input
	inputType := reflect.TypeOf(zero).Elem()
	return reflect.New(inputType).Interface().(Input)
}

// createOutputInstance creates a new instance of the Output type using reflection
func (w *Workflow[Input, Output]) createOutputInstance() Output {
	var zero Output
	outputType := reflect.TypeOf(zero).Elem()
	return reflect.New(outputType).Interface().(Output)
}

// Run creates a new workflow run with the given script and input, returning the run ID.
func (w *Workflow[Input, Output]) Run(script []byte, input Input) (string, error) {
	// Save script
	scriptHash, err := w.store.SaveScript(script)
	if err != nil {
		return "", fmt.Errorf("failed to save script: %w", err)
	}

	// Marshal input
	var inputBytes []byte
	inputVal := reflect.ValueOf(input)
	if !inputVal.IsNil() {
		inputBytes, err = proto.Marshal(input)
		if err != nil {
			return "", fmt.Errorf("failed to marshal input: %w", err)
		}
	}

	// Create run
	runID, err := w.store.CreateRun(scriptHash, inputBytes)
	if err != nil {
		return "", fmt.Errorf("failed to create run: %w", err)
	}

	return runID, nil
}

// Resume executes or resumes a workflow run until completion, returning the output.
func (w *Workflow[Input, Output]) resumeRun(ctx context.Context, runID string) (Output, error) {
	var zero Output

	run, err := w.store.GetRun(runID)
	if err != nil {
		return zero, fmt.Errorf("failed to get run: %w", err)
	}

	script, err := w.store.GetScript(run.ScriptHash)
	if err != nil {
		return zero, fmt.Errorf("failed to get script: %w", err)
	}

	events, err := w.store.GetEvents(runID)
	if err != nil {
		return zero, fmt.Errorf("failed to get events: %w", err)
	}

	// Unmarshal the input from bytes
	var input Input
	if len(run.Input) > 0 {
		input = w.createInputInstance()
		if err := proto.Unmarshal(run.Input, input); err != nil {
			return zero, fmt.Errorf("failed to unmarshal run input: %w", err)
		}
	}

	// Convert events to have proto.Message types for workflow execution
	protoEvents := make([]*Event, len(events))
	for i, e := range events {
		protoEvent := &Event{
			Timestamp:    e.Timestamp,
			Type:         e.Type,
			FunctionName: e.FunctionName,
			Error:        e.Error,
		}

		// Unmarshal input if we have the type
		if reqType, ok := w.types[e.FunctionName]; ok && len(e.Input) > 0 {
			req := proto.Clone(reqType)
			if err := proto.Unmarshal(e.Input, req); err != nil {
				return zero, fmt.Errorf("failed to unmarshal event input: %w", err)
			}
			inputBytes, _ := proto.Marshal(req)
			protoEvent.Input = inputBytes
		} else {
			protoEvent.Input = e.Input
		}

		// Unmarshal output if we have the type
		if resType, ok := w.types[e.FunctionName+"_response"]; ok && len(e.Output) > 0 {
			res := proto.Clone(resType)
			if err := proto.Unmarshal(e.Output, res); err != nil {
				return zero, fmt.Errorf("failed to unmarshal event output: %w", err)
			}
			outputBytes, _ := proto.Marshal(res)
			protoEvent.Output = outputBytes
		} else {
			protoEvent.Output = e.Output
		}

		protoEvents[i] = protoEvent
	}

	return w.execute(ctx, runID, script, input, protoEvents)
}

func (w *Workflow[Input, Output]) execute(ctx context.Context, runID string, script []byte, input Input, resumeEvents []*Event) (Output, error) {
	var zero Output

	thread := &starlark.Thread{
		Name:  fmt.Sprintf("run-%s", runID),
		Print: func(_ *starlark.Thread, msg string) { fmt.Println(msg) },
		Load: func(thread *starlark.Thread, module string) (starlark.StringDict, error) {
			if module == "proto" {
				protoModule := starlarkproto.NewModule(protoregistry.GlobalFiles)
				return starlark.StringDict{
					"proto": protoModule,
				}, nil
			}
			if module == "time" {
				members := make(starlark.StringDict)
				maps.Copy(members, starlarktime.Module.Members)
				members["sleep"] = w.makeSleepBuiltin(runID)
				return members, nil
			}
			if module == "math" {
				return math.Module.Members, nil
			}
			if module == "json" {
				return json.Module.Members, nil
			}
			return nil, fmt.Errorf("module %q not found", module)
		},
	}

	eventHistory := w.buildEventHistory(resumeEvents)
	globals, err := w.buildGlobals(runID, eventHistory)
	if err != nil {
		_ = w.store.UpdateRunError(ctx, runID, fmt.Sprintf("failed to build globals: %v", err))
		return zero, fmt.Errorf("failed to build globals: %w", err)
	}

	// When resuming, we still execute the whole script from the beginning,
	// but the wrapped functions will short-circuit and return recorded values.
	globalsAfterExec, err := starlark.ExecFile(thread, "script", script, globals)
	if err != nil {
		if errors.Is(err, ErrYield) {
			// Yield is an expected pause, status already set to WAITING in wrapFn.
			return zero, nil
		}
		_ = w.store.UpdateRunError(ctx, runID, fmt.Sprintf("starlark execution failed: %v", err))
		return zero, fmt.Errorf("starlark execution failed: %w", err)
	}

	mainVal, ok := globalsAfterExec["main"]
	if !ok {
		_ = w.store.UpdateRunError(ctx, runID, "starlark script must have a main function")
		return zero, fmt.Errorf("starlark script must have a main function")
	}

	mainFn, ok := mainVal.(starlark.Callable)
	if !ok {
		_ = w.store.UpdateRunError(ctx, runID, "main must be a function")
		return zero, fmt.Errorf("main must be a function")
	}

	// Create starlark context value that can be passed to main
	starlarkCtx := &starlarkContext{ctx: ctx}
	starlarkInput := starlarkproto.MakeMessage(input)

	// Call main with context and input
	starlarkOutput, err := starlark.Call(thread, mainFn, starlark.Tuple{starlarkCtx, starlarkInput}, nil)
	if err != nil {
		if errors.Is(err, ErrYield) {
			// already set to waiting inside yielding function
			return zero, nil
		}
		_ = w.store.UpdateRunError(ctx, runID, fmt.Sprintf("error calling main function: %v", err))
		return zero, fmt.Errorf("error calling main function: %w", err)
	}

	output := w.createOutputInstance()
	if starlarkOutput != starlark.None {
		pm, ok := starlarkOutput.(*starlarkproto.Message)
		if !ok {
			_ = w.store.UpdateRunError(ctx, runID, fmt.Sprintf("main return value is not a proto message, got %s", starlarkOutput.Type()))
			return zero, fmt.Errorf("main return value is not a proto message, got %s", starlarkOutput.Type())
		}
		proto.Merge(output, pm.ProtoReflect().Interface())
	}

	outputBytes, err := proto.Marshal(output)
	if err != nil {
		_ = w.store.UpdateRunError(ctx, runID, fmt.Sprintf("failed to marshal output: %v", err))
		return zero, fmt.Errorf("failed to marshal output: %w", err)
	}

	if err := w.store.UpdateRunOutput(ctx, runID, outputBytes); err != nil {
		_ = w.store.UpdateRunError(ctx, runID, fmt.Sprintf("failed to update run output: %v", err))
		return zero, fmt.Errorf("failed to update run output: %w", err)
	}

	return output, nil
}

// starlarkContext wraps Go context for use in Starlark
type starlarkContext struct {
	ctx context.Context
}

func (sc *starlarkContext) String() string        { return "context" }
func (sc *starlarkContext) Type() string          { return "context" }
func (sc *starlarkContext) Freeze()               {}
func (sc *starlarkContext) Truth() starlark.Bool  { return starlark.True }
func (sc *starlarkContext) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable type: context") }

func (w *Workflow[Input, Output]) buildEventHistory(events []*Event) map[string][]*Event {
	history := make(map[string][]*Event)
	for _, e := range events {
		if e.Type == EventTypeReturn {
			history[e.FunctionName] = append(history[e.FunctionName], e)
		}
	}
	return history
}

func (w *Workflow[Input, Output]) buildGlobals(runID string, history map[string][]*Event) (starlark.StringDict, error) {
	globals := make(starlark.StringDict)
	for name, regFn := range w.registry {
		var hist []*Event
		if history != nil {
			hist = history[name]
		}
		globals[name] = w.wrapFn(runID, name, regFn, hist)
	}
	return globals, nil
}

func (w *Workflow[Input, Output]) wrapFn(runID string, name string, regFn registeredFn, history []*Event) *starlark.Builtin {
	// Maintain a cursor into the slice so subsequent calls replay sequential events.
	idx := 0

	return starlark.NewBuiltin(name, func(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		// If we have historical events remaining and the recorded event succeeded, short-circuit.
		if idx < len(history) {
			evt := history[idx]
			idx++
			if evt.Error == "" {
				if len(evt.Output) > 0 {
					if resType, ok := w.types[name+"_response"]; ok {
						res := proto.Clone(resType)
						if err := proto.Unmarshal(evt.Output, res); err == nil {
							return starlarkproto.MakeMessage(res), nil
						}
					}
				}
			}
			// If the historical event recorded an error we will fall through and invoke the function again.
		}

		var ctxVal starlark.Value
		var reqVal starlark.Value
		if err := starlark.UnpackArgs(name, args, kwargs, "ctx", &ctxVal, "req", &reqVal); err != nil {
			return nil, err
		}

		// Extract context from starlark value
		starlarkCtx, ok := ctxVal.(*starlarkContext)
		if !ok {
			return nil, fmt.Errorf("first argument must be context, got %s", ctxVal.Type())
		}

		ctx, span := w.tracer.Start(starlarkCtx.ctx, name)
		defer span.End()

		req := proto.Clone(regFn.reqType)
		if reqVal != starlark.None {
			pm, ok := reqVal.(*starlarkproto.Message)
			if !ok {
				return nil, fmt.Errorf("failed to convert starlark value to proto: expected proto message, got %s", reqVal.Type())
			}
			proto.Merge(req, pm.ProtoReflect().Interface())
		}

		inputBytes, err := proto.Marshal(req)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal call input: %w", err)
		}

		if err := w.store.RecordEvent(runID, &Event{
			Timestamp:    time.Now(),
			Type:         EventTypeCall,
			FunctionName: name,
			Input:        inputBytes,
		}); err != nil {
			return nil, fmt.Errorf("failed to record call event: %w", err)
		}

		var resp proto.Message

		callFunc := func() error {
			var innerErr error
			resp, innerErr = regFn.fn(ctx, req)

			// Do not retry on ErrYield
			if errors.Is(innerErr, ErrYield) {
				return backoff.Permanent(innerErr)
			}
			return innerErr
		}

		if regFn.retryPolicy != nil {
			policy := backoff.WithContext(regFn.retryPolicy, ctx)
			err = backoff.Retry(callFunc, policy)
		} else {
			err = callFunc()
		}

		// Handle yield sentinel separately
		var yerr interface{ CorrelationID() string }
		if errors.As(err, &yerr) {
			cid := yerr.CorrelationID()
			evt := &Event{Timestamp: time.Now(), Type: EventTypeYield, FunctionName: name, CorrelationID: cid, Input: inputBytes}
			if recErr := w.store.UpdateRunStatusAndRecordEvent(ctx, runID, RunStatusWaiting, evt, nil); recErr != nil {
				return nil, recErr
			}
			return nil, err
		}

		// Marshal response if present
		var outputBytes []byte
		if resp != nil {
			outputBytes, _ = proto.Marshal(resp)
		}

		event := &Event{
			Timestamp:    time.Now(),
			Type:         EventTypeReturn,
			FunctionName: name,
			Output:       outputBytes,
		}
		if err != nil {
			event.Error = err.Error()
		}
		if recordErr := w.store.RecordEvent(runID, event); recordErr != nil {
			if err != nil {
				return nil, fmt.Errorf("function call failed: %v; also failed to record return event: %v", err, recordErr)
			}
			return nil, fmt.Errorf("failed to record return event: %w", recordErr)
		}

		if err != nil {
			return nil, err
		}

		return starlarkproto.MakeMessage(resp), nil
	})
}

// makeSleepBuiltin returns a starlark builtin implementing durable sleep.
func (w *Workflow[Input, Output]) makeSleepBuiltin(runID string) *starlark.Builtin {
	return starlark.NewBuiltin("sleep", func(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var ctxVal starlark.Value
		var durationVal starlark.Value
		if err := starlark.UnpackArgs("sleep", args, kwargs, "ctx", &ctxVal, "duration", &durationVal); err != nil {
			return nil, err
		}

		starlarkCtx, ok := ctxVal.(*starlarkContext)
		if !ok {
			return nil, fmt.Errorf("first argument must be context")
		}

		// If this run was previously sleeping and wake time already passed, just return immediately.
		run, _ := w.store.GetRun(runID)
		if run != nil {
			if run.WakeAt != nil && time.Now().After(*run.WakeAt) {
				return starlark.None, nil
			}
		}

		// duration in milliseconds
		var durMs int64
		switch v := durationVal.(type) {
		case starlark.Int:
			if i, ok := v.Int64(); ok {
				durMs = i
			} else {
				return nil, fmt.Errorf("duration too big")
			}
		default:
			return nil, fmt.Errorf("duration must be int milliseconds")
		}

		wake := time.Now().Add(time.Duration(durMs) * time.Millisecond)

		// create yield error and get cid
		cid, yerr := Yield()

		// Persist yield event and status/wake
		evt := &Event{Timestamp: time.Now(), Type: EventTypeYield, FunctionName: "time.sleep", CorrelationID: cid}
		if recErr := w.store.UpdateRunStatusAndRecordEvent(starlarkCtx.ctx, runID, RunStatusWaiting, evt, &wake); recErr != nil {
			return nil, recErr
		}

		return nil, yerr
	})
}

// Signal delivers an external response to a previously yielded function call, identified by its correlation ID.
func (w *Workflow[Input, Output]) Signal(ctx context.Context, correlationID string, resp proto.Message) error {
	runID, yieldEvent, err := w.store.FindEventByCorrelationID(correlationID)
	if err != nil {
		return err
	}

	if yieldEvent.Type != EventTypeYield {
		return fmt.Errorf("correlation id %s is not a yield event", correlationID)
	}

	var outputBytes []byte
	if resp != nil {
		outputBytes, _ = proto.Marshal(resp)
	}

	evt := &Event{Timestamp: time.Now(), Type: EventTypeReturn, FunctionName: yieldEvent.FunctionName, CorrelationID: correlationID, Output: outputBytes}
	return w.store.UpdateRunStatusAndRecordEvent(ctx, runID, RunStatusPending, evt, nil)
}
