package starflow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/emcfarlane/starlarkproto"
	"github.com/google/uuid"
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

type registeredFn struct {
	fn      func(ctx context.Context, req proto.Message) (proto.Message, error)
	reqType proto.Message
	resType proto.Message
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
func Register[Input proto.Message, Output proto.Message, Req proto.Message, Res proto.Message](w *Workflow[Input, Output], fn func(ctx context.Context, req Req) (Res, error)) {
	// Use reflection to get function name
	funcValue := reflect.ValueOf(fn)
	funcName := runtime.FuncForPC(funcValue.Pointer()).Name()

	// Extract just the function name (remove package path)
	parts := strings.Split(funcName, ".")
	name := parts[len(parts)-1]

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

	w.registry[name] = registeredFn{fn: wrappedFn, reqType: reqInstance, resType: resInstance}
	w.types[name] = reqInstance
	w.types[name+"_response"] = resInstance
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
				return starlarktime.Module.Members, nil
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
	globals, err := w.buildGlobals(ctx, runID, eventHistory)
	if err != nil {
		w.store.UpdateRunStatus(ctx, runID, RunStatusFailed)
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
		w.store.UpdateRunStatus(ctx, runID, RunStatusFailed)
		return zero, fmt.Errorf("starlark execution failed: %w", err)
	}

	mainVal, ok := globalsAfterExec["main"]
	if !ok {
		w.store.UpdateRunStatus(ctx, runID, RunStatusFailed)
		return zero, fmt.Errorf("starlark script must have a main function")
	}

	mainFn, ok := mainVal.(starlark.Callable)
	if !ok {
		w.store.UpdateRunStatus(ctx, runID, RunStatusFailed)
		return zero, fmt.Errorf("main must be a function")
	}

	// Create starlark context value that can be passed to main
	starlarkCtx := &starlarkContext{ctx: ctx}
	starlarkInput := starlarkproto.MakeMessage(input)

	// Call main with context and input
	starlarkOutput, err := starlark.Call(thread, mainFn, starlark.Tuple{starlarkCtx, starlarkInput}, nil)
	if err != nil {
		w.store.UpdateRunStatus(ctx, runID, RunStatusFailed)
		return zero, fmt.Errorf("error calling main function: %w", err)
	}

	output := w.createOutputInstance()
	if starlarkOutput != starlark.None {
		pm, ok := starlarkOutput.(*starlarkproto.Message)
		if !ok {
			w.store.UpdateRunStatus(ctx, runID, RunStatusFailed)
			return zero, fmt.Errorf("main return value is not a proto message, got %s", starlarkOutput.Type())
		}
		proto.Merge(output, pm.ProtoReflect().Interface())
	}

	outputBytes, err := proto.Marshal(output)
	if err != nil {
		w.store.UpdateRunStatus(ctx, runID, RunStatusFailed)
		return zero, fmt.Errorf("failed to marshal output: %w", err)
	}

	if err := w.store.UpdateRunOutput(ctx, runID, outputBytes); err != nil {
		w.store.UpdateRunStatus(ctx, runID, RunStatusFailed)
		return zero, fmt.Errorf("failed to update run output: %w", err)
	}

	if err := w.store.UpdateRunStatus(ctx, runID, RunStatusCompleted); err != nil {
		return zero, fmt.Errorf("failed to update run status: %w", err)
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

func (w *Workflow[Input, Output]) buildEventHistory(events []*Event) map[string]*Event {
	history := make(map[string]*Event)
	if events == nil {
		return history
	}
	// This is simplified. A real implementation needs to handle multiple calls to the same function.
	// We'll use the function name as the key for now.
	for _, e := range events {
		if e.Type == EventTypeReturn {
			// Convert byte events to proto events for workflow execution
			protoEvent := &Event{
				Timestamp:    e.Timestamp,
				Type:         e.Type,
				FunctionName: e.FunctionName,
				Error:        e.Error,
			}

			// Unmarshal output if we have the type and it's not an error
			if e.Error == "" {
				if resType, ok := w.types[e.FunctionName+"_response"]; ok && len(e.Output) > 0 {
					res := proto.Clone(resType)
					if err := proto.Unmarshal(e.Output, res); err == nil {
						protoEvent.Output = e.Output // Keep as bytes but unmarshaled for use
					}
				}
			}

			history[e.FunctionName] = protoEvent
		}
	}
	return history
}

func (w *Workflow[Input, Output]) buildGlobals(ctx context.Context, runID string, history map[string]*Event) (starlark.StringDict, error) {
	globals := make(starlark.StringDict)
	for name, regFn := range w.registry {
		var historicalEvent *Event
		if history != nil {
			historicalEvent = history[name]
		}
		globals[name] = w.wrapFn(runID, name, regFn, historicalEvent)
	}
	return globals, nil
}

func (w *Workflow[Input, Output]) wrapFn(runID string, name string, regFn registeredFn, history *Event) *starlark.Builtin {
	return starlark.NewBuiltin(name, func(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		if history != nil && history.Error == "" {
			// Successful event in history, short-circuit and return the recorded value.
			if len(history.Output) > 0 {
				if resType, ok := w.types[name+"_response"]; ok {
					res := proto.Clone(resType)
					if err := proto.Unmarshal(history.Output, res); err == nil {
						return starlarkproto.MakeMessage(res), nil
					}
				}
			}
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

		resp, err := regFn.fn(ctx, req)

		// Handle yield sentinel separately
		if errors.Is(err, ErrYield) {
			cid := uuid.New().String()[:8]
			if recErr := w.store.RecordEvent(runID, &Event{
				Timestamp:     time.Now(),
				Type:          EventTypeYield,
				FunctionName:  name,
				CorrelationID: cid,
				Input:         inputBytes,
			}); recErr != nil {
				return nil, fmt.Errorf("failed to record yield event: %w", recErr)
			}
			// Set status to waiting
			if errStatus := w.store.UpdateRunStatus(ctx, runID, RunStatusWaiting); errStatus != nil {
				return nil, errStatus
			}
			return nil, ErrYield
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

// Signal delivers an external response to a previously yielded function call, identified by its correlation ID.
func (w *Workflow[Input, Output]) Signal(ctx context.Context, runID string, correlationID string, resp proto.Message) error {
	// Fetch the corresponding yield event to validate existence
	yieldEvent, err := w.store.GetEventByCorrelationID(runID, correlationID)
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

	if err := w.store.RecordEvent(runID, &Event{
		Timestamp:     time.Now(),
		Type:          EventTypeReturn,
		FunctionName:  yieldEvent.FunctionName,
		CorrelationID: correlationID,
		Output:        outputBytes,
	}); err != nil {
		return err
	}

	// Mark the run as ready to continue
	return w.store.UpdateRunStatus(ctx, runID, RunStatusPending)
}
