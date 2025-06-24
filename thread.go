package starflow

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/emcfarlane/starlarkproto"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.starlark.net/lib/json"
	"go.starlark.net/lib/math"
	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Thread executes a single workflow run.
type thread[Input proto.Message, Output proto.Message] struct {
	w      *Worker[Input, Output]
	run    *Run
	events []*Event
}

func popEvent[ET EventMetadata, Input proto.Message, Output proto.Message](t *thread[Input, Output]) (ET, error, bool) {
	var zero ET
	if len(t.events) == 0 {
		return zero, nil, false
	}

	nextEvent := t.events[0]
	if nextEvent.Type != zero.EventType() {
		return zero, fmt.Errorf("expected event type %s, got %s", zero.EventType(), nextEvent.Type), false
	}

	t.events = t.events[1:]
	return nextEvent.Metadata.(ET), nil, true
}

func recordEvent[ET EventMetadata, Input proto.Message, Output proto.Message](ctx context.Context, t *thread[Input, Output], event ET) error {
	nextEventID, err := t.w.store.RecordEvent(ctx, t.run.ID, t.run.NextEventID, event)
	if err != nil {
		return fmt.Errorf("failed to record event: %w", err)
	}

	t.run.NextEventID = nextEventID
	return nil
}

// createInputInstance creates a new instance of the Input type using reflection
func (t *thread[Input, Output]) createInputInstance() Input {
	var zero Input
	inputType := reflect.TypeOf(zero).Elem()
	return reflect.New(inputType).Interface().(Input)
}

// createOutputInstance creates a new instance of the Output type using reflection
func (t *thread[Input, Output]) createOutputInstance() Output {
	var zero Output
	outputType := reflect.TypeOf(zero).Elem()
	return reflect.New(outputType).Interface().(Output)
}

// starlarkModule represents a module in Starlark
type starlarkModule struct {
	name    string
	members starlark.StringDict
}

func (m *starlarkModule) String() string        { return fmt.Sprintf("<module %s>", m.name) }
func (m *starlarkModule) Type() string          { return "module" }
func (m *starlarkModule) Freeze()               { m.members.Freeze() }
func (m *starlarkModule) Truth() starlark.Bool  { return starlark.True }
func (m *starlarkModule) Hash() (uint32, error) { return 0, fmt.Errorf("unhashable type: module") }

// Attr returns the value of the specified attribute
func (m *starlarkModule) Attr(name string) (starlark.Value, error) {
	if value, ok := m.members[name]; ok {
		return value, nil
	}
	return nil, nil // Return nil for missing attributes
}

// AttrNames returns the names of all attributes
func (m *starlarkModule) AttrNames() []string {
	names := make([]string, 0, len(m.members))
	for name := range m.members {
		names = append(names, name)
	}
	return names
}

func (t *thread[Input, Output]) globals() (starlark.StringDict, error) {
	globals := make(starlark.StringDict)

	// Group functions by module
	modules := make(map[string]starlark.StringDict)

	for name, regFn := range t.w.registry {
		parts := strings.Split(name, ".")
		moduleName := parts[0]
		funcName := parts[1]

		if modules[moduleName] == nil {
			modules[moduleName] = make(starlark.StringDict)
		}
		modules[moduleName][funcName] = wrapFn(t, regFn)
	}

	// Add modules to globals
	for moduleName, moduleDict := range modules {
		moduleDict.Freeze()
		globals[moduleName] = &starlarkModule{name: moduleName, members: moduleDict}
	}

	return globals, nil
}

// makeSleepBuiltin returns a starlark builtin implementing durable sleep.
func (t *thread[Input, Output]) makeSleepBuiltin() *starlark.Builtin {
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

		// duration must be a google.protobuf.Duration proto message
		durVal, ok := durationVal.(*starlarkproto.Message)
		if !ok {
			return nil, fmt.Errorf("duration must be google.protobuf.Duration proto message")
		}

		durMsg, ok := durVal.ProtoReflect().Interface().(*durationpb.Duration)
		if !ok {
			return nil, fmt.Errorf("duration must be google.protobuf.Duration")
		}

		sleepDuration := durMsg.AsDuration()
		if sleepEvent, err, ok := popEvent[SleepEvent](t); ok {
			if err != nil {
				return nil, err
			}

			sleepDuration = time.Until(sleepEvent.WakeupAt)
		} else {
			if err := recordEvent(starlarkCtx.ctx, t, SleepEvent{WakeupAt: time.Now().Add(sleepDuration)}); err != nil {
				return nil, err
			}
		}

		select {
		case <-starlarkCtx.ctx.Done():
			return nil, starlarkCtx.ctx.Err()
		case <-time.After(sleepDuration):
		}

		return starlark.None, nil
	})
}

// makeTimeNowBuiltin returns a starlark builtin implementing deterministic time.now.
func (t *thread[Input, Output]) makeTimeNowBuiltin() *starlark.Builtin {
	return starlark.NewBuiltin("now", func(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var ctxVal starlark.Value
		if err := starlark.UnpackArgs("now", args, kwargs, "ctx", &ctxVal); err != nil {
			return nil, err
		}

		starlarkCtx, ok := ctxVal.(*starlarkContext)
		if !ok {
			return nil, fmt.Errorf("first argument must be context")
		}

		var timestamp time.Time
		if timeNowEvent, err, ok := popEvent[TimeNowEvent](t); ok {
			if err != nil {
				return nil, err
			}

			timestamp = timeNowEvent.Timestamp
		} else {
			// record a time_now event
			timestamp = time.Now()
			if err := recordEvent(starlarkCtx.ctx, t, TimeNowEvent{Timestamp: timestamp}); err != nil {
				return nil, err
			}
		}

		// Convert to google.protobuf.Timestamp
		ts := timestamppb.New(timestamp)
		return starlarkproto.MakeMessage(ts), nil
	})
}

// makeRandIntBuiltin returns a starlark builtin implementing deterministic rand.int.
func (t *thread[Input, Output]) makeRandIntBuiltin() *starlark.Builtin {
	return starlark.NewBuiltin("int", func(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var ctxVal starlark.Value
		var maxVal starlark.Value
		if err := starlark.UnpackArgs("int", args, kwargs, "ctx", &ctxVal, "max", &maxVal); err != nil {
			return nil, err
		}

		starlarkCtx, ok := ctxVal.(*starlarkContext)
		if !ok {
			return nil, fmt.Errorf("first argument must be context")
		}

		max, ok := maxVal.(starlark.Int)
		if !ok {
			return nil, fmt.Errorf("max must be an integer")
		}

		maxInt64, ok := max.Int64()
		if !ok {
			return nil, fmt.Errorf("max value too large")
		}

		var result int64
		if randIntEvent, err, ok := popEvent[RandIntEvent](t); ok {
			if err != nil {
				return nil, err
			}

			if randIntEvent.Max != maxInt64 {
				return nil, fmt.Errorf("expected max value %d, got %d", randIntEvent.Max, maxInt64)
			}

			result = randIntEvent.Result
		} else {
			// record a rand_int event
			result = rand.Int63n(maxInt64)
			if err := recordEvent(starlarkCtx.ctx, t, RandIntEvent{Max: maxInt64, Result: result}); err != nil {
				return nil, err
			}
		}

		return starlark.MakeInt64(result), nil
	})
}

func runThread[Input proto.Message, Output proto.Message](
	ctx context.Context,
	w *Worker[Input, Output],
	run *Run,
) (Output, error) {
	t := &thread[Input, Output]{w: w, run: run}

	var zero Output
	script, err := w.store.GetScript(ctx, run.ScriptHash)
	if err != nil {
		return zero, fmt.Errorf("failed to get script: %w", err)
	}

	events, err := w.store.GetEvents(ctx, run.ID)
	if err != nil {
		return zero, fmt.Errorf("failed to get events: %w", err)
	}
	t.events = events

	// Unmarshal the input from anypb.Any
	var input Input
	if run.Input != nil {
		input = t.createInputInstance()
		if err := run.Input.UnmarshalTo(input); err != nil {
			return zero, fmt.Errorf("failed to unmarshal run input: %w", err)
		}
	}

	thread := &starlark.Thread{
		Name:  fmt.Sprintf("run-%s-%s", t.w.workerID, run.ID),
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
				members["sleep"] = t.makeSleepBuiltin()
				members["now"] = t.makeTimeNowBuiltin()
				return members, nil
			}
			if module == "rand" {
				members := make(starlark.StringDict)
				members["int"] = t.makeRandIntBuiltin()
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

	globals, err := t.globals()
	if err != nil {
		return zero, fmt.Errorf("failed to get globals: %w", err)
	}
	globalsAfterExec, err := starlark.ExecFileOptions(&syntax.FileOptions{}, thread, "script", script, globals)
	if err != nil {
		return zero, fmt.Errorf("starlark execution failed: %w", err)
	}

	mainVal, ok := globalsAfterExec["main"]
	if !ok {
		return zero, fmt.Errorf("starlark script must have a main function")
	}

	mainFn, ok := mainVal.(starlark.Callable)
	if !ok {
		return zero, fmt.Errorf("main must be a function")
	}

	// Start a span for the main function
	ctx, span := t.w.tracer.Start(ctx, "main", trace.WithAttributes(attribute.String("run_id", t.run.ID)))
	defer span.End()

	// Create starlark context value that can be passed to main
	starlarkCtx := &starlarkContext{ctx: ctx}
	starlarkInput := starlarkproto.MakeMessage(input)

	// Call main with context and input
	starlarkOutput, err := starlark.Call(thread, mainFn, starlark.Tuple{starlarkCtx, starlarkInput}, nil)
	if err != nil {
		return zero, fmt.Errorf("error calling main function: %w", err)
	}

	output := t.createOutputInstance()
	if starlarkOutput != starlark.None {
		pm, ok := starlarkOutput.(*starlarkproto.Message)
		if !ok {
			return zero, fmt.Errorf("main return value is not a proto message, got %s", starlarkOutput.Type())
		}
		proto.Merge(output, pm.ProtoReflect().Interface())
	}

	outputAny, err := anypb.New(output)
	if err != nil {
		return zero, fmt.Errorf("failed to convert output to anypb.Any: %w", err)
	}

	if err := t.w.store.FinishRun(ctx, t.run.ID, outputAny); err != nil {
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

// wrapFn wraps a Go function to be callable from Starlark.
func wrapFn[Input proto.Message, Output proto.Message](t *thread[Input, Output], regFn registeredFn) *starlark.Builtin {
	return starlark.NewBuiltin(regFn.name, func(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var ctxVal starlark.Value
		var reqVal starlark.Value
		if err := starlark.UnpackArgs(regFn.name, args, kwargs, "ctx", &ctxVal, "req", &reqVal); err != nil {
			return starlark.None, err
		}

		// Extract context from starlark value
		starlarkCtx, ok := ctxVal.(*starlarkContext)
		if !ok {
			return starlark.None, fmt.Errorf("first argument must be context, got %s", ctxVal.Type())
		}

		ctx, wrapFnSpan := t.w.tracer.Start(starlarkCtx.ctx, regFn.name+".wrapFn")
		defer wrapFnSpan.End()

		req := proto.Clone(regFn.reqType)
		if reqVal != starlark.None {
			pm, ok := reqVal.(*starlarkproto.Message)
			if !ok {
				return starlark.None, fmt.Errorf("failed to convert starlark value to proto: expected proto message, got %s", reqVal.Type())
			}
			proto.Merge(req, pm.ProtoReflect().Interface())
		}

		if callEvent, err, ok := popEvent[CallEvent](t); ok {
			if err != nil {
				return starlark.None, err
			}

			if callEvent.FunctionName != regFn.name {
				return starlark.None, fmt.Errorf("expected function name %s, got %s", regFn.name, callEvent.FunctionName)
			}

			expectedProto := proto.Clone(regFn.reqType)
			if err := callEvent.Input.UnmarshalTo(expectedProto); err != nil {
				return starlark.None, fmt.Errorf("failed to unmarshal expected proto: %w", err)
			}

			if !proto.Equal(req, expectedProto) {
				return starlark.None, fmt.Errorf("expected input to be %v, got %v", expectedProto, req)
			}
		} else {
			inputAny, err := anypb.New(req)
			if err != nil {
				return starlark.None, fmt.Errorf("failed to marshal request: %w", err)
			}

			if err := recordEvent(starlarkCtx.ctx, t, CallEvent{FunctionName: regFn.name, Input: inputAny}); err != nil {
				return starlark.None, err
			}
		}

		if returnEvent, err, ok := popEvent[ReturnEvent](t); ok {
			if err != nil {
				return starlark.None, err
			}

			if returnEvent.Error != nil {
				return starlark.None, returnEvent.Error
			}

			if returnEvent.Output != nil {
				respProto := proto.Clone(regFn.resType)
				if err := returnEvent.Output.UnmarshalTo(respProto); err != nil {
					return starlark.None, fmt.Errorf("failed to unmarshal return event output: %w", err)
				}

				return starlarkproto.MakeMessage(respProto), nil
			} else {
				return starlark.None, nil
			}
		}

		if yieldEvent, err, ok := popEvent[YieldEvent](t); ok {
			if err != nil {
				return starlark.None, err
			}

			resumeEvent, err, ok := popEvent[ResumeEvent](t)
			if !ok {
				// still waiting for the signal to resume. ideally we should have not tried to resume the run
				// but anyways, life happens.
				return starlark.None, &YieldError{cid: yieldEvent.SignalID}
			}
			if err != nil {
				return starlark.None, err
			}

			if resumeEvent.SignalID != yieldEvent.SignalID {
				return starlark.None, fmt.Errorf("expected signal id %s, got %s", yieldEvent.SignalID, resumeEvent.SignalID)
			}

			if resumeEvent.Output != nil {
				respProto := proto.Clone(regFn.resType)
				if err := resumeEvent.Output.UnmarshalTo(respProto); err != nil {
					return starlark.None, fmt.Errorf("failed to unmarshal resume event output: %w", err)
				}

				return starlarkproto.MakeMessage(respProto), nil
			} else {
				return starlark.None, nil
			}
		}

		var resp proto.Message
		callFunc := func() error {
			ctx, span := t.w.tracer.Start(ctx, regFn.name)
			defer span.End()

			var innerErr error
			resp, innerErr = regFn.fn(ctx, req)
			if innerErr != nil {
				span.SetStatus(codes.Error, innerErr.Error())
				if errors.Is(innerErr, &YieldError{}) {
					return backoff.Permanent(innerErr)
				}

				return innerErr
			}

			span.SetStatus(codes.Ok, "success")
			return nil
		}

		var callErr error
		if regFn.retryPolicy != nil {
			policy := backoff.WithContext(regFn.retryPolicy, starlarkCtx.ctx)
			callErr = backoff.Retry(callFunc, policy)
		} else {
			callErr = callFunc()
		}

		var event EventMetadata
		var yerr *YieldError
		if errors.As(callErr, &yerr) {
			wrapFnSpan.SetStatus(codes.Error, yerr.Error())

			event = YieldEvent{SignalID: yerr.cid}
		} else if callErr != nil {
			wrapFnSpan.SetStatus(codes.Error, callErr.Error())

			event = ReturnEvent{Error: callErr}
		} else {
			wrapFnSpan.SetStatus(codes.Ok, "success")
			outputAny, err := anypb.New(resp)
			if err != nil {
				return starlark.None, fmt.Errorf("failed to marshal response: %w", err)
			}

			event = ReturnEvent{Output: outputAny}
		}

		if err := recordEvent(starlarkCtx.ctx, t, event); err != nil {
			return starlark.None, err
		}

		return starlarkproto.MakeMessage(resp), callErr
	})
}
