package starflow

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/emcfarlane/starlarkproto"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.starlark.net/lib/json"
	"go.starlark.net/lib/math"
	starlarktime "go.starlark.net/lib/time"
	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Thread executes a single workflow run.
type thread[Input proto.Message, Output proto.Message] struct {
	w      *Worker[Input, Output]
	run    *Run
	events []*Event
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

func (t *thread[Input, Output]) globals() (starlark.StringDict, error) {
	globals := make(starlark.StringDict)
	for name, regFn := range t.w.registry {
		globals[name] = wrapFn(t, regFn)
	}
	return globals, nil
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

	// Unmarshal the input from bytes
	var input Input
	if len(run.Input) > 0 {
		input = t.createInputInstance()
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
		}

		reqType, ok := w.types[e.FunctionName]
		if !ok {
			return zero, fmt.Errorf("unknown function: %s", e.FunctionName)
		}

		if callEvent, ok := e.AsCallEvent(); ok && callEvent.Input != nil {
			req := proto.Clone(reqType)
			if err := callEvent.Input.UnmarshalTo(req); err != nil {
				return zero, fmt.Errorf("failed to unmarshal event input: %w", err)
			}
			inputAny, _ := anypb.New(req)
			protoEvent.Metadata = CallEvent{Input: inputAny}
		}

		resType, ok := w.types[e.FunctionName+"_response"]
		if !ok {
			return zero, fmt.Errorf("unknown function: %s", e.FunctionName)
		}

		if returnEvent, ok := e.AsReturnEvent(); ok {
			if returnEvent.Error != "" {
				protoEvent.Metadata = ReturnEvent{Error: returnEvent.Error}
			} else if returnEvent.Output != nil {
				res := proto.Clone(resType)
				if err := returnEvent.Output.UnmarshalTo(res); err != nil {
					return zero, fmt.Errorf("failed to unmarshal event output: %w", err)
				}
				outputAny, _ := anypb.New(res)
				protoEvent.Metadata = ReturnEvent{Output: outputAny}
			}
		}

		protoEvents[i] = protoEvent
	}
	t.events = protoEvents

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
				maps.Copy(members, starlarktime.Module.Members)
				members["sleep"] = makeSleepBuiltin()
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

	outputBytes, err := proto.Marshal(output)
	if err != nil {
		return zero, fmt.Errorf("failed to marshal output: %w", err)
	}

	if err := t.w.store.FinishRun(ctx, t.run.ID, outputBytes); err != nil {
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

		if len(t.events) > 0 {
			// verify the next event is a call event with same args
			nextEvent := t.events[0]
			t.events = t.events[1:]
			if nextEvent.Type != EventTypeCall || nextEvent.FunctionName != regFn.name {
				return starlark.None, fmt.Errorf("expected call event, got %s", nextEvent.Type)
			}

			callEvent, ok := nextEvent.AsCallEvent()
			if !ok {
				return starlark.None, fmt.Errorf("expected call event metadata")
			}

			expectedProto := proto.Clone(regFn.reqType)
			if err := callEvent.Input.UnmarshalTo(expectedProto); err != nil {
				return starlark.None, fmt.Errorf("failed to unmarshal expected proto: %w", err)
			}

			if !proto.Equal(req, expectedProto) {
				return starlark.None, fmt.Errorf("expected input to be %v, got %v", expectedProto, req)
			}
		} else {
			inputAny, _ := anypb.New(req)
			nextEventID, err := t.w.store.RecordEvent(ctx, t.run.ID, t.run.NextEventID, &Event{
				Timestamp:    time.Now(),
				Type:         EventTypeCall,
				FunctionName: regFn.name,
				Metadata:     CallEvent{Input: inputAny},
			})
			if err != nil {
				return starlark.None, fmt.Errorf("failed to record call event: %w", err)
			}

			t.run.NextEventID = nextEventID
		}

		if len(t.events) > 0 {
			// verify the next event is a return event with same args
			nextEvent := t.events[0]
			t.events = t.events[1:]
			if nextEvent.Type != EventTypeReturn || nextEvent.FunctionName != regFn.name {
				return starlark.None, fmt.Errorf("expected return event, got %s", nextEvent.Type)
			}

			returnEvent, ok := nextEvent.AsReturnEvent()
			if !ok {
				return starlark.None, fmt.Errorf("expected return event metadata")
			}

			// re-use the output from the return event
			if returnEvent.Output != nil {
				respProto := proto.Clone(regFn.resType)
				if err := returnEvent.Output.UnmarshalTo(respProto); err != nil {
					return starlark.None, fmt.Errorf("failed to unmarshal return event output: %w", err)
				}

				return starlarkproto.MakeMessage(respProto), nil
			} else if returnEvent.Error != "" {
				return starlark.None, fmt.Errorf("function returned error: %s", returnEvent.Error)
			}
		}

		// otherwise, we need to call the function
		var resp proto.Message
		callFunc := func() error {
			ctx, span := t.w.tracer.Start(ctx, regFn.name)
			defer span.End()

			var innerErr error
			resp, innerErr = regFn.fn(ctx, req)
			if innerErr != nil {
				span.SetStatus(codes.Error, innerErr.Error())
			} else {
				span.SetStatus(codes.Ok, "success")
			}

			return innerErr
		}

		var callErr error
		if regFn.retryPolicy != nil {
			policy := backoff.WithContext(regFn.retryPolicy, starlarkCtx.ctx)
			callErr = backoff.Retry(callFunc, policy)
		} else {
			callErr = callFunc()
		}

		if callErr != nil {
			event := &Event{
				Timestamp:    time.Now(),
				Type:         EventTypeReturn,
				FunctionName: regFn.name,
				Metadata:     ReturnEvent{Error: callErr.Error()},
			}

			wrapFnSpan.SetStatus(codes.Error, callErr.Error())

			nextEventID, err := t.w.store.RecordEvent(starlarkCtx.ctx, t.run.ID, t.run.NextEventID, event)
			if err != nil {
				return starlark.None, fmt.Errorf("failed to record return event: %w", err)
			}
			t.run.NextEventID = nextEventID

			// Return the error to propagate it to the Starlark execution
			return starlark.None, callErr
		}

		// Success case
		wrapFnSpan.SetStatus(codes.Ok, "success")
		outputAny, err := anypb.New(resp)
		if err != nil {
			return starlark.None, fmt.Errorf("failed to marshal response: %w", err)
		}

		event := &Event{
			Timestamp:    time.Now(),
			Type:         EventTypeReturn,
			FunctionName: regFn.name,
			Metadata:     ReturnEvent{Output: outputAny},
		}

		nextEventID, err := t.w.store.RecordEvent(starlarkCtx.ctx, t.run.ID, t.run.NextEventID, event)
		if err != nil {
			return starlark.None, fmt.Errorf("failed to record return event: %w", err)
		}
		t.run.NextEventID = nextEventID

		return starlarkproto.MakeMessage(resp), nil
	})
}

// makeSleepBuiltin returns a starlark builtin implementing durable sleep.
func makeSleepBuiltin() *starlark.Builtin {
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

		select {
		case <-starlarkCtx.ctx.Done():
			return nil, starlarkCtx.ctx.Err()
		case <-time.After(durMsg.AsDuration()):
		}

		return starlark.None, nil
	})
}
