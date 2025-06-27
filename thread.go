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
	"go.starlark.net/lib/json"
	"go.starlark.net/lib/math"
	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/dynoinc/starflow/events"
)

type trace struct {
	runID string
	store Store

	events      []*events.Event
	nextEventID int
}

func newTrace(runID string, store Store, events []*events.Event) *trace {
	return &trace{
		runID: runID,
		store: store,

		events:      events,
		nextEventID: len(events),
	}
}

func peekEvent[ET events.EventMetadata](t *trace) (ET, bool) {
	var zero ET
	if len(t.events) == 0 {
		return zero, false
	}

	nextEvent := t.events[0]
	if nextEvent.Type() != zero.EventType() {
		return zero, false
	}

	return nextEvent.Metadata.(ET), true
}

func popEvent[ET events.EventMetadata](t *trace) (ET, bool) {
	var zero ET
	if len(t.events) == 0 {
		return zero, false
	}

	nextEvent := t.events[0]
	if nextEvent.Type() != zero.EventType() {
		return zero, false
	}

	t.events = t.events[1:]
	return nextEvent.Metadata.(ET), true
}

func recordEvent[ET events.EventMetadata](ctx context.Context, t *trace, event ET) error {
	if _, ok := popEvent[ET](t); ok {
		// TODO: verify recorded event and expected event should be the same
		return nil
	}

	if len(t.events) != 0 {
		return fmt.Errorf("trying to record event %s, but there are %d events left in the trace", event.EventType(), len(t.events))
	}

	nextEventID, err := t.store.RecordEvent(ctx, t.runID, t.nextEventID, event)
	if err != nil {
		return fmt.Errorf("failed to record event: %w", err)
	}

	t.nextEventID = nextEventID
	return nil
}

// createInputInstance creates a new instance of the Input type using reflection
func createInputInstance[Input proto.Message]() Input {
	var zero Input
	inputType := reflect.TypeOf(zero).Elem()
	return reflect.New(inputType).Interface().(Input)
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

func globals(t *trace, registry map[string]registeredFn) (starlark.StringDict, error) {
	globals := make(starlark.StringDict)

	// Group functions by module
	modules := make(map[string]starlark.StringDict)

	for name, regFn := range registry {
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
func makeSleepBuiltin(t *trace) *starlark.Builtin {
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
		if sleepEvent, ok := popEvent[events.SleepEvent](t); ok {
			sleepDuration = time.Until(sleepEvent.WakeupAt())
		} else {
			if err := recordEvent(starlarkCtx.ctx, t, events.NewSleepEvent(time.Now().Add(sleepDuration))); err != nil {
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
func makeTimeNowBuiltin(t *trace) *starlark.Builtin {
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
		if timeNowEvent, ok := popEvent[events.TimeNowEvent](t); ok {
			timestamp = timeNowEvent.Timestamp()
		} else {
			timestamp = time.Now()

			if err := recordEvent(starlarkCtx.ctx, t, events.NewTimeNowEvent(timestamp)); err != nil {
				return nil, err
			}
		}

		// Convert to google.protobuf.Timestamp
		ts := timestamppb.New(timestamp)
		return starlarkproto.MakeMessage(ts), nil
	})
}

// makeRandIntBuiltin returns a starlark builtin implementing deterministic rand.int.
func makeRandIntBuiltin(t *trace) *starlark.Builtin {
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
		if randIntEvent, ok := popEvent[events.RandIntEvent](t); ok {
			result = randIntEvent.Result()
		} else {
			result = rand.Int63n(maxInt64)

			if err := recordEvent(starlarkCtx.ctx, t, events.NewRandIntEvent(result)); err != nil {
				return nil, err
			}
		}

		return starlark.MakeInt64(result), nil
	})
}

func runThread[Input proto.Message, Output proto.Message](
	ctx context.Context,
	c *Client[Input, Output],
	runID string,
	script []byte,
	input Input,
) (Output, error) {
	var zero Output
	eventList, err := c.store.GetEvents(ctx, runID)
	if err != nil {
		return zero, fmt.Errorf("failed to get events: %w", err)
	}

	t := newTrace(runID, c.store, eventList)

	thread := &starlark.Thread{
		Name:  fmt.Sprintf("run-%s", runID),
		Print: func(_ *starlark.Thread, msg string) { fmt.Println(msg) },
		Load: func(thread *starlark.Thread, module string) (starlark.StringDict, error) {
			if module == "proto" {
				protoModule := starlarkproto.NewModule(c.protoRegistry)
				return starlark.StringDict{
					"proto": protoModule,
				}, nil
			}
			if module == "time" {
				members := make(starlark.StringDict)
				members["sleep"] = makeSleepBuiltin(t)
				members["now"] = makeTimeNowBuiltin(t)
				return members, nil
			}
			if module == "rand" {
				members := make(starlark.StringDict)
				members["int"] = makeRandIntBuiltin(t)
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

	globals, err := globals(t, c.registry)
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

	// Create starlark context value that can be passed to main
	ctxWithRunID := WithRunID(ctx, runID)
	starlarkCtx := &starlarkContext{ctx: ctxWithRunID}
	starlarkInput := starlarkproto.MakeMessage(input)

	inputAny, err := anypb.New(input)
	if err != nil {
		return zero, fmt.Errorf("failed to marshal input: %w", err)
	}

	if err := recordEvent(ctxWithRunID, t, events.NewStartEvent(runID, inputAny)); err != nil {
		return zero, fmt.Errorf("failed to record start event: %w", err)
	}

	// Call main with context and input
	starlarkOutput, err := starlark.Call(thread, mainFn, starlark.Tuple{starlarkCtx, starlarkInput}, nil)
	if err != nil {
		var yerr *YieldError
		if errors.As(err, &yerr) {
			return zero, err
		}

		if recordErr := recordEvent(ctxWithRunID, t, events.NewFinishEvent(nil, err)); recordErr != nil {
			return zero, fmt.Errorf("failed to record finish event with error: %w", recordErr)
		}

		return zero, fmt.Errorf("error calling main function: %w", err)
	}

	var output Output
	if starlarkOutput != starlark.None {
		pm, ok := starlarkOutput.(*starlarkproto.Message)
		if !ok {
			return zero, fmt.Errorf("main return value is not a proto message, got %s", starlarkOutput.Type())
		}
		output = pm.ProtoReflect().Interface().(Output)
	}

	outputAny, err := anypb.New(output)
	if err != nil {
		return zero, fmt.Errorf("failed to convert output to anypb.Any: %w", err)
	}

	if err := recordEvent(ctxWithRunID, t, events.NewFinishEvent(outputAny, nil)); err != nil {
		return zero, fmt.Errorf("failed to record finish event: %w", err)
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
func wrapFn(t *trace, regFn registeredFn) *starlark.Builtin {
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

		req := proto.Clone(regFn.reqType)
		if reqVal != starlark.None {
			pm, ok := reqVal.(*starlarkproto.Message)
			if !ok {
				return starlark.None, fmt.Errorf("failed to convert starlark value to proto: expected proto message, got %s", reqVal.Type())
			}
			proto.Merge(req, pm.ProtoReflect().Interface())
		}

		reqAny, err := anypb.New(req)
		if err != nil {
			return starlark.None, fmt.Errorf("failed to marshal request: %w", err)
		}

		if err := recordEvent(starlarkCtx.ctx, t, events.NewCallEvent(regFn.name, reqAny)); err != nil {
			return starlark.None, fmt.Errorf("failed to record call event: %w", err)
		}

		if returnEvent, ok := popEvent[events.ReturnEvent](t); ok {
			if _, retErr := returnEvent.Output(); retErr != nil {
				return starlark.None, retErr
			}

			if outputAny, _ := returnEvent.Output(); outputAny != nil {
				respProto := proto.Clone(regFn.resType)
				if err := outputAny.UnmarshalTo(respProto); err != nil {
					return starlark.None, fmt.Errorf("failed to unmarshal return event output: %w", err)
				}

				return starlarkproto.MakeMessage(respProto), nil
			}
		}

		if yieldEvent, ok := popEvent[events.YieldEvent](t); ok {
			resumeEvent, ok := popEvent[events.ResumeEvent](t)
			if !ok {
				// still waiting for the signal to resume. ideally we should have not tried to resume the run
				// but anyways, life happens.
				return starlark.None, YieldErrorFrom(yieldEvent)
			}

			if resumeEvent.Output() != nil {
				respProto := proto.Clone(regFn.resType)
				if err := resumeEvent.Output().UnmarshalTo(respProto); err != nil {
					return starlark.None, fmt.Errorf("failed to unmarshal resume event output: %w", err)
				}

				return starlarkproto.MakeMessage(respProto), nil
			}

			return starlark.None, nil
		}

		var resp proto.Message
		callFunc := func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic: %v", r)
				}
			}()

			resp, err = regFn.fn(starlarkCtx.ctx, req)
			if err != nil {
				if errors.Is(err, &YieldError{}) {
					return backoff.Permanent(err)
				}

				return err
			}

			return nil
		}

		var callErr error
		if regFn.retryPolicy != nil {
			policy := backoff.WithContext(regFn.retryPolicy, starlarkCtx.ctx)
			callErr = backoff.Retry(callFunc, policy)
		} else {
			callErr = callFunc()
		}

		var event events.EventMetadata
		var yerr *YieldError
		if errors.As(callErr, &yerr) {
			runID, _ := GetRunID(starlarkCtx.ctx)
			event = events.NewYieldEvent(yerr.cid, runID)
		} else if callErr != nil {
			event = events.NewReturnEvent(nil, callErr)
		} else {
			outputAny, err := anypb.New(resp)
			if err != nil {
				return starlark.None, fmt.Errorf("failed to marshal response: %w", err)
			}

			event = events.NewReturnEvent(outputAny, nil)
		}

		if err := recordEvent(starlarkCtx.ctx, t, event); err != nil {
			return starlark.None, err
		}

		if callErr != nil {
			return starlark.None, callErr
		}

		return starlarkproto.MakeMessage(resp), nil
	})
}
