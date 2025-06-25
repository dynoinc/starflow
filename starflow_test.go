package starflow_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/dynoinc/starflow"
	"github.com/dynoinc/starflow/events"
	"github.com/dynoinc/starflow/suite"
	testpb "github.com/dynoinc/starflow/suite/proto"
)

func TestInMemoryStore(t *testing.T) {
	suite.RunStoreSuite(t, func(t *testing.T) starflow.Store {
		return starflow.NewInMemoryStore()
	})
}

func TestWorkflow(t *testing.T) {
	store := starflow.NewInMemoryStore()

	wf := starflow.NewWorker[*testpb.PingRequest, *testpb.PingResponse](store, 10*time.Millisecond)
	wf.RegisterProto(testpb.File_suite_proto_ping_proto)

	pingFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		return &testpb.PingResponse{Message: "pong: " + req.Message}, nil
	}
	starflow.Register(wf, pingFn, starflow.WithName("starflow_test.pingFn"))

	script := `
load("proto", "proto")

def main(ctx, input):
	ping_proto = proto.file("suite/proto/ping.proto")
	return starflow_test.pingFn(ctx=ctx, req=ping_proto.PingRequest(message=input.message))
`

	client := starflow.NewClient[*testpb.PingRequest](store)
	runID, err := client.Run(t.Context(), []byte(script), &testpb.PingRequest{Message: "hello"})
	require.NoError(t, err)

	wf.ProcessOnce(t.Context())

	run, err := client.GetRun(t.Context(), runID)
	require.NoError(t, err)
	require.Equal(t, starflow.RunStatusCompleted, run.Status)

	runEvents, err := client.GetEvents(t.Context(), runID)
	require.NoError(t, err)
	require.Len(t, runEvents, 4)
	require.Equal(t, events.EventTypeClaim, runEvents[0].Type())
	require.Equal(t, events.EventTypeCall, runEvents[1].Type())
	if callEvent, ok := runEvents[1].Metadata.(events.CallEvent); ok {
		require.Equal(t, "starflow_test.pingFn", callEvent.FunctionName())
	}
	require.Equal(t, events.EventTypeReturn, runEvents[2].Type())
	if returnEvent, ok := runEvents[2].Metadata.(events.ReturnEvent); ok {
		_, err := returnEvent.Output()
		require.Empty(t, err)
	}
	require.Equal(t, events.EventTypeFinish, runEvents[3].Type())

	var outputResp testpb.PingResponse
	require.NoError(t, run.Output.UnmarshalTo(&outputResp))
	require.Equal(t, "pong: hello", outputResp.Message)
}

func TestWorkflow_ReplaySupport(t *testing.T) {
	store := starflow.NewInMemoryStore()

	wf := starflow.NewWorker[*testpb.PingRequest, *testpb.PingResponse](store, 10*time.Millisecond)
	wf.RegisterProto(testpb.File_suite_proto_ping_proto)

	httpCallFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		return &testpb.PingResponse{Message: "HTTP response simulated"}, nil
	}
	dbQueryFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		return &testpb.PingResponse{Message: "DB result for: " + req.Message}, nil
	}

	starflow.Register(wf, httpCallFn, starflow.WithName("starflow_test.httpCallFn"))
	starflow.Register(wf, dbQueryFn, starflow.WithName("starflow_test.dbQueryFn"))

	script := `
load("proto", "proto")

def main(ctx, input):
	print("Starting workflow with input:", input.message)
	
	# Load the proto file to access message types
	ping_proto = proto.file("suite/proto/ping.proto")
	
	# Make an HTTP call
	http_req = ping_proto.PingRequest(message="http_" + input.message)
	http_resp = starflow_test.httpCallFn(ctx=ctx, req=http_req)
	print("HTTP response:", http_resp.message)
	
	# Query the database
	db_req = ping_proto.PingRequest(message="db_" + input.message)
	db_resp = starflow_test.dbQueryFn(ctx=ctx, req=db_req)
	print("DB response:", db_resp.message)
	
	# Return final result
	return ping_proto.PingResponse(message="Completed: " + http_resp.message + " + " + db_resp.message)
`

	client := starflow.NewClient[*testpb.PingRequest](store)
	runID, err := client.Run(t.Context(), []byte(script), &testpb.PingRequest{Message: "example"})
	require.NoError(t, err)

	wf.ProcessOnce(t.Context())

	// Fetch run output
	run, err := client.GetRun(t.Context(), runID)
	require.NoError(t, err)
	var outputResp testpb.PingResponse
	require.NoError(t, run.Output.UnmarshalTo(&outputResp))

	expectedMessage := "Completed: HTTP response simulated + DB result for: db_example"
	require.Equal(t, expectedMessage, outputResp.Message)

	require.Equal(t, starflow.RunStatusCompleted, run.Status)

	runEvents, err := client.GetEvents(t.Context(), runID)
	require.NoError(t, err)

	require.Equal(t, 6, len(runEvents))

	expectedFunctions := []string{"", "starflow_test.httpCallFn", "starflow_test.httpCallFn", "starflow_test.dbQueryFn", "starflow_test.dbQueryFn", ""}
	expectedTypes := []events.EventType{
		events.EventTypeClaim,
		events.EventTypeCall, events.EventTypeReturn,
		events.EventTypeCall, events.EventTypeReturn,
		events.EventTypeFinish,
	}

	for i, event := range runEvents {
		require.Equal(t, expectedTypes[i], event.Type(), "event %d type mismatch", i)
		expectedFunc := expectedFunctions[i]
		if callEvent, ok := event.Metadata.(events.CallEvent); ok {
			require.Equal(t, expectedFunc, callEvent.FunctionName(), "event %d function name mismatch", i)
		}
	}

	t.Log("✅ Workflow completed successfully!")
	t.Logf("Run ID: %s", runID)
	t.Logf("Output: %s", outputResp.Message)
	t.Logf("Events recorded: %d", len(runEvents))
}

func TestWorkflow_StarlarkMathImport(t *testing.T) {
	store := starflow.NewInMemoryStore()

	wf := starflow.NewWorker[*testpb.PingRequest, *testpb.PingResponse](store, 10*time.Millisecond)
	wf.RegisterProto(testpb.File_suite_proto_ping_proto)

	pingFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		return &testpb.PingResponse{Message: "pong: " + req.Message}, nil
	}
	starflow.Register(wf, pingFn, starflow.WithName("starflow_test.pingFn"))

	script := `
load("proto", "proto")
load("math", "sqrt")

def main(ctx, input):
    # Use math.sqrt to compute the square root of 16
    result = sqrt(16)
    return proto.file("suite/proto/ping.proto").PingResponse(message=str(result))
`

	client := starflow.NewClient[*testpb.PingRequest](store)
	runID, err := client.Run(t.Context(), []byte(script), &testpb.PingRequest{Message: "test"})
	require.NoError(t, err)

	worker := starflow.NewWorker[*testpb.PingRequest, *testpb.PingResponse](store, 10*time.Millisecond)
	worker.ProcessOnce(t.Context())

	// Fetch run output
	run, err := client.GetRun(t.Context(), runID)
	require.NoError(t, err)
	var outputResp testpb.PingResponse
	require.NoError(t, run.Output.UnmarshalTo(&outputResp))

	require.Contains(t, []string{"4", "4.0"}, outputResp.Message)
}

func TestWorkflow_RetryPolicy(t *testing.T) {
	store := starflow.NewInMemoryStore()

	wf := starflow.NewWorker[*testpb.PingRequest, *testpb.PingResponse](store, 10*time.Millisecond)
	wf.RegisterProto(testpb.File_suite_proto_ping_proto)

	attempts := 0
	flakyFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		attempts++
		if attempts < 3 {
			return nil, fmt.Errorf("transient error")
		}
		return &testpb.PingResponse{Message: "ok"}, nil
	}

	policy := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Millisecond), 3)
	starflow.Register(wf, flakyFn, starflow.WithName("starflow_test.flakyFn"), starflow.WithRetryPolicy(policy))

	script := `
load("proto", "proto")

def main(ctx, input):
	ping_proto = proto.file("suite/proto/ping.proto")
	return starflow_test.flakyFn(ctx=ctx, req=ping_proto.PingRequest(message=input.message))
`

	client := starflow.NewClient[*testpb.PingRequest](store)
	runID, err := client.Run(t.Context(), []byte(script), &testpb.PingRequest{Message: "retry"})
	require.NoError(t, err)

	// Use the same worker instance that has the registered functions
	wf.ProcessOnce(t.Context())

	require.Equal(t, 3, attempts)

	run, err := client.GetRun(t.Context(), runID)
	require.NoError(t, err)
	require.Equal(t, starflow.RunStatusCompleted, run.Status)

	// Debug output
	t.Logf("Run Status: %s", run.Status)
	t.Logf("Run Error: %s", run.Error)
	if run.Output != nil {
		t.Logf("Run Output: present")
	} else {
		t.Logf("Run Output: nil")
	}
}

func TestWorkflow_SleepFunction(t *testing.T) {
	store := starflow.NewInMemoryStore()

	wf := starflow.NewWorker[*testpb.PingRequest, *testpb.PingResponse](store, 10*time.Millisecond)
	wf.RegisterProto(testpb.File_suite_proto_ping_proto)

	script := `
load("proto", "proto")
load("time", "sleep")

def main(ctx, input):
	dur_proto = proto.file("google/protobuf/duration.proto")
	sleep(ctx=ctx, duration=dur_proto.Duration(seconds=0, nanos=5000000))  # 5ms sleep
	return proto.file("suite/proto/ping.proto").PingResponse(message="woke")
`

	client := starflow.NewClient[*testpb.PingRequest](store)
	runID, err := client.Run(t.Context(), []byte(script), &testpb.PingRequest{Message: "zzz"})
	require.NoError(t, err)

	worker := starflow.NewWorker[*testpb.PingRequest, *testpb.PingResponse](store, 10*time.Millisecond)
	worker.ProcessOnce(t.Context()) // should complete

	run, _ := client.GetRun(t.Context(), runID)
	require.Equal(t, starflow.RunStatusCompleted, run.Status)
}

func TestWorkflow_Failure(t *testing.T) {
	store := starflow.NewInMemoryStore()

	wf := starflow.NewWorker[*testpb.PingRequest, *testpb.PingResponse](store, 10*time.Millisecond)
	wf.RegisterProto(testpb.File_suite_proto_ping_proto)

	// Register a function that always fails
	failingFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		return nil, fmt.Errorf("intentional failure: %s", req.Message)
	}
	starflow.Register(wf, failingFn, starflow.WithName("starflow_test.failingFn"))

	script := `
load("proto", "proto")

def main(ctx, input):
	ping_proto = proto.file("suite/proto/ping.proto")
	return starflow_test.failingFn(ctx=ctx, req=ping_proto.PingRequest(message=input.message))
`

	client := starflow.NewClient[*testpb.PingRequest](store)
	runID, err := client.Run(t.Context(), []byte(script), &testpb.PingRequest{Message: "should fail"})
	require.NoError(t, err)

	// Process the workflow
	wf.ProcessOnce(t.Context())

	// Fetch run output and verify it failed
	run, err := client.GetRun(t.Context(), runID)
	require.NoError(t, err)

	// Debug output
	t.Logf("Run Status: %s", run.Status)
	t.Logf("Run Error: %s", run.Error)
	if run.Output != nil {
		t.Logf("Run Output: present")
	} else {
		t.Logf("Run Output: nil")
	}

	require.Equal(t, starflow.RunStatusFailed, run.Status)
	require.Error(t, run.Error)
	require.Contains(t, run.Error.Error(), "intentional failure: should fail")

	// Verify events show the failure
	runEvents, err := client.GetEvents(t.Context(), runID)
	require.NoError(t, err)
	require.Len(t, runEvents, 3)

	// First event should be the claim
	require.Equal(t, events.EventTypeClaim, runEvents[0].Type())

	// Second event should be the function call
	require.Equal(t, events.EventTypeCall, runEvents[1].Type())
	if callEvent, ok := runEvents[1].Metadata.(events.CallEvent); ok {
		require.Equal(t, "starflow_test.failingFn", callEvent.FunctionName())
	}

	// Third event should be the return with error
	require.Equal(t, events.EventTypeReturn, runEvents[2].Type())
	if returnEvent, ok := runEvents[2].Metadata.(events.ReturnEvent); ok {
		_, err := returnEvent.Output()
		require.Error(t, err)
		require.Contains(t, err.Error(), "intentional failure: should fail")
	}

	t.Log("✅ Workflow failure correctly detected!")
	t.Logf("Run ID: %s", runID)
	t.Logf("Status: %s", run.Status)
	t.Logf("Error: %s", run.Error)
}

func PingPong(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
	return &testpb.PingResponse{Message: "pong: " + req.Message}, nil
}

func TestWorkflow_FullPackagePath(t *testing.T) {
	store := starflow.NewInMemoryStore()

	wf := starflow.NewWorker[*testpb.PingRequest, *testpb.PingResponse](store, 10*time.Millisecond)
	wf.RegisterProto(testpb.File_suite_proto_ping_proto)

	starflow.Register(wf, PingPong, starflow.WithName("tests_test.PingPong"))
	for _, name := range wf.RegisteredNames() {
		t.Logf("Registered: %s", name)
	}

	script := `
load("proto", "proto")

def main(ctx, input):
	ping_proto = proto.file("suite/proto/ping.proto")
	output = tests_test.PingPong(ctx=ctx, req=ping_proto.PingRequest(message=input.message))
	return output
`

	client := starflow.NewClient[*testpb.PingRequest](store)
	runID, err := client.Run(t.Context(), []byte(script), &testpb.PingRequest{Message: "test"})
	require.NoError(t, err)

	wf.ProcessOnce(t.Context())

	run, err := client.GetRun(t.Context(), runID)
	require.NoError(t, err)
	require.Equal(t, starflow.RunStatusCompleted, run.Status)

	var outputResp testpb.PingResponse
	require.NoError(t, run.Output.UnmarshalTo(&outputResp))
	require.Equal(t, "pong: test", outputResp.Message)
}

func TestWorkflow_DeterministicFunctions(t *testing.T) {
	store := starflow.NewInMemoryStore()

	wf := starflow.NewWorker[*testpb.PingRequest, *testpb.PingResponse](store, 10*time.Millisecond)
	wf.RegisterProto(testpb.File_suite_proto_ping_proto)

	var runID, cid string
	yieldFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		var err error
		runID, cid, err = starflow.NewYieldError(ctx)
		return nil, err
	}
	starflow.Register(wf, yieldFn, starflow.WithName("starflow_test.yieldFn"))

	script := `
load("proto", "proto")
load("time", time_now="now")
load("rand", rand_int="int")

ping_proto = proto.file("suite/proto/ping.proto")

def main(ctx, input):
	now = time_now(ctx=ctx)
	rand = rand_int(ctx=ctx, max=100)

	starflow_test.yieldFn(ctx=ctx, req=input)
	return ping_proto.PingResponse(message="now: " + str(now) + ", rand: " + str(rand))
`

	client := starflow.NewClient[*testpb.PingRequest](store)
	runID, err := client.Run(t.Context(), []byte(script), &testpb.PingRequest{Message: "test"})
	require.NoError(t, err)

	// Process the workflow
	wf.ProcessOnce(t.Context())

	// Fetch run output
	run, err := client.GetRun(t.Context(), runID)
	require.NoError(t, err)
	require.Equal(t, starflow.RunStatusYielded, run.Status)

	// Resume the workflow
	outputAny, err := anypb.New(&testpb.PingResponse{Message: "resumed"})
	require.NoError(t, err)

	err = client.Signal(t.Context(), runID, cid, outputAny)
	require.NoError(t, err)

	wf.ProcessOnce(t.Context())

	// Verify events were recorded
	run, err = client.GetRun(t.Context(), runID)
	require.NoError(t, err)
	require.Equal(t, starflow.RunStatusCompleted, run.Status)

	runEvents, err := client.GetEvents(t.Context(), runID)
	require.NoError(t, err)
	require.Len(t, runEvents, 8)

	// Check that we have the expected event types
	timeNowCount := 0
	randIntCount := 0
	for _, event := range runEvents {
		switch event.Type() {
		case events.EventTypeTimeNow:
			timeNowCount++
		case events.EventTypeRandInt:
			randIntCount++
		}
	}
	require.Equal(t, 1, timeNowCount, "Expected 1 time.now events")
	require.Equal(t, 1, randIntCount, "Expected 1 rand.int events")

	// Verify the output contains the expected values
	var outputResp testpb.PingResponse
	require.NoError(t, run.Output.UnmarshalTo(&outputResp))
	require.Contains(t, outputResp.Message, "now:")
	require.Contains(t, outputResp.Message, "rand:")

	t.Log("✅ Deterministic functions test completed successfully!")
	t.Logf("Run ID: %s", runID)
	t.Logf("Output: %s", outputResp.Message)
	t.Logf("Events recorded: %d", len(runEvents))
}

func TestWorkflow_YieldError(t *testing.T) {
	store := starflow.NewInMemoryStore()

	wf := starflow.NewWorker[*testpb.PingRequest, *testpb.PingResponse](store, 10*time.Millisecond)
	wf.RegisterProto(testpb.File_suite_proto_ping_proto)

	var called int
	var runID, cid string

	yieldFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		called++

		var err error
		runID, cid, err = starflow.NewYieldError(ctx)
		return nil, err
	}
	starflow.Register(wf, yieldFn, starflow.WithName("starflow_test.yieldFn"))

	script := `
load("proto", "proto")

def main(ctx, input):
	ping_proto = proto.file("suite/proto/ping.proto")
	starflow_test.yieldFn(ctx=ctx, req=ping_proto.PingRequest(message=input.message))
	return ping_proto.PingResponse(message="should not be reached")
`

	client := starflow.NewClient[*testpb.PingRequest](store)
	runID, err := client.Run(t.Context(), []byte(script), &testpb.PingRequest{Message: "test"})
	require.NoError(t, err)

	wf.ProcessOnce(t.Context())
	require.Equal(t, 1, called)

	run, err := client.GetRun(t.Context(), runID)
	require.NoError(t, err)
	require.Equal(t, starflow.RunStatusYielded, run.Status)

	outputAny, err := anypb.New(&testpb.PingResponse{Message: "resumed"})
	require.NoError(t, err)

	err = client.Signal(t.Context(), runID, cid, outputAny)
	require.NoError(t, err)

	run, err = client.GetRun(t.Context(), runID)
	require.NoError(t, err)
	require.Equal(t, starflow.RunStatusPending, run.Status)

	wf.ProcessOnce(t.Context())
	require.Equal(t, 1, called)

	run, err = client.GetRun(t.Context(), runID)
	require.NoError(t, err)
	require.Equal(t, starflow.RunStatusCompleted, run.Status)
}

func TestWorkflow_StringValue(t *testing.T) {
	store := starflow.NewInMemoryStore()

	wf := starflow.NewWorker[*testpb.PingRequest, *testpb.PingResponse](store, 10*time.Millisecond)
	// Note: No proto registration needed for well-known types

	script := `
load("proto", "proto")

def main(ctx, input):
	# Test StringValue well-known proto type
	
	# Create a StringValue message
	stringvalue_proto = proto.file("google/protobuf/wrappers.proto")
	string_value = stringvalue_proto.StringValue(value="test string value")
	
	# Return a PingResponse with the StringValue info
	ping_proto = proto.file("suite/proto/ping.proto")
	message = "StringValue: " + string_value.value + ", Input: " + input.message
	return ping_proto.PingResponse(message=message)
`

	client := starflow.NewClient[*testpb.PingRequest](store)
	runID, err := client.Run(t.Context(), []byte(script), &testpb.PingRequest{Message: "hello world"})
	require.NoError(t, err)

	wf.ProcessOnce(t.Context())

	run, err := client.GetRun(t.Context(), runID)
	require.NoError(t, err)
	require.Equal(t, starflow.RunStatusCompleted, run.Status)

	var outputResp testpb.PingResponse
	require.NoError(t, run.Output.UnmarshalTo(&outputResp))
	require.Equal(t, "StringValue: test string value, Input: hello world", outputResp.Message)
}
