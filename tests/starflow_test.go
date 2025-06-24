package starflow_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dynoinc/starflow"
	testpb "github.com/dynoinc/starflow/tests/proto"
	"github.com/stretchr/testify/require"
)

func TestWorkflow(t *testing.T) {
	store := NewMemoryStore(t)

	wf := starflow.NewWorker[*testpb.PingRequest, *testpb.PingResponse](store, 10*time.Millisecond)

	pingFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		return &testpb.PingResponse{Message: "pong: " + req.Message}, nil
	}
	starflow.Register(wf, pingFn, starflow.WithName("pingFn"))

	script := `
load("proto", "proto")

def main(ctx, input):
	ping_proto = proto.file("ping.proto")
	output = pingFn(ctx=ctx, req=ping_proto.PingRequest(message=input.message))
	return output
`

	client := starflow.NewClient[*testpb.PingRequest](store)
	runID, err := client.Run(t.Context(), []byte(script), &testpb.PingRequest{Message: "hello"})
	require.NoError(t, err)

	// Use the same worker instance that has the registered functions
	wf.ProcessOnce(t.Context())

	// Fetch run output
	run, err := client.GetRun(t.Context(), runID)
	require.NoError(t, err)
	require.Equal(t, starflow.RunStatusCompleted, run.Status)

	events, err := client.GetEvents(t.Context(), runID)
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, starflow.EventTypeCall, events[0].Type)
	if callEvent, ok := events[0].AsCallEvent(); ok {
		require.Equal(t, "pingFn", callEvent.FunctionName)
	}
	require.Equal(t, starflow.EventTypeReturn, events[1].Type)
	if returnEvent, ok := events[1].AsReturnEvent(); ok {
		require.Empty(t, returnEvent.Error)
	}

	var outputResp testpb.PingResponse
	require.NoError(t, run.Output.UnmarshalTo(&outputResp))
	require.Equal(t, "pong: hello", outputResp.Message)
}

func TestWorkflow_ReplaySupport(t *testing.T) {
	store := NewMemoryStore(t)

	wf := starflow.NewWorker[*testpb.PingRequest, *testpb.PingResponse](store, 10*time.Millisecond)

	httpCallFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		return &testpb.PingResponse{Message: "HTTP response simulated"}, nil
	}
	dbQueryFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		return &testpb.PingResponse{Message: "DB result for: " + req.Message}, nil
	}

	starflow.Register(wf, httpCallFn, starflow.WithName("httpCallFn"))
	starflow.Register(wf, dbQueryFn, starflow.WithName("dbQueryFn"))

	script := `
load("proto", "proto")

def main(ctx, input):
	print("Starting workflow with input:", input.message)
	
	# Load the proto file to access message types
	ping_proto = proto.file("ping.proto")
	
	# Make an HTTP call
	http_req = ping_proto.PingRequest(message="http_" + input.message)
	http_resp = httpCallFn(ctx=ctx, req=http_req)
	print("HTTP response:", http_resp.message)
	
	# Query the database
	db_req = ping_proto.PingRequest(message="db_" + input.message)
	db_resp = dbQueryFn(ctx=ctx, req=db_req)
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

	events, err := client.GetEvents(t.Context(), runID)
	require.NoError(t, err)
	require.NoError(t, err)

	require.Equal(t, 4, len(events))

	expectedFunctions := []string{"httpCallFn", "httpCallFn", "dbQueryFn", "dbQueryFn"}
	expectedTypes := []starflow.EventType{
		starflow.EventTypeCall, starflow.EventTypeReturn,
		starflow.EventTypeCall, starflow.EventTypeReturn,
	}

	for i, event := range events {
		require.Equal(t, expectedTypes[i], event.Type, "event %d type mismatch", i)
		expectedFunc := expectedFunctions[i]
		if callEvent, ok := event.AsCallEvent(); ok {
			require.Equal(t, expectedFunc, callEvent.FunctionName, "event %d function name mismatch", i)
		}
	}

	t.Log("✅ Workflow completed successfully!")
	t.Logf("Run ID: %s", runID)
	t.Logf("Output: %s", outputResp.Message)
	t.Logf("Events recorded: %d", len(events))
}

func TestWorkflow_StarlarkMathImport(t *testing.T) {
	store := NewMemoryStore(t)

	wf := starflow.NewWorker[*testpb.PingRequest, *testpb.PingResponse](store, 10*time.Millisecond)

	pingFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		return &testpb.PingResponse{Message: "pong: " + req.Message}, nil
	}
	starflow.Register(wf, pingFn, starflow.WithName("pingFn"))

	script := `
load("proto", "proto")
load("math", "sqrt")

def main(ctx, input):
    # Use math.sqrt to compute the square root of 16
    result = sqrt(16)
    return proto.file("ping.proto").PingResponse(message=str(result))
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
	store := NewMemoryStore(t)

	wf := starflow.NewWorker[*testpb.PingRequest, *testpb.PingResponse](store, 10*time.Millisecond)

	attempts := 0
	flakyFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		attempts++
		if attempts < 3 {
			return nil, fmt.Errorf("transient error")
		}
		return &testpb.PingResponse{Message: "ok"}, nil
	}

	policy := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Millisecond), 3)
	starflow.Register(wf, flakyFn, starflow.WithName("flakyFn"), starflow.WithRetryPolicy(policy))

	script := `
load("proto", "proto")

def main(ctx, input):
	ping_proto = proto.file("ping.proto")
	return flakyFn(ctx=ctx, req=ping_proto.PingRequest(message=input.message))
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
	store := NewMemoryStore(t)

	script := `
load("proto", "proto")
load("time", "sleep")

def main(ctx, input):
	dur_proto = proto.file("google/protobuf/duration.proto")
	sleep(ctx=ctx, duration=dur_proto.Duration(seconds=0, nanos=5000000))  # 5ms sleep
	return proto.file("ping.proto").PingResponse(message="woke")
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
	store := NewMemoryStore(t)

	wf := starflow.NewWorker[*testpb.PingRequest, *testpb.PingResponse](store, 10*time.Millisecond)

	// Register a function that always fails
	failingFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		return nil, fmt.Errorf("intentional failure: %s", req.Message)
	}
	starflow.Register(wf, failingFn, starflow.WithName("failingFn"))

	script := `
load("proto", "proto")

def main(ctx, input):
	ping_proto = proto.file("ping.proto")
	return failingFn(ctx=ctx, req=ping_proto.PingRequest(message=input.message))
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
	events, err := client.GetEvents(t.Context(), runID)
	require.NoError(t, err)
	require.Len(t, events, 2)

	// First event should be the function call
	require.Equal(t, starflow.EventTypeCall, events[0].Type)
	if callEvent, ok := events[0].AsCallEvent(); ok {
		require.Equal(t, "failingFn", callEvent.FunctionName)
	}

	// Second event should be the return with error
	require.Equal(t, starflow.EventTypeReturn, events[1].Type)
	if callEvent, ok := events[1].AsCallEvent(); ok {
		require.Equal(t, "failingFn", callEvent.FunctionName)
	}
	if returnEvent, ok := events[1].AsReturnEvent(); ok {
		require.Error(t, returnEvent.Error)
		require.Contains(t, returnEvent.Error.Error(), "intentional failure: should fail")
	}

	t.Log("✅ Workflow failure correctly detected!")
	t.Logf("Run ID: %s", runID)
	t.Logf("Status: %s", run.Status)
	t.Logf("Error: %s", run.Error)
}
