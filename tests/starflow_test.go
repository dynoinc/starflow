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
	"google.golang.org/protobuf/proto"
)

func TestWorkflow(t *testing.T) {
	store := NewMemoryStore(t)

	wf := starflow.New[*testpb.PingRequest, *testpb.PingResponse](store)

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

	runID, err := wf.Run([]byte(script), &testpb.PingRequest{Message: "hello"})
	require.NoError(t, err)

	worker := wf.NewWorker(0)
	worker.ProcessOnce(t.Context())

	// Fetch run output
	run, err := store.GetRun(runID)
	require.NoError(t, err)
	var outputMsg testpb.PingResponse

	require.Equal(t, starflow.RunStatusCompleted, run.Status)

	events, err := store.GetEvents(runID)
	require.NoError(t, err)

	require.Len(t, events, 2)

	require.Equal(t, starflow.EventTypeCall, events[0].Type)
	require.Equal(t, "pingFn", events[0].FunctionName)

	require.Equal(t, starflow.EventTypeReturn, events[1].Type)
	require.Empty(t, events[1].Error)

	require.NoError(t, proto.Unmarshal(run.Output, &outputMsg))

	require.Equal(t, "pong: hello", outputMsg.Message)

	fmt.Println("events:")
	for _, e := range events {
		fmt.Printf("%+v\n", *e)
	}
}

func TestWorkflow_Resume(t *testing.T) {
	store := NewMemoryStore(t)

	// --- First run: Fails during baking ---
	bakingShouldFail := true

	paymentFn := func(ctx context.Context, req *testpb.ProcessPaymentRequest) (*testpb.ProcessPaymentResponse, error) {
		return &testpb.ProcessPaymentResponse{Success: true, TransactionId: "txn_123"}, nil
	}

	bakingFnFails := func(ctx context.Context, req *testpb.BakePizzaRequest) (*testpb.BakePizzaResponse, error) {
		if bakingShouldFail {
			return &testpb.BakePizzaResponse{Success: false}, nil
		}

		return &testpb.BakePizzaResponse{Success: true}, nil
	}

	var cid string
	yieldFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingRequest, error) {
		var err error
		cid, err = starflow.Yield()
		fmt.Println("yielding", cid, err)
		return nil, err
	}

	wf1 := starflow.New[*testpb.OrderPizzaRequest, *testpb.OrderPizzaResponse](store)
	starflow.Register(wf1, paymentFn, starflow.WithName("paymentFn"))
	starflow.Register(wf1, bakingFnFails, starflow.WithName("bakingFnFails"))
	starflow.Register(wf1, yieldFn, starflow.WithName("yieldFn"))

	script := `
load("proto", "proto")

def main(ctx, input):
	print("workflow input:", input)
	ping_proto = proto.file("ping.proto")
	pizza_proto = proto.file("pizza.proto")
	payment_req = pizza_proto.ProcessPaymentRequest(credit_card_number=input.credit_card_number, amount=1500)
	payment_res = paymentFn(ctx=ctx, req=payment_req)

	if not payment_res.success:
		return pizza_proto.OrderPizzaResponse(status="PAYMENT_FAILED")

	bake_req = pizza_proto.BakePizzaRequest(pizza_type=input.pizza_type, quantity=input.quantity)
	bake_res = bakingFnFails(ctx=ctx, req=bake_req)
	print("bake_res:", bake_res)

	if not bake_res.success:
		print("yielding")
		yieldFn(ctx=ctx, req=ping_proto.PingRequest(message="oven_on_fire"))

	bake_res = bakingFnFails(ctx=ctx, req=bake_req)
	if not bake_res.success:
		return pizza_proto.OrderPizzaResponse(status="BAKING_FAILED")

	return pizza_proto.OrderPizzaResponse(order_id="order_456", status="ORDER_COMPLETE")
`
	runID, err := wf1.Run([]byte(script), &testpb.OrderPizzaRequest{
		PizzaType:        "pepperoni",
		Quantity:         1,
		CreditCardNumber: "1234-5678-8765-4321",
	})

	require.NoError(t, err)

	// Try to execute - this should yield
	worker1 := wf1.NewWorker(0)
	worker1.ProcessOnce(t.Context())

	fmt.Println("events:")
	events, err := store.GetEvents(runID)
	require.NoError(t, err)
	for _, e := range events {
		fmt.Printf("%+v\n", *e)
	}

	run, err := store.GetRun(runID)
	require.NoError(t, err)
	require.Equal(t, starflow.RunStatusWaiting, run.Status, run.Error)

	// --- Second run: Resumes and succeeds ---
	bakingShouldFail = false

	// Send signal to resume with flag flipped
	require.NoError(t, wf1.Signal(t.Context(), cid, &testpb.BakePizzaResponse{Success: true}))

	// Process again – should complete
	worker1.ProcessOnce(t.Context())

	run, err = store.GetRun(runID)
	require.NoError(t, err)
	require.Equal(t, starflow.RunStatusCompleted, run.Status)

	var outResp testpb.OrderPizzaResponse
	require.NoError(t, proto.Unmarshal(run.Output, &outResp))

	require.Equal(t, "ORDER_COMPLETE", outResp.Status)
}

// TestWorkflowLibraryUsage demonstrates how to use the starflow library.
func TestWorkflowLibraryUsage(t *testing.T) {
	// Step 1: Create a temporary database for workflow storage
	store := NewMemoryStore(t)

	// Step 3: Create the workflow and register functions
	wf := starflow.New[*testpb.PingRequest, *testpb.PingResponse](store)

	httpCallFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		return &testpb.PingResponse{Message: "HTTP response simulated"}, nil
	}
	dbQueryFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		return &testpb.PingResponse{Message: "DB result for: " + req.Message}, nil
	}

	starflow.Register(wf, httpCallFn, starflow.WithName("httpCallFn"))
	starflow.Register(wf, dbQueryFn, starflow.WithName("dbQueryFn"))

	// Step 4: Define your Starlark workflow script
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

	// Step 5: Run the workflow
	runID, err := wf.Run([]byte(script), &testpb.PingRequest{Message: "example"})
	require.NoError(t, err)

	// Step 6: Execute the workflow
	worker := wf.NewWorker(0)
	worker.ProcessOnce(t.Context())

	// Fetch run output
	run, err := store.GetRun(runID)
	require.NoError(t, err)
	var outputResp testpb.PingResponse
	require.NoError(t, proto.Unmarshal(run.Output, &outputResp))

	expectedMessage := "Completed: HTTP response simulated + DB result for: db_example"
	require.Equal(t, expectedMessage, outputResp.Message)

	// Step 7: Verify the workflow completed successfully
	require.Equal(t, starflow.RunStatusCompleted, run.Status)

	// Step 8: Check that all function calls were recorded
	events, err := store.GetEvents(runID)
	require.NoError(t, err)
	require.NoError(t, err)

	// Should have 4 events: 2 calls + 2 returns
	require.Equal(t, 4, len(events))

	// Verify the events are for our functions
	expectedFunctions := []string{"httpCallFn", "httpCallFn", "dbQueryFn", "dbQueryFn"}
	expectedTypes := []starflow.EventType{
		starflow.EventTypeCall, starflow.EventTypeReturn,
		starflow.EventTypeCall, starflow.EventTypeReturn,
	}

	for i, event := range events {
		require.Equal(t, expectedTypes[i], event.Type, "event %d type mismatch", i)
		expectedFunc := expectedFunctions[i]
		require.Equal(t, expectedFunc, event.FunctionName, "event %d function name mismatch", i)
	}

	fmt.Println("events:")
	for _, e := range events {
		fmt.Printf("%+v\n", *e)
	}

	t.Log("✅ Workflow completed successfully!")
	t.Logf("Run ID: %s", runID)
	t.Logf("Output: %s", outputResp.Message)
	t.Logf("Events recorded: %d", len(events))
}

func TestWorkflow_StarlarkMathImport(t *testing.T) {
	store := NewMemoryStore(t)

	wf := starflow.New[*testpb.PingRequest, *testpb.PingResponse](store)

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

	runID, err := wf.Run([]byte(script), &testpb.PingRequest{Message: "test"})
	require.NoError(t, err)

	worker := wf.NewWorker(0)
	worker.ProcessOnce(t.Context())

	// Fetch run output
	run, err := store.GetRun(runID)
	require.NoError(t, err)
	var outputResp testpb.PingResponse
	require.NoError(t, proto.Unmarshal(run.Output, &outputResp))

	require.Contains(t, []string{"4", "4.0"}, outputResp.Message)
}

func TestWorkflow_RetryPolicy(t *testing.T) {
	store := NewMemoryStore(t)

	wf := starflow.New[*testpb.PingRequest, *testpb.PingResponse](store)

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

	runID, err := wf.Run([]byte(script), &testpb.PingRequest{Message: "retry"})
	require.NoError(t, err)

	worker := wf.NewWorker(0)
	worker.ProcessOnce(t.Context())

	require.Equal(t, 3, attempts)

	run, err := store.GetRun(runID)
	require.NoError(t, err)
	require.Equal(t, starflow.RunStatusCompleted, run.Status)
}

func TestWorkflow_SleepFunction(t *testing.T) {
	store := NewMemoryStore(t)

	wf := starflow.New[*testpb.PingRequest, *testpb.PingResponse](store)

	script := `
load("proto", "proto")
load("time", "sleep")

def main(ctx, input):
	dur_proto = proto.file("google/protobuf/duration.proto")
	sleep(ctx=ctx, duration=dur_proto.Duration(seconds=0, nanos=5000000))  # 5ms durable sleep
	return proto.file("ping.proto").PingResponse(message="woke")
`

	runID, err := wf.Run([]byte(script), &testpb.PingRequest{Message: "zzz"})
	require.NoError(t, err)

	worker := wf.NewWorker(0)
	worker.ProcessOnce(t.Context()) // should yield

	run, _ := store.GetRun(runID)
	require.Equal(t, starflow.RunStatusWaiting, run.Status)

	time.Sleep(50 * time.Millisecond)

	worker.ProcessOnce(t.Context()) // should complete

	run, _ = store.GetRun(runID)
	require.Equal(t, starflow.RunStatusCompleted, run.Status)
}

func TestWorkflow_Yield(t *testing.T) {
	store := NewMemoryStore(t)

	wf := starflow.New[*testpb.PingRequest, *testpb.PingResponse](store)

	yieldFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		_, err := starflow.Yield()
		return nil, err
	}
	starflow.Register(wf, yieldFn, starflow.WithName("yieldFn"))

	script := `
load("proto", "proto")

def main(ctx, input):
	ping_proto = proto.file("ping.proto")
	resp = yieldFn(ctx=ctx, req=ping_proto.PingRequest(message=input.message))
	return resp
`

	runID, err := wf.Run([]byte(script), &testpb.PingRequest{Message: "yield"})
	require.NoError(t, err)

	worker := wf.NewWorker(0)
	// First processing – should hit yield
	worker.ProcessOnce(t.Context())

	// Verify status WAITING
	run, err := store.GetRun(runID)
	require.NoError(t, err)
	require.Equal(t, starflow.RunStatusWaiting, run.Status)

	// Find the correlation ID from the last event
	events, err := store.GetEvents(runID)
	fmt.Println("events:", events)
	require.NoError(t, err)
	require.NotEmpty(t, events)
	var cid string
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Type == starflow.EventTypeYield {
			cid = events[i].CorrelationID
			break
		}
	}
	require.NotEmpty(t, cid, "correlation id not found")

	// Send signal
	require.NoError(t, wf.Signal(t.Context(), cid, &testpb.PingResponse{Message: "done"}))

	// Process again – should complete
	worker.ProcessOnce(t.Context())

	run, err = store.GetRun(runID)
	require.NoError(t, err)
	require.Equal(t, starflow.RunStatusCompleted, run.Status)

	fmt.Println("events:")
	for _, e := range events {
		fmt.Printf("%+v\n", *e)
	}
}
