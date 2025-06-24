package starflow_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dynoinc/starflow"
	testpb "github.com/dynoinc/starflow/tests/proto"
	"google.golang.org/protobuf/proto"
)

func TestWorkflow(t *testing.T) {
	f, err := os.CreateTemp("", "starflow-*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())
	f.Close()

	store, err := starflow.NewSQLiteStore(f.Name())
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

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
	if err != nil {
		t.Fatalf("workflow run failed: %v", err)
	}

	worker := wf.NewWorker(0)
	worker.ProcessOnce(context.Background())

	// Fetch run output
	run, err := store.GetRun(runID)
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}
	var outputMsg testpb.PingResponse

	if run.Status != starflow.RunStatusCompleted {
		t.Errorf("expected run status to be COMPLETED, got %s", run.Status)
	}

	events, err := store.GetEvents(runID)
	if err != nil {
		t.Fatalf("failed to get events: %v", err)
	}

	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	if events[0].Type != starflow.EventTypeCall {
		t.Errorf("expected first event to be CALL, got %s", events[0].Type)
	}
	if events[0].FunctionName != "pingFn" {
		t.Errorf("expected first event to be for function pingFn, got %s", events[0].FunctionName)
	}

	if events[1].Type != starflow.EventTypeReturn {
		t.Errorf("expected second event to be RETURN, got %s", events[1].Type)
	}
	if events[1].Error != "" {
		t.Errorf("expected second event to have no error, got %s", events[1].Error)
	}

	if err := proto.Unmarshal(run.Output, &outputMsg); err != nil {
		t.Fatalf("failed to unmarshal output: %v", err)
	}

	if outputMsg.Message != "pong: hello" {
		t.Errorf("expected output message to be 'pong: hello', got %s", outputMsg.Message)
	}

	fmt.Println("events:")
	for _, e := range events {
		fmt.Printf("%+v\n", *e)
	}
}

func TestWorkflow_Resume(t *testing.T) {
	f, err := os.CreateTemp("", "starflow-*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())
	f.Close()

	store, err := starflow.NewSQLiteStore(f.Name())
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	// --- First run: Fails during baking ---
	bakingShouldFail := true

	paymentFn := func(ctx context.Context, req *testpb.ProcessPaymentRequest) (*testpb.ProcessPaymentResponse, error) {
		return &testpb.ProcessPaymentResponse{Success: true, TransactionId: "txn_123"}, nil
	}

	bakingFnFails := func(ctx context.Context, req *testpb.BakePizzaRequest) (*testpb.BakePizzaResponse, error) {
		if bakingShouldFail {
			return nil, fmt.Errorf("oven is on fire")
		}
		return &testpb.BakePizzaResponse{Success: true}, nil
	}

	wf1 := starflow.New[*testpb.OrderPizzaRequest, *testpb.OrderPizzaResponse](store)
	starflow.Register(wf1, paymentFn, starflow.WithName("paymentFn"))
	starflow.Register(wf1, bakingFnFails, starflow.WithName("bakingFnFails"))

	script := `
load("proto", "proto")

def main(ctx, input):
	print("workflow input:", input)
	pizza_proto = proto.file("pizza.proto")
	payment_req = pizza_proto.ProcessPaymentRequest(credit_card_number=input.credit_card_number, amount=1500)
	payment_res = paymentFn(ctx=ctx, req=payment_req)

	if not payment_res.success:
		return pizza_proto.OrderPizzaResponse(status="PAYMENT_FAILED")

	bake_req = pizza_proto.BakePizzaRequest(pizza_type=input.pizza_type, quantity=input.quantity)
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

	if err != nil {
		t.Fatalf("failed to create run: %v", err)
	}

	// Try to execute - this should fail
	worker1 := wf1.NewWorker(0)
	worker1.ProcessOnce(context.Background())

	run, err := store.GetRun(runID)
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}
	if run.Status != starflow.RunStatusFailed {
		t.Errorf("expected run status to be FAILED, got %s", run.Status)
	}

	// --- Second run: Resumes and succeeds ---
	bakingShouldFail = false

	if err := store.UpdateRunStatus(context.Background(), runID, starflow.RunStatusPending); err != nil {
		t.Fatalf("failed to reset status: %v", err)
	}

	wf2 := starflow.New[*testpb.OrderPizzaRequest, *testpb.OrderPizzaResponse](store)
	starflow.Register(wf2, paymentFn, starflow.WithName("paymentFn"))
	starflow.Register(wf2, bakingFnFails, starflow.WithName("bakingFnFails"))

	worker2 := wf2.NewWorker(0)
	worker2.ProcessOnce(context.Background())

	run, err = store.GetRun(runID)
	if err != nil {
		t.Fatalf("failed to get run after second processing: %v", err)
	}

	if run.Status != starflow.RunStatusCompleted {
		t.Errorf("expected run status to be COMPLETED, got %s", run.Status)
	}

	var outResp testpb.OrderPizzaResponse
	if err := proto.Unmarshal(run.Output, &outResp); err != nil {
		t.Fatalf("failed to unmarshal output: %v", err)
	}

	if outResp.Status != "ORDER_COMPLETE" {
		t.Errorf("expected output status to be ORDER_COMPLETE, got %s", outResp.Status)
	}

	events, err := store.GetEvents(runID)
	if err != nil {
		t.Fatalf("failed to get events after second processing: %v", err)
	}
	if len(events) != 6 {
		t.Fatalf("expected 6 events after second processing, got %d", len(events))
	}

	fmt.Println("events:")
	for _, e := range events {
		fmt.Printf("%+v\n", *e)
	}
}

// TestWorkflowLibraryUsage demonstrates how to use the starflow library.
func TestWorkflowLibraryUsage(t *testing.T) {
	// Step 1: Create a temporary database for workflow storage
	f, err := os.CreateTemp("", "starflow-example-*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())
	f.Close()

	store, err := starflow.NewSQLiteStore(f.Name())
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

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
	if err != nil {
		t.Fatalf("workflow run failed: %v", err)
	}

	// Step 6: Execute the workflow
	worker := wf.NewWorker(0)
	worker.ProcessOnce(context.Background())

	// Fetch run output
	run, err := store.GetRun(runID)
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}
	var outputResp testpb.PingResponse
	if err := proto.Unmarshal(run.Output, &outputResp); err != nil {
		t.Fatalf("failed to unmarshal output: %v", err)
	}

	expectedMessage := "Completed: HTTP response simulated + DB result for: db_example"
	if outputResp.Message != expectedMessage {
		t.Errorf("expected output message to be %s, got %s", expectedMessage, outputResp.Message)
	}

	// Step 7: Verify the workflow completed successfully
	if run.Status != starflow.RunStatusCompleted {
		t.Errorf("expected run status to be COMPLETED, got %s", run.Status)
	}

	// Step 8: Check that all function calls were recorded
	events, err := store.GetEvents(runID)
	if err != nil {
		t.Fatalf("failed to get events: %v", err)
	}

	// Should have 4 events: 2 calls + 2 returns
	if len(events) != 4 {
		t.Fatalf("expected 4 events, got %d", len(events))
	}

	// Verify the events are for our functions
	expectedFunctions := []string{"httpCallFn", "httpCallFn", "dbQueryFn", "dbQueryFn"}
	expectedTypes := []starflow.EventType{
		starflow.EventTypeCall, starflow.EventTypeReturn,
		starflow.EventTypeCall, starflow.EventTypeReturn,
	}

	for i, event := range events {
		if event.Type != expectedTypes[i] {
			t.Errorf("event %d: expected type %s, got %s", i, expectedTypes[i], event.Type)
		}
		expectedFunc := expectedFunctions[i]
		if expectedFunc != event.FunctionName {
			t.Errorf("event %d: expected function %s, got %s", i, expectedFunc, event.FunctionName)
		}
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
	f, err := os.CreateTemp("", "starflow-math-*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())
	f.Close()

	store, err := starflow.NewSQLiteStore(f.Name())
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

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
	if err != nil {
		t.Fatalf("workflow run failed: %v", err)
	}

	worker := wf.NewWorker(0)
	worker.ProcessOnce(context.Background())

	// Fetch run output
	run, err := store.GetRun(runID)
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}
	var outputResp testpb.PingResponse
	if err := proto.Unmarshal(run.Output, &outputResp); err != nil {
		t.Fatalf("failed to unmarshal output: %v", err)
	}

	if outputResp.Message != "4" && outputResp.Message != "4.0" {
		t.Errorf("expected output message to be '4' or '4.0', got %s", outputResp.Message)
	}
}

func TestWorkflow_RetryPolicy(t *testing.T) {
	f, err := os.CreateTemp("", "starflow-retry-*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())
	f.Close()

	store, err := starflow.NewSQLiteStore(f.Name())
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

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
	if err != nil {
		t.Fatalf("workflow run failed: %v", err)
	}

	worker := wf.NewWorker(0)
	worker.ProcessOnce(context.Background())

	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}

	run, err := store.GetRun(runID)
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}
	if run.Status != starflow.RunStatusCompleted {
		t.Errorf("expected COMPLETED, got %s", run.Status)
	}
}

func TestWorkflow_SleepFunction(t *testing.T) {
	f, err := os.CreateTemp("", "starflow-sleep-*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())
	f.Close()

	store, err := starflow.NewSQLiteStore(f.Name())
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	wf := starflow.New[*testpb.PingRequest, *testpb.PingResponse](store)

	script := `
load("proto", "proto")
load("time", "sleep")

def main(ctx, input):
	sleep(ctx=ctx, duration=5)  # 5ms durable sleep
	return proto.file("ping.proto").PingResponse(message="woke")
`

	runID, err := wf.Run([]byte(script), &testpb.PingRequest{Message: "zzz"})
	if err != nil {
		t.Fatalf("workflow run failed: %v", err)
	}

	worker := wf.NewWorker(0)
	worker.ProcessOnce(context.Background()) // should yield

	run, _ := store.GetRun(runID)
	if run.Status != starflow.RunStatusWaiting {
		t.Fatalf("expected WAITING, got %s", run.Status)
	}

	time.Sleep(50 * time.Millisecond)

	worker.ProcessOnce(context.Background()) // should complete

	run, _ = store.GetRun(runID)
	if run.Status != starflow.RunStatusCompleted {
		t.Errorf("expected COMPLETED, got %s", run.Status)
	}
}

func TestWorkflow_Yield(t *testing.T) {
	f, err := os.CreateTemp("", "starflow-yield-*.db")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())
	f.Close()

	store, err := starflow.NewSQLiteStore(f.Name())
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

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
	if err != nil {
		t.Fatalf("workflow run failed: %v", err)
	}

	worker := wf.NewWorker(0)
	// First processing – should hit yield
	worker.ProcessOnce(context.Background())

	// Verify status WAITING
	run, err := store.GetRun(runID)
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}
	if run.Status != starflow.RunStatusWaiting {
		t.Fatalf("expected WAITING, got %s", run.Status)
	}

	// Find the correlation ID from the last event
	events, err := store.GetEvents(runID)
	fmt.Println("events:", events)
	if err != nil || len(events) == 0 {
		t.Fatalf("failed to get events: %v", err)
	}
	var cid string
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Type == starflow.EventTypeYield {
			cid = events[i].CorrelationID
			break
		}
	}
	if cid == "" {
		t.Fatalf("correlation id not found")
	}

	// Send signal
	if err := wf.Signal(context.Background(), cid, &testpb.PingResponse{Message: "done"}); err != nil {
		t.Fatalf("signal failed: %v", err)
	}

	// Process again – should complete
	worker.ProcessOnce(context.Background())

	run, err = store.GetRun(runID)
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}
	if run.Status != starflow.RunStatusCompleted {
		t.Errorf("expected COMPLETED after signal, got %s", run.Status)
	}

	fmt.Println("events:")
	for _, e := range events {
		fmt.Printf("%+v\n", *e)
	}
}
