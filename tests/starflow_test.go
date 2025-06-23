package starflow_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/dynoinc/starflow"
	pb "github.com/dynoinc/starflow/tests/proto"

	// Import proto packages to register types
	_ "github.com/dynoinc/starflow/tests/proto"
)

// Global functions for proper reflection usage
func pingFn(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{Message: "pong: " + req.Message}, nil
}

func paymentFn(ctx context.Context, req *pb.ProcessPaymentRequest) (*pb.ProcessPaymentResponse, error) {
	return &pb.ProcessPaymentResponse{Success: true, TransactionId: "txn_123"}, nil
}

// Global flag to control bakingFnFails behavior for testing
var bakingFnShouldFail = true

func bakingFnFails(ctx context.Context, req *pb.BakePizzaRequest) (*pb.BakePizzaResponse, error) {
	if bakingFnShouldFail {
		return nil, fmt.Errorf("oven is on fire")
	}
	return &pb.BakePizzaResponse{Success: true}, nil
}

func httpCallFn(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	// In a real implementation, you would make an actual HTTP call
	// For this example, we'll just simulate it
	return &pb.PingResponse{Message: "HTTP response simulated"}, nil
}

func dbQueryFn(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	// Simulate database query
	return &pb.PingResponse{Message: "DB result for: " + req.Message}, nil
}

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

	wf := starflow.New[*pb.PingRequest, *pb.PingResponse](store)
	starflow.Register(wf, pingFn)

	script := `
load("proto", "proto")

def main(ctx, input):
	ping_proto = proto.file("tests/proto/ping.proto")
	output = pingFn(ctx=ctx, req=ping_proto.PingRequest(message=input.message))
	return output
`

	runID, err := wf.Run([]byte(script), &pb.PingRequest{Message: "hello"})
	if err != nil {
		t.Fatalf("workflow run failed: %v", err)
	}

	output, err := wf.Resume(context.Background(), runID)
	if err != nil {
		t.Fatalf("workflow resume failed: %v", err)
	}

	if output.Message != "pong: hello" {
		t.Errorf("expected output message to be 'pong: hello', got %s", output.Message)
	}

	run, err := store.GetRun(runID)
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}

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
	bakingFnShouldFail = true // Make sure it fails the first time
	wf1 := starflow.New[*pb.OrderPizzaRequest, *pb.OrderPizzaResponse](store)
	starflow.Register(wf1, paymentFn)
	starflow.Register(wf1, bakingFnFails)

	script := `
load("proto", "proto")

def main(ctx, input):
	print("workflow input:", input)
	pizza_proto = proto.file("tests/proto/pizza.proto")
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
	runID, err := wf1.Run([]byte(script), &pb.OrderPizzaRequest{
		PizzaType:        "pepperoni",
		Quantity:         1,
		CreditCardNumber: "1234-5678-8765-4321",
	})

	if err != nil {
		t.Fatalf("failed to create run: %v", err)
	}

	// Try to execute - this should fail
	_, err = wf1.Resume(context.Background(), runID)
	if err == nil {
		t.Fatalf("expected workflow to fail on the first run, but it succeeded")
	}

	run, err := store.GetRun(runID)
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}
	if run.Status != starflow.RunStatusFailed {
		t.Errorf("expected run status to be FAILED, got %s", run.Status)
	}

	// --- Second run: Resumes and succeeds ---
	bakingFnShouldFail = false // Make it succeed this time
	wf2 := starflow.New[*pb.OrderPizzaRequest, *pb.OrderPizzaResponse](store)
	starflow.Register(wf2, paymentFn)
	starflow.Register(wf2, bakingFnFails) // Same function, but now it will succeed

	output, err := wf2.Resume(context.Background(), runID)
	if err != nil {
		t.Fatalf("workflow resume failed: %v", err)
	}

	if output.Status != "ORDER_COMPLETE" {
		t.Errorf("expected output status to be ORDER_COMPLETE, got %s", output.Status)
	}

	run, err = store.GetRun(runID)
	if err != nil {
		t.Fatalf("failed to get run after resume: %v", err)
	}
	if run.Status != starflow.RunStatusCompleted {
		t.Errorf("expected run status to be COMPLETED after resume, got %s", run.Status)
	}

	events, err := store.GetEvents(runID)
	if err != nil {
		t.Fatalf("failed to get events after resume: %v", err)
	}
	if len(events) != 6 {
		t.Fatalf("expected 6 events after resume, got %d", len(events))
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
	wf := starflow.New[*pb.PingRequest, *pb.PingResponse](store)
	starflow.Register(wf, httpCallFn)
	starflow.Register(wf, dbQueryFn)

	// Step 4: Define your Starlark workflow script
	script := `
load("proto", "proto")

def main(ctx, input):
	print("Starting workflow with input:", input.message)
	
	# Load the proto file to access message types
	ping_proto = proto.file("tests/proto/ping.proto")
	
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
	runID, err := wf.Run([]byte(script), &pb.PingRequest{Message: "example"})
	if err != nil {
		t.Fatalf("workflow run failed: %v", err)
	}

	// Step 6: Execute the workflow
	output, err := wf.Resume(context.Background(), runID)
	if err != nil {
		t.Fatalf("workflow resume failed: %v", err)
	}

	expectedMessage := "Completed: HTTP response simulated + DB result for: db_example"
	if output.Message != expectedMessage {
		t.Errorf("expected output message to be %s, got %s", expectedMessage, output.Message)
	}

	// Step 7: Verify the workflow completed successfully
	run, err := store.GetRun(runID)
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}

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

	t.Log("✅ Workflow completed successfully!")
	t.Logf("Run ID: %s", runID)
	t.Logf("Output: %s", output.Message)
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

	wf := starflow.New[*pb.PingRequest, *pb.PingResponse](store)
	starflow.Register(wf, pingFn)

	script := `
load("proto", "proto")
load("math", "sqrt")

def main(ctx, input):
    # Use math.sqrt to compute the square root of 16
    result = sqrt(16)
    return proto.file("tests/proto/ping.proto").PingResponse(message=str(result))
`

	runID, err := wf.Run([]byte(script), &pb.PingRequest{Message: "test"})
	if err != nil {
		t.Fatalf("workflow run failed: %v", err)
	}

	output, err := wf.Resume(context.Background(), runID)
	if err != nil {
		t.Fatalf("workflow resume failed: %v", err)
	}

	if output.Message != "4" && output.Message != "4.0" {
		t.Errorf("expected output message to be '4' or '4.0', got %s", output.Message)
	}
}
