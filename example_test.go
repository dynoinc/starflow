package starflow_test

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dynoinc/starflow"
)

// Example demonstrates the basic usage of the starflow package.
// This example shows how to create a simple workflow that echoes input.
func Example() {
	// Create an in-memory store
	store := starflow.NewInMemoryStore()

	// Create a worker with 10ms poll interval
	worker := starflow.NewWorker[*wrapperspb.StringValue](store)

	// Register a simple echo function
	echoFn := func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return &wrapperspb.StringValue{Value: "echo: " + req.Value}, nil
	}
	starflow.RegisterFunc(worker, echoFn, starflow.WithName("module.echoFn"))

	// Create a client
	client := starflow.NewClient[*wrapperspb.StringValue](store)

	// Define a simple workflow script
	script := `
load("proto", "proto")

def main(ctx, input):
    # Use well-known protobuf types
    stringvalue_proto = proto.file("google/protobuf/wrappers.proto")
    
    # Call our registered function
    result = module.echoFn(ctx=ctx, req=stringvalue_proto.StringValue(value=input.value))
    
    # Return the result
    return result
`

	// Run the workflow
	runID, err := client.Run(context.Background(), []byte(script), &wrapperspb.StringValue{Value: "hello world"})
	if err != nil {
		panic(err)
	}

	// Process the workflow once
	worker.ProcessOnce(context.Background())

	// Get the result
	run, err := client.GetRun(context.Background(), runID)
	if err != nil {
		panic(err)
	}

	var output wrapperspb.StringValue
	run.Output.UnmarshalTo(&output)
	fmt.Printf("Result: %s\n", output.Value)

	// Output: Result: echo: hello world
}

// Example_worker demonstrates how to start a continuous worker.
func Example_worker() {
	// Create an in-memory store
	store := starflow.NewInMemoryStore()

	// Create a worker
	worker := starflow.NewWorker[*wrapperspb.StringValue](store)

	// Register a function
	processFn := func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return &wrapperspb.StringValue{Value: "processed: " + req.Value}, nil
	}
	starflow.RegisterFunc(worker, processFn, starflow.WithName("module.processFn"))

	// Start the worker in a goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go worker.Start(ctx)

	// The worker will now continuously poll for new runs to process
	// You can create runs using a client and the worker will pick them up automatically
}
