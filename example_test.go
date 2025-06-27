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

	// Create a client
	client := starflow.NewClient[*wrapperspb.StringValue, *wrapperspb.StringValue](store)
	client.RegisterProto(wrapperspb.File_google_protobuf_wrappers_proto)

	// Register a simple echo function
	echoFn := func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return &wrapperspb.StringValue{Value: "echo: " + req.Value}, nil
	}
	starflow.RegisterFunc(client, echoFn, starflow.WithName("module.echoFn"))

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
	output, err := client.Run(context.Background(), "run-id", []byte(script), &wrapperspb.StringValue{Value: "hello world"})
	if err != nil {
		panic(err)
	}

	fmt.Printf("Result: %s\n", output.Value)

	// Output: Result: echo: hello world
}
