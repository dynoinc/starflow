package starflow_test

import (
	"context"
	"fmt"

	"github.com/dynoinc/starflow"
)

// StringValue represents a simple string wrapper for the example
type StringValue struct {
	Value string `json:"value"`
}

// Example demonstrates the basic usage of the starflow package.
// This example shows how to create a simple workflow that echoes input using JSON.
func Example() {
	// Create an in-memory store
	store := starflow.NewInMemoryStore()

	// Create a client with JSON-based types
	client := starflow.NewClient[StringValue, StringValue](store)

	// Register a simple echo function
	echoFn := func(ctx context.Context, req StringValue) (StringValue, error) {
		return StringValue{Value: "echo: " + req.Value}, nil
	}
	starflow.RegisterFunc(client, echoFn, starflow.WithName("module.echoFn"))

	// Define a simple workflow script using JSON
	script := `
def main(ctx, input):
    # Call our registered function with JSON data
    result = module.echoFn(ctx=ctx, req={"value": input["value"]})
    
    # Return the result
    return result
`

	// Run the workflow
	output, err := client.Run(context.Background(), "run-id", []byte(script), StringValue{Value: "hello world"})
	if err != nil {
		panic(err)
	}

	fmt.Printf("Result: %s\n", output.Value)

	// Output: Result: echo: hello world
}
