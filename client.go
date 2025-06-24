package starflow

import (
	"context"
	"fmt"
	"reflect"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

type Client[Input proto.Message] struct {
	store  Store
	tracer trace.Tracer
}

// NewClient creates a new Workflow executor with the specified input and output types.
func NewClient[Input proto.Message](store Store) *Client[Input] {
	return &Client[Input]{
		store:  store,
		tracer: otel.Tracer("starflow.client"),
	}
}

// Run creates a new workflow run with the given script and input, returning the run ID.
func (c *Client[Input]) Run(ctx context.Context, script []byte, input Input) (string, error) {
	// Save script
	scriptHash, err := c.store.SaveScript(ctx, script)
	if err != nil {
		return "", fmt.Errorf("failed to save script: %w", err)
	}

	// Marshal input
	var inputBytes []byte
	inputVal := reflect.ValueOf(input)
	if !inputVal.IsNil() {
		inputBytes, err = proto.Marshal(input)
		if err != nil {
			return "", fmt.Errorf("failed to marshal input: %w", err)
		}
	}

	// Create run
	runID, err := c.store.CreateRun(ctx, scriptHash, inputBytes)
	if err != nil {
		return "", fmt.Errorf("failed to create run: %w", err)
	}

	return runID, nil
}

func (c *Client[Input]) GetRun(ctx context.Context, runID string) (*Run, error) {
	return c.store.GetRun(ctx, runID)
}

func (c *Client[Input]) GetEvents(ctx context.Context, runID string) ([]*Event, error) {
	return c.store.GetEvents(ctx, runID)
}
