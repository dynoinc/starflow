package starflow

import (
	"context"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type Client[Input proto.Message] struct {
	store Store
}

// NewClient creates a new Workflow executor with the specified input and output types.
func NewClient[Input proto.Message](store Store) *Client[Input] {
	return &Client[Input]{
		store: store,
	}
}

// Run creates a new workflow run with the given script and input, returning the run ID.
func (c *Client[Input]) Run(ctx context.Context, script []byte, input Input) (string, error) {
	// Save script
	scriptHash, err := c.store.SaveScript(ctx, script)
	if err != nil {
		return "", fmt.Errorf("failed to save script: %w", err)
	}

	// Convert input to anypb.Any
	var inputAny *anypb.Any
	inputVal := reflect.ValueOf(input)
	if !inputVal.IsNil() {
		inputAny, err = anypb.New(input)
		if err != nil {
			return "", fmt.Errorf("failed to convert input to anypb.Any: %w", err)
		}
	}

	// Create run
	runID, err := c.store.CreateRun(ctx, scriptHash, inputAny)
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

func (c *Client[Input]) Signal(ctx context.Context, cid string, output *anypb.Any) error {
	return c.store.Signal(ctx, cid, output)
}
