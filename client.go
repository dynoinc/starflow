package starflow

import (
	"context"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"go.starlark.net/syntax"

	"github.com/dynoinc/starflow/events"
)

// Client provides an interface for creating and managing workflow runs.
type Client[Input proto.Message] struct {
	store Store
}

// NewClient creates a new workflow client with the specified input type.
// The client uses the provided store for persistence and workflow management.
func NewClient[Input proto.Message](store Store) *Client[Input] {
	return &Client[Input]{
		store: store,
	}
}

// validateScript performs validation on the Starlark script.
// It checks for syntax errors and ensures the script has a main function.
func (c *Client[Input]) validateScript(script []byte) error {
	// Parse the script to check for syntax errors
	parsed, err := (&syntax.FileOptions{}).Parse("script", script, 0)
	if err != nil {
		return fmt.Errorf("script syntax error: %w", err)
	}

	for _, stmt := range parsed.Stmts {
		if defStmt, ok := stmt.(*syntax.DefStmt); ok {
			if defStmt.Name.Name == "main" {
				return nil
			}
		}
	}

	return fmt.Errorf("script must contain a main function")
}

// Run creates a new workflow run with the given script and input, returning the run ID.
// The script should be valid Starlark code with a main function that accepts context and input parameters.
// The input is serialized as a protobuf Any message for storage and execution.
func (c *Client[Input]) Run(ctx context.Context, script []byte, input Input) (string, error) {
	// Validate script before saving
	if err := c.validateScript(script); err != nil {
		return "", fmt.Errorf("script validation failed: %w", err)
	}

	// Save script
	scriptHash, err := c.store.SaveScript(ctx, script)
	if err != nil {
		return "", fmt.Errorf("failed to save script: %w", err)
	}

	return c.createRunWithHash(ctx, scriptHash, input)
}

// createRunWithHash creates a run with the given script hash and input
func (c *Client[Input]) createRunWithHash(ctx context.Context, scriptHash string, input Input) (string, error) {
	// Convert input to anypb.Any
	var inputAny *anypb.Any
	inputVal := reflect.ValueOf(input)
	if !inputVal.IsNil() {
		var err error
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

// GetRun retrieves a workflow run by its ID.
// Returns the run details including status, input, output, and error information.
func (c *Client[Input]) GetRun(ctx context.Context, runID string) (*Run, error) {
	return c.store.GetRun(ctx, runID)
}

// GetEvents retrieves the execution history of a workflow run.
// Returns a chronological list of events that occurred during execution.
func (c *Client[Input]) GetEvents(ctx context.Context, runID string) ([]*events.Event, error) {
	return c.store.GetEvents(ctx, runID)
}

// Signal resumes a yielded workflow run with the provided output.
// The cid parameter should match the signal ID from the yield event.
func (c *Client[Input]) Signal(ctx context.Context, runID, cid string, output *anypb.Any) error {
	return c.store.Signal(ctx, runID, cid, output)
}
