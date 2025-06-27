package starflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"go.starlark.net/syntax"

	"github.com/lithammer/shortuuid/v4"

	"github.com/dynoinc/starflow/events"
)

// Client provides an interface for creating and managing workflow runs.
type Client[Input proto.Message] struct {
	store       Store
	scriptCache sync.Map // map[string]struct{} (scriptHash -> struct{})
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

func (c *Client[Input]) validateAndSave(ctx context.Context, script []byte) (string, error) {
	hash := sha256.Sum256(script)
	scriptHash := hex.EncodeToString(hash[:])

	if _, ok := c.scriptCache.Load(scriptHash); ok {
		return scriptHash, nil
	}

	if err := c.validateScript(script); err != nil {
		return "", fmt.Errorf("script validation failed: %w", err)
	}

	err := c.store.SaveScript(ctx, scriptHash, script)
	if err != nil {
		return "", fmt.Errorf("failed to save script: %w", err)
	}

	c.scriptCache.Store(scriptHash, struct{}{})
	return scriptHash, nil
}

// Run creates a new workflow run with a script, and input, returning the run ID.
func (c *Client[Input]) Run(ctx context.Context, script []byte, input Input) (string, error) {
	scriptHash, err := c.validateAndSave(ctx, script)
	if err != nil {
		return "", fmt.Errorf("failed to validate and save script: %w", err)
	}

	var inputAny *anypb.Any
	inputVal := reflect.ValueOf(input)
	if !inputVal.IsNil() {
		inputAny, err = anypb.New(input)
		if err != nil {
			return "", fmt.Errorf("failed to convert input to anypb.Any: %w", err)
		}
	}

	runID := shortuuid.New()
	if err := c.store.CreateRun(ctx, runID, scriptHash, inputAny); err != nil {
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
	run, err := c.store.GetRun(ctx, runID)
	if err != nil {
		return err
	}

	resumeEvent := events.NewResumeEvent(cid, output)
	_, err = c.store.RecordEvent(ctx, runID, run.NextEventID, resumeEvent)
	return err
}
