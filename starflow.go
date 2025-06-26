// Package starflow provides a workflow engine for Go that enables deterministic,
// resumable, and distributed workflow execution using Starlark scripting. Every
// execution step is recorded and can be resumed exactly where it left off.
//
// # Key Features
//
//   - Deterministic & Durable Workflows: Write workflows that are deterministic
//     and can be replayed from any point with full durability guarantees
//   - Pluggable Backends: Support for in-memory, DynamoDB, and PostgreSQL storage
//     with easy extensibility for custom backends
//   - Distributed Execution: Multiple workers can process workflow runs concurrently
//   - Resumable Workflows: Workflows can yield and resume based on external signals
//
// For more information, see https://github.com/dynoinc/starflow
package starflow

import (
	"context"
	"fmt"
	"time"

	"github.com/lithammer/shortuuid/v4"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/dynoinc/starflow/events"
)

// Context key for runID
type runIDKey struct{}

// WithRunID adds runID to the context
func WithRunID(ctx context.Context, runID string) context.Context {
	return context.WithValue(ctx, runIDKey{}, runID)
}

// GetRunID extracts runID from context
func GetRunID(ctx context.Context) (string, bool) {
	runID, ok := ctx.Value(runIDKey{}).(string)
	return runID, ok
}

type YieldError struct {
	cid   string
	runID string
}

func (e *YieldError) Error() string {
	return fmt.Sprintf("yield error: %s (run: %s)", e.cid, e.runID)
}

func NewYieldError(ctx context.Context) (string, string, error) {
	runID, ok := GetRunID(ctx)
	if !ok {
		return "", "", fmt.Errorf("runID not found in context")
	}
	cid := shortuuid.New()
	return runID, cid, &YieldError{cid: cid, runID: runID}
}

func YieldErrorFrom(yieldEvent events.YieldEvent) *YieldError {
	return &YieldError{cid: yieldEvent.SignalID(), runID: yieldEvent.RunID()}
}

// RunStatus represents the status of a workflow run.
type RunStatus string

const (
	// RunStatusRunning indicates that the workflow is currently running.
	RunStatusRunning RunStatus = "RUNNING"
	// RunStatusCompleted indicates that the workflow has completed successfully.
	RunStatusCompleted RunStatus = "COMPLETED"
	// RunStatusFailed indicates that the workflow has failed.
	RunStatusFailed RunStatus = "FAILED"
	// RunStatusPending indicates that the workflow has been created and is waiting to be picked up by a worker.
	RunStatusPending RunStatus = "PENDING"
	// RunStatusYielded indicates that the workflow has yielded and waiting for signal to resume.
	RunStatusYielded RunStatus = "YIELDED"
)

// Run represents a single execution of a workflow.
type Run struct {
	// Fixed on creation
	ID         string
	ScriptHash string
	Input      *anypb.Any
	CreatedAt  time.Time

	// Updated at each event
	Status      RunStatus
	NextEventID int64
	UpdatedAt   time.Time

	// Set when finished.
	Output *anypb.Any
	Error  error

	// Lease
	LeasedBy    string
	LeasedUntil time.Time
}
