package starflow

import (
	"context"
	"errors"
	"time"
)

// ErrConcurrentUpdate indicates optimistic concurrency failure.
var ErrConcurrentUpdate = errors.New("concurrent update")

// Store is the interface for persisting workflow data.
type Store interface {
	// Scripts
	//
	// Methods to save/restore scripts. Saving same script twice succeeds and returns the same hash.
	SaveScript(ctx context.Context, content []byte) (string, error)
	GetScript(ctx context.Context, scriptHash string) ([]byte, error)

	// Runs
	//
	// Methods to create/introspect/claim runs.
	CreateRun(ctx context.Context, scriptHash string, input []byte) (string, error)
	GetRun(ctx context.Context, runID string) (*Run, error)
	ClaimRun(ctx context.Context, runID string, workerID string, leaseUntil time.Time) (bool, error)
	FinishRun(ctx context.Context, runID string, output []byte) error
	ListRuns(ctx context.Context, statuses ...RunStatus) ([]*Run, error)

	// Events
	//
	// Methods to record events.
	RecordEvent(ctx context.Context, runID string, nextEventID int64, event *Event) (int64, error)
	GetEvents(ctx context.Context, runID string) ([]*Event, error)
}
