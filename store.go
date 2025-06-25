package starflow

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
)

// ErrConcurrentUpdate indicates optimistic concurrency failure.
var ErrConcurrentUpdate = errors.New("concurrent update")

// Store is the interface for persisting workflow data.
type Store interface {
	// Scripts - Methods to save/restore scripts.
	//
	// Invariants:
	// - Saving same script twice succeeds and returns the same hash.
	SaveScript(ctx context.Context, content []byte) (string, error)
	GetScript(ctx context.Context, scriptHash string) ([]byte, error)

	// Runs - Methods to create/introspect/claim runs.
	//
	// Invariants:
	// - Creating a run with a non-existent script hash fails.
	// - Claim run succeeds iff workerID is empty or leaseUntil is in the past or workerID is the same as the current worker ID.
	CreateRun(ctx context.Context, scriptHash string, input *anypb.Any) (string, error)
	GetRun(ctx context.Context, runID string) (*Run, error)
	ClaimRun(ctx context.Context, runID string, workerID string, leaseUntil time.Time) (bool, error)
	ListRuns(ctx context.Context, statuses ...RunStatus) ([]*Run, error)

	// Signals - Methods to signal a run.
	//
	// Invariants:
	// - Signaling a run with a non-existent signal ID or signaling twice with the same signal ID fails.
	// - Signaling a run with a non-existent run ID fails.
	// - Run will be updated to be in status RunStatusPending and workerID will be cleared.
	Signal(ctx context.Context, cid string, output *anypb.Any) error

	// Events - Methods to record events.
	//
	// Invariants:
	// - RecordEvent succeeds iff runID is valid and nextEventID is equal to current nextEventID.
	// - If event is a return event with error, run will be updated to be in status RunStatusFailed.
	// - If event is a yield event, run will be updated to be in status RunStatusYielded.
	// - If event is a finish event, run will be updated to be in status RunStatusCompleted with the output.
	RecordEvent(ctx context.Context, runID string, nextEventID int64, eventMetadata EventMetadata) (int64, error)
	GetEvents(ctx context.Context, runID string) ([]*Event, error)
}
