package starflow

import (
	"context"
	"errors"
	"time"

	"github.com/dynoinc/starflow/events"
)

// ErrConcurrentUpdate indicates optimistic concurrency failure.
// This error is returned when a concurrent update to a run is detected,
// typically when multiple workers try to claim the same run simultaneously.
var ErrConcurrentUpdate = errors.New("concurrent update")

// Store is the interface for persisting workflow data.
// Implementations of this interface provide the storage backend for workflow
// scripts, runs, events, and signals. The interface is designed to support
// both in-memory and persistent storage backends like DynamoDB and PostgreSQL.
type Store interface {
	// Scripts - Methods to save/restore scripts.
	//
	// Invariants:
	// - SaveScript verifies that the content hash matches the passed scriptHash.
	// - Saving same script twice succeeds and returns the same hash.
	SaveScript(ctx context.Context, scriptHash string, content []byte) error
	GetScript(ctx context.Context, scriptHash string) ([]byte, error)

	// Runs - Methods to introspect runs.
	//
	// Invariants:
	// - Runs are created by recording a START event. If a START event is recorded for a new runID, the run is created with the provided scriptHash and input.
	// - Recording a START event for an existing runID fails.
	// - GetRun returns a deep copy of the run state at the time of retrieval, ensuring that external modifications to the returned object do not affect the stored run object.
	// - ClaimRuns finds runs that are either in RunStatusPending or in RunStatusRunning with an expired lease, records a ClaimEvent for each, and returns the updated runs.
	// - Run status transitions are strictly defined:
	//   - Pending -> Running (via Claim event)
	//   - Running -> Yielded (via Yield event)
	//   - Running -> Completed (via Finish event)
	//   - Running -> Failed (via Finish event with error)
	//   - Yielded -> Pending (via Resume event)
	GetRun(ctx context.Context, runID string) (*Run, error)
	ClaimRuns(ctx context.Context, workerID string, leaseUntil time.Time) ([]*Run, error)

	// Events - Methods to record events.
	//
	// Invariants:
	// - RecordEvent succeeds iff runID is valid and nextEventID is equal to current nextEventID, or if it is a START event for a new runID.
	// - Events for a given run are recorded sequentially, and NextEventID always reflects the expected next sequence number for an event, incrementing by one upon successful record.
	// - If event is a yield event, run will be updated to be in status RunStatusYielded.
	// - If event is a finish event, run will be updated to be in status RunStatusCompleted with the output.
	// - If event is a finish event with error, run will be updated to be in status RunStatusFailed with the error.
	// - If event is a claim event, run will be updated to be in status RunStatusRunning. A run can be claimed by a worker if:
	//   - Its current status is RunStatusPending.
	//   - Its current status is RunStatusRunning AND it is already leased by the same worker (for lease renewal).
	//   - Its current status is RunStatusRunning AND its lease has expired.
	//   Upon a successful claim event, the run's LeasedBy field is set to the claiming worker's ID and LeasedUntil is set to the provided lease expiry timestamp.
	// - If event is a resume event, run must be in RunStatusYielded and signal ID must exist; run is updated to RunStatusPending and signal is removed.
	// - For all events, the run must be in the correct state for the event type, as per the allowed state transitions.
	RecordEvent(ctx context.Context, runID string, nextEventID int64, eventMetadata events.EventMetadata) (int64, error)
	GetEvents(ctx context.Context, runID string) ([]*events.Event, error)
}
