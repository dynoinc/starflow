package starflow

import (
	"context"
	"time"
)

// Store is the interface for persisting workflow data.
type Store interface {
	// Scripts
	// SaveScript persists the Starlark script content.
	// It returns the sha256 hash of the content, which is used as the script ID.
	SaveScript(content []byte) (string, error)

	// GetScript retrieves a script by its sha256 hash.
	GetScript(scriptHash string) ([]byte, error)

	// Runs
	// CreateRun creates a new run record for a given script.
	CreateRun(scriptHash string, input []byte) (string, error)

	// GetRun retrieves the details of a specific run.
	GetRun(runID string) (*Run, error)

	// ListRuns returns all runs whose status matches any of the supplied states.
	ListRuns(ctx context.Context, statuses ...RunStatus) ([]*Run, error)

	// Events
	// RecordEvent records an event in the execution history of a run.
	RecordEvent(runID string, event *Event) error

	// GetEvents retrieves all events for a specific run, ordered by time.
	GetEvents(runID string) ([]*Event, error)

	// UpdateRunOutput updates the output of a run and typically sets status to COMPLETED.
	UpdateRunOutput(ctx context.Context, runID string, output []byte) error

	// UpdateRunError sets the error message for a run.
	UpdateRunError(ctx context.Context, runID string, errMsg string) error

	// FindEventByCorrelationID retrieves the first event with the given correlationID across all runs.
	// It returns the associated runID together with the event.
	FindEventByCorrelationID(correlationID string) (string, *Event, error)

	// UpdateRunStatusAndRecordEvent performs the following atomically in a single transaction:
	//   1. Insert the supplied event (if not nil)
	//   2. Update the run's status to the supplied value (if status != "")
	//   3. Update wake_at timestamp (may be nil to clear)
	UpdateRunStatusAndRecordEvent(ctx context.Context, runID string, status RunStatus, event *Event, wakeAt *time.Time) error
}
