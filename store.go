package starflow

import (
	"context"
)

// Store is the interface for persisting workflow data.
type Store interface {
	// SaveScript persists the Starlark script content.
	// It returns the sha256 hash of the content, which is used as the script ID.
	SaveScript(content []byte) (string, error)

	// GetScript retrieves a script by its sha256 hash.
	GetScript(scriptHash string) ([]byte, error)

	// CreateRun creates a new run record for a given script.
	CreateRun(scriptHash string, input []byte) (string, error)

	// GetRun retrieves the details of a specific run.
	GetRun(runID string) (*Run, error)

	// RecordEvent records an event in the execution history of a run.
	RecordEvent(runID string, event *Event) error

	// GetEvents retrieves all events for a specific run, ordered by time.
	GetEvents(runID string) ([]*Event, error)

	// UpdateRunStatus updates the status of a run.
	UpdateRunStatus(ctx context.Context, runID string, status RunStatus) error

	// UpdateRunOutput updates the output of a run.
	UpdateRunOutput(ctx context.Context, runID string, output []byte) error

	// ListRuns returns all runs whose status matches any of the supplied states.
	ListRuns(ctx context.Context, statuses ...RunStatus) ([]*Run, error)

	// GetEventByCorrelationID retrieves an event matching the given correlation ID for the run.
	GetEventByCorrelationID(runID string, correlationID string) (*Event, error)
}
