package starflow

import (
	"context"
)

// Store is the interface for persisting workflow data.
// This is a simple append-only interface with optimistic concurrency control.
type Store interface {
	// AppendEvent appends an event to a run's history.
	// expectedVersion should match the current number of events for the run.
	// Returns the new version (number of events) after append.
	// Returns ErrConcurrentUpdate if expectedVersion doesn't match current version.
	AppendEvent(ctx context.Context, runID string, expectedVersion int, event *Event) (int, error)

	// GetEvents returns the events for a given run in the order they were recorded.
	GetEvents(ctx context.Context, runID string) ([]*Event, error)

	// GetLastEvent returns the last event and version for a given run.
	GetLastEvent(ctx context.Context, runID string) (*Event, int, error)
}
