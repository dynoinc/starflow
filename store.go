package starflow

import (
	"context"
)

// Store is the interface for persisting workflow data.
type Store interface {
	// RecordEvent records an event for a given run.
	RecordEvent(ctx context.Context, runID string, nextEventID int, eventMetadata EventMetadata) (int, error)

	// GetEvents returns the events for a given run in the order they were recorded.
	GetEvents(ctx context.Context, runID string) ([]*Event, error)
}
