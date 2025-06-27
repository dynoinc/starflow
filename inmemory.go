package starflow

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dynoinc/starflow/events"
)

// InMemoryStore is an in-memory implementation of the Store interface.
// This store is suitable for testing, development, and single-instance deployments.
// All data is stored in memory and will be lost when the process terminates.
type InMemoryStore struct {
	mu     sync.RWMutex
	events map[string][]*events.Event
}

// NewInMemoryStore creates a new InMemoryStore.
// This is the recommended way to create an in-memory store for testing
// or single-instance workflow execution.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		events: make(map[string][]*events.Event),
	}
}

// RecordEvent records an event for a given run.
func (s *InMemoryStore) RecordEvent(ctx context.Context, runID string, nextEventID int, eventMetadata events.EventMetadata) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if eventMetadata.EventType() == events.EventTypeStart {
		if _, exists := s.events[runID]; exists {
			// Invariant: Start event is only allowed for new runs.
			return 0, fmt.Errorf("run %s already exists", runID)
		}

		event := &events.Event{
			Timestamp: time.Now(),
			Metadata:  eventMetadata,
		}

		s.events[runID] = append(s.events[runID], event)
		return 1, nil
	}

	storedEvents, exists := s.events[runID]
	if !exists {
		// Invariant: All other events are only allowed for existing runs.
		return 0, fmt.Errorf("run %s not found", runID)
	}

	lastEvent := storedEvents[len(storedEvents)-1].Metadata
	if lastEvent.EventType() == events.EventTypeFinish {
		// Invariant: Nothing is allowed after a finish event.
		return 0, fmt.Errorf("run %s has already finished", runID)
	}

	if nextEventID != len(storedEvents) && eventMetadata.EventType() != events.EventTypeResume {
		// Invariant: Event ID must be sequential except for resume events.
		return 0, ErrConcurrentUpdate
	}

	switch e := eventMetadata.(type) {
	case events.ResumeEvent:
		yieldEvent, ok := lastEvent.(events.YieldEvent)
		if !ok {
			// Invariant: Only yield events can be resumed.
			return 0, fmt.Errorf("run %s is not in yielded state", runID)
		}

		if yieldEvent.SignalID() != e.SignalID() {
			// Invariant: The signal ID must match the yield event.
			return 0, fmt.Errorf("signal ID mismatch: %s != %s", yieldEvent.SignalID(), e.SignalID())
		}
	case events.YieldEvent:
		if lastEvent.EventType() != events.EventTypeCall {
			// Invariant: Only call events can be yielded.
			return 0, fmt.Errorf("invalid event type: %s -> %s not allowed", lastEvent.EventType(), eventMetadata.EventType())
		}
	case events.ReturnEvent:
		if lastEvent.EventType() != events.EventTypeCall {
			// Invariant: Only call events can be returned.
			return 0, fmt.Errorf("invalid event type: %s -> %s not allowed", lastEvent.EventType(), eventMetadata.EventType())
		}
	default:
		if lastEvent.EventType() == events.EventTypeCall {
			// Invariant: Only yield or return events are allowed after a call event.
			return 0, fmt.Errorf("invalid event type: %s -> %s not allowed", lastEvent.EventType(), eventMetadata.EventType())
		}
		if lastEvent.EventType() == events.EventTypeYield {
			// Invariant: Only resume events are allowed after a yield event.
			return 0, fmt.Errorf("invalid event type: %s -> %s not allowed", lastEvent.EventType(), eventMetadata.EventType())
		}
	}

	s.events[runID] = append(s.events[runID], &events.Event{
		Timestamp: time.Now(),
		Metadata:  eventMetadata,
	})

	return len(s.events[runID]), nil
}

// GetEvents retrieves all events for a specific run, ordered by time.
func (s *InMemoryStore) GetEvents(ctx context.Context, runID string) ([]*events.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	runEvents, exists := s.events[runID]
	if !exists {
		return nil, fmt.Errorf("run with ID %s not found", runID)
	}

	// Return a copy to avoid external modifications
	result := make([]*events.Event, len(runEvents))
	copy(result, runEvents)
	return result, nil
}
