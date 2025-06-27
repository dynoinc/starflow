package starflow

import (
	"context"
	"sync"
)

// InMemoryStore is an in-memory implementation of the Store interface.
// This store is suitable for testing, development, and single-instance deployments.
// All data is stored in memory and will be lost when the process terminates.
type InMemoryStore struct {
	mu     sync.RWMutex
	events map[string][]*Event
}

// NewInMemoryStore creates a new InMemoryStore.
// This is the recommended way to create an in-memory store for testing
// or single-instance workflow execution.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		events: make(map[string][]*Event),
	}
}

// AppendEvent appends an event to a run's history with optimistic concurrency control.
func (s *InMemoryStore) AppendEvent(ctx context.Context, runID string, expectedVersion int, event *Event) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	storedEvents := s.events[runID]
	currentVersion := len(storedEvents)

	// Optimistic concurrency control - check version matches
	if currentVersion != expectedVersion {
		return 0, ErrConcurrentUpdate
	}

	// Append the event
	s.events[runID] = append(s.events[runID], event)

	return currentVersion + 1, nil
}

// GetEvents retrieves all events for a specific run, ordered by time.
func (s *InMemoryStore) GetEvents(ctx context.Context, runID string) ([]*Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	runEvents := s.events[runID]

	// Return a copy to avoid external modifications
	result := make([]*Event, len(runEvents))
	copy(result, runEvents)
	return result, nil
}
