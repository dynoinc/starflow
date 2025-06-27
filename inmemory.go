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
	events map[string][][]byte
}

// NewInMemoryStore creates a new InMemoryStore.
// This is the recommended way to create an in-memory store for testing
// or single-instance workflow execution.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		events: make(map[string][][]byte),
	}
}

// AppendEvent appends an event to a run's history with optimistic concurrency control.
func (s *InMemoryStore) AppendEvent(ctx context.Context, runID string, expectedVersion int, eventData []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	storedEvents := s.events[runID]
	currentVersion := len(storedEvents)

	// Optimistic concurrency control - check version matches
	if currentVersion != expectedVersion {
		return 0, ErrConcurrentUpdate
	}

	// Append the event data
	s.events[runID] = append(s.events[runID], eventData)

	return currentVersion + 1, nil
}

// GetEvents retrieves all events for a specific run, ordered by time.
func (s *InMemoryStore) GetEvents(ctx context.Context, runID string) ([][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	runEvents := s.events[runID]

	// Return a copy to avoid external modifications
	result := make([][]byte, len(runEvents))
	for i, eventData := range runEvents {
		result[i] = make([]byte, len(eventData))
		copy(result[i], eventData)
	}
	return result, nil
}

// GetLastEvent returns the last event for a given run.
func (s *InMemoryStore) GetLastEvent(ctx context.Context, runID string) ([]byte, int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	runEvents := s.events[runID]
	if len(runEvents) == 0 {
		return nil, 0, nil
	}

	lastEventData := runEvents[len(runEvents)-1]
	// Return a copy to avoid external modifications
	result := make([]byte, len(lastEventData))
	copy(result, lastEventData)

	return result, len(runEvents), nil
}
