package starflow

import (
	"bytes"
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

	if currentVersion != expectedVersion {
		return 0, ErrConcurrentUpdate
	}

	s.events[runID] = append(s.events[runID], eventData)
	return currentVersion + 1, nil
}

// GetEvents retrieves all events for a specific run, ordered by time.
func (s *InMemoryStore) GetEvents(ctx context.Context, runID string) ([][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	runEvents := s.events[runID]
	result := make([][]byte, len(runEvents))
	for i, eventData := range runEvents {
		result[i] = bytes.Clone(eventData)
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
	return bytes.Clone(lastEventData), len(runEvents), nil
}
