package starflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/lithammer/shortuuid/v4"
	"google.golang.org/protobuf/types/known/anypb"
)

// InMemoryStore is an in-memory implementation of the Store interface.
type InMemoryStore struct {
	mu      sync.RWMutex
	scripts map[string][]byte
	runs    map[string]*Run
	events  map[string][]*Event
	yields  map[string]string
}

// NewInMemoryStore creates a new InMemoryStore.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		scripts: make(map[string][]byte),
		runs:    make(map[string]*Run),
		events:  make(map[string][]*Event),
		yields:  make(map[string]string),
	}
}

// SaveScript persists the Starlark script content.
func (s *InMemoryStore) SaveScript(ctx context.Context, content []byte) (string, error) {
	hash := sha256.Sum256(content)
	hashStr := hex.EncodeToString(hash[:])

	s.mu.Lock()
	defer s.mu.Unlock()

	s.scripts[hashStr] = content
	return hashStr, nil
}

// GetScript retrieves a script by its sha256 hash.
func (s *InMemoryStore) GetScript(ctx context.Context, scriptHash string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	content, exists := s.scripts[scriptHash]
	if !exists {
		return nil, fmt.Errorf("script with hash %s not found", scriptHash)
	}
	return content, nil
}

// CreateRun creates a new run record for a given script.
func (s *InMemoryStore) CreateRun(ctx context.Context, scriptHash string, input *anypb.Any) (string, error) {
	runID := shortuuid.New()
	now := time.Now()

	run := &Run{
		ID:          runID,
		ScriptHash:  scriptHash,
		Status:      RunStatusPending,
		Input:       input,
		NextEventID: 0,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.scripts[scriptHash]; !exists {
		return "", fmt.Errorf("script with hash %s not found", scriptHash)
	}

	s.runs[runID] = run
	s.events[runID] = make([]*Event, 0)
	return runID, nil
}

// GetRun retrieves the details of a specific run.
func (s *InMemoryStore) GetRun(ctx context.Context, runID string) (*Run, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	run, exists := s.runs[runID]
	if !exists {
		return nil, fmt.Errorf("run with ID %s not found", runID)
	}

	// Return a copy to avoid external modifications
	runCopy := *run
	return &runCopy, nil
}

// ClaimableRuns retrieves runs that are either pending or haven't been updated recently.
func (s *InMemoryStore) ClaimableRuns(ctx context.Context, staleThreshold time.Duration) ([]*Run, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var runs []*Run
	staleTime := time.Now().Add(-staleThreshold)

	for _, run := range s.runs {
		if run.Status == RunStatusPending ||
			(run.Status == RunStatusRunning && run.UpdatedAt.Before(staleTime)) ||
			(run.Status == RunStatusYielded && run.UpdatedAt.Before(staleTime)) {
			// Create a copy to avoid external modifications
			runCopy := *run
			runs = append(runs, &runCopy)
		}
	}

	return runs, nil
}

// RecordEvent records an event. It succeeds only if run.NextEventID==expectedNextID.
// On success the store increments NextEventID by one.
func (s *InMemoryStore) RecordEvent(ctx context.Context, runID string, nextEventID int64, eventMetadata EventMetadata) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	storedRun, exists := s.runs[runID]
	if !exists {
		return 0, fmt.Errorf("run with ID %s not found", runID)
	}

	// Check optimistic concurrency precondition
	if storedRun.NextEventID != nextEventID {
		return 0, ErrConcurrentUpdate
	}

	// Add event to the list
	event := &Event{
		Timestamp: time.Now(),
		Type:      eventMetadata.EventType(),
		Metadata:  eventMetadata,
	}

	s.events[runID] = append(s.events[runID], event)

	// Update the runs
	switch event.Type {
	case EventTypeReturn:
		if returnEvent, ok := event.Metadata.(ReturnEvent); ok && returnEvent.Error != nil {
			storedRun.Status = RunStatusFailed
			storedRun.Error = returnEvent.Error
		}
	case EventTypeYield:
		if yieldEvent, ok := event.Metadata.(YieldEvent); ok {
			storedRun.Status = RunStatusYielded
			s.yields[yieldEvent.SignalID] = runID
		}
	case EventTypeFinish:
		if finishEvent, ok := event.Metadata.(FinishEvent); ok {
			storedRun.Status = RunStatusCompleted
			storedRun.Output = finishEvent.Output
		}
	case EventTypeClaim:
		storedRun.Status = RunStatusRunning
	}

	storedRun.NextEventID++
	storedRun.UpdatedAt = time.Now()
	return storedRun.NextEventID, nil
}

// Signal resumes a yielded workflow with the given output.
func (s *InMemoryStore) Signal(ctx context.Context, cid string, output *anypb.Any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	runID, exists := s.yields[cid]
	if !exists {
		return fmt.Errorf("signal with ID %s not found", cid)
	}

	run, exists := s.runs[runID]
	if !exists {
		return fmt.Errorf("run with ID %s not found", runID)
	}

	s.events[runID] = append(s.events[runID], &Event{
		Type:     EventTypeResume,
		Metadata: ResumeEvent{SignalID: cid, Output: output},
	})

	run.NextEventID++
	run.Status = RunStatusPending
	run.UpdatedAt = time.Now()

	delete(s.yields, cid)
	return nil
}

// GetEvents retrieves all events for a specific run, ordered by time.
func (s *InMemoryStore) GetEvents(ctx context.Context, runID string) ([]*Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	events, exists := s.events[runID]
	if !exists {
		return nil, fmt.Errorf("run with ID %s not found", runID)
	}

	// Return a copy to avoid external modifications
	result := make([]*Event, len(events))
	copy(result, events)
	return result, nil
}
