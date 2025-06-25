package starflow_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lithammer/shortuuid/v4"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/dynoinc/starflow"
	"github.com/dynoinc/starflow/suite"
)

// MemoryStore is an in-memory implementation of the Store interface.
type MemoryStore struct {
	mu      sync.RWMutex
	scripts map[string][]byte
	runs    map[string]*starflow.Run
	events  map[string][]*starflow.Event
	yields  map[string]string
}

// New creates a new MemoryStore.
func NewInMemoryStore() *MemoryStore {
	return &MemoryStore{
		scripts: make(map[string][]byte),
		runs:    make(map[string]*starflow.Run),
		events:  make(map[string][]*starflow.Event),
		yields:  make(map[string]string),
	}
}

// SaveScript persists the Starlark script content.
func (s *MemoryStore) SaveScript(ctx context.Context, content []byte) (string, error) {
	hash := sha256.Sum256(content)
	hashStr := hex.EncodeToString(hash[:])

	s.mu.Lock()
	defer s.mu.Unlock()

	s.scripts[hashStr] = content
	return hashStr, nil
}

// GetScript retrieves a script by its sha256 hash.
func (s *MemoryStore) GetScript(ctx context.Context, scriptHash string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	content, exists := s.scripts[scriptHash]
	if !exists {
		return nil, fmt.Errorf("script with hash %s not found", scriptHash)
	}
	return content, nil
}

// CreateRun creates a new run record for a given script.
func (s *MemoryStore) CreateRun(ctx context.Context, scriptHash string, input *anypb.Any) (string, error) {
	runID := shortuuid.New()
	now := time.Now()

	run := &starflow.Run{
		ID:          runID,
		ScriptHash:  scriptHash,
		Status:      starflow.RunStatusPending,
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
	s.events[runID] = make([]*starflow.Event, 0)
	return runID, nil
}

// GetRun retrieves the details of a specific run.
func (s *MemoryStore) GetRun(ctx context.Context, runID string) (*starflow.Run, error) {
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

// ListRuns returns all runs whose status matches any of the supplied states.
func (s *MemoryStore) ListRuns(ctx context.Context, statuses ...starflow.RunStatus) ([]*starflow.Run, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*starflow.Run
	statusSet := make(map[starflow.RunStatus]bool)
	for _, status := range statuses {
		statusSet[status] = true
	}

	for _, run := range s.runs {
		if len(statuses) == 0 || statusSet[run.Status] {
			runCopy := *run
			result = append(result, &runCopy)
		}
	}

	return result, nil
}

// ListRunsForClaiming retrieves runs that are either pending or haven't been updated recently.
func (s *MemoryStore) ListRunsForClaiming(ctx context.Context, staleThreshold time.Duration) ([]*starflow.Run, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var runs []*starflow.Run
	staleTime := time.Now().Add(-staleThreshold)

	for _, run := range s.runs {
		if run.Status == starflow.RunStatusPending ||
			(run.Status == starflow.RunStatusRunning && run.UpdatedAt.Before(staleTime)) ||
			(run.Status == starflow.RunStatusYielded && run.UpdatedAt.Before(staleTime)) {
			// Create a copy to avoid external modifications
			runCopy := *run
			runs = append(runs, &runCopy)
		}
	}

	return runs, nil
}

// RecordEvent records an event. It succeeds only if run.NextEventID==expectedNextID.
// On success the store increments NextEventID by one.
func (s *MemoryStore) RecordEvent(ctx context.Context, runID string, nextEventID int64, eventMetadata starflow.EventMetadata) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	storedRun, exists := s.runs[runID]
	if !exists {
		return 0, fmt.Errorf("run with ID %s not found", runID)
	}

	// Check optimistic concurrency precondition
	if storedRun.NextEventID != nextEventID {
		return 0, starflow.ErrConcurrentUpdate
	}

	// Add event to the list
	event := &starflow.Event{
		Timestamp: time.Now(),
		Type:      eventMetadata.EventType(),
		Metadata:  eventMetadata,
	}

	s.events[runID] = append(s.events[runID], event)

	// Update the runs
	switch event.Type {
	case starflow.EventTypeReturn:
		if returnEvent, ok := event.Metadata.(starflow.ReturnEvent); ok && returnEvent.Error != nil {
			storedRun.Status = starflow.RunStatusFailed
			storedRun.Error = returnEvent.Error
		}
	case starflow.EventTypeYield:
		if yieldEvent, ok := event.Metadata.(starflow.YieldEvent); ok {
			storedRun.Status = starflow.RunStatusYielded
			s.yields[yieldEvent.SignalID] = runID
		}
	case starflow.EventTypeFinish:
		if finishEvent, ok := event.Metadata.(starflow.FinishEvent); ok {
			storedRun.Status = starflow.RunStatusCompleted
			storedRun.Output = finishEvent.Output
		}
	case starflow.EventTypeClaim:
		storedRun.Status = starflow.RunStatusRunning
	}

	storedRun.NextEventID++
	storedRun.UpdatedAt = time.Now()
	return storedRun.NextEventID, nil
}

func (s *MemoryStore) Signal(ctx context.Context, cid string, output *anypb.Any) error {
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

	s.events[runID] = append(s.events[runID], &starflow.Event{
		Type:     starflow.EventTypeResume,
		Metadata: starflow.ResumeEvent{SignalID: cid, Output: output},
	})

	run.NextEventID++
	run.Status = starflow.RunStatusPending
	run.UpdatedAt = time.Now()

	delete(s.yields, cid)
	return nil
}

// GetEvents retrieves all events for a specific run, ordered by time.
func (s *MemoryStore) GetEvents(ctx context.Context, runID string) ([]*starflow.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	events, exists := s.events[runID]
	if !exists {
		return nil, fmt.Errorf("run with ID %s not found", runID)
	}

	// Return a copy to avoid external modifications
	result := make([]*starflow.Event, len(events))
	copy(result, events)
	return result, nil
}

func TestInMemoryStore(t *testing.T) {
	suite.RunStoreSuite(t, func(t *testing.T) starflow.Store {
		return NewInMemoryStore()
	})
}
