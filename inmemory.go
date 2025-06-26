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

	"github.com/dynoinc/starflow/events"
)

// InMemoryStore is an in-memory implementation of the Store interface.
// This store is suitable for testing, development, and single-instance deployments.
// All data is stored in memory and will be lost when the process terminates.
//
// The store uses optimistic concurrency control to handle concurrent access
// and provides thread-safe operations for all Store interface methods.
type InMemoryStore struct {
	mu      sync.RWMutex
	scripts map[string][]byte
	runs    map[string]*Run
	events  map[string][]*events.Event
	yields  map[string]string
}

// NewInMemoryStore creates a new InMemoryStore.
// This is the recommended way to create an in-memory store for testing
// or single-instance workflow execution.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		scripts: make(map[string][]byte),
		runs:    make(map[string]*Run),
		events:  make(map[string][]*events.Event),
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
	s.events[runID] = make([]*events.Event, 0)
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

// ClaimableRuns retrieves runs that are either pending or their lease has expired.
func (s *InMemoryStore) ClaimableRuns(ctx context.Context) ([]*Run, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var runs []*Run
	now := time.Now()

	for _, run := range s.runs {
		if run.Status == RunStatusPending ||
			(run.Status == RunStatusRunning && run.LeasedUntil.Before(now)) {
			runCopy := *run
			runs = append(runs, &runCopy)
		}
	}

	return runs, nil
}

// RecordEvent records an event. It succeeds only if run.NextEventID==expectedNextID.
// On success the store increments NextEventID by one.
func (s *InMemoryStore) RecordEvent(ctx context.Context, runID string, nextEventID int64, eventMetadata events.EventMetadata) (int64, error) {
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
	event := &events.Event{
		Timestamp: time.Now(),
		Metadata:  eventMetadata,
	}

	s.events[runID] = append(s.events[runID], event)

	// Update the runs
	switch event.Type() {
	case events.EventTypeReturn:
		if returnEvent, ok := event.Metadata.(events.ReturnEvent); ok {
			_, err := returnEvent.Output()
			if err != nil {
				storedRun.Status = RunStatusFailed
				storedRun.Error = err
			}
		}
	case events.EventTypeYield:
		if yieldEvent, ok := event.Metadata.(events.YieldEvent); ok {
			storedRun.Status = RunStatusYielded
			s.yields[yieldEvent.SignalID()] = runID
		}
	case events.EventTypeFinish:
		if finishEvent, ok := event.Metadata.(events.FinishEvent); ok {
			storedRun.Status = RunStatusCompleted
			storedRun.Output = finishEvent.Output()
		}
	case events.EventTypeClaim:
		if claimEvent, ok := event.Metadata.(events.ClaimEvent); ok {
			storedRun.Status = RunStatusRunning
			storedRun.LeasedBy = claimEvent.WorkerID()
			storedRun.LeasedUntil = claimEvent.Until()
		}
	}

	storedRun.NextEventID++
	storedRun.UpdatedAt = time.Now()
	return storedRun.NextEventID, nil
}

// Signal resumes a yielded workflow with the given output.
func (s *InMemoryStore) Signal(ctx context.Context, runID, cid string, output *anypb.Any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if run exists
	run, exists := s.runs[runID]
	if !exists {
		// Invariant: Signaling with non-existent run ID succeeds silently
		return nil
	}

	// Check if signal exists
	_, signalExists := s.yields[cid]
	if !signalExists {
		// Invariant: Signaling with non-existent signal ID succeeds silently
		return nil
	}

	// Valid signal - add resume event and update run
	s.events[runID] = append(s.events[runID], &events.Event{
		Metadata: events.NewResumeEvent(cid, output),
	})

	run.NextEventID++
	run.Status = RunStatusPending
	run.UpdatedAt = time.Now()

	delete(s.yields, cid)
	return nil
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
