package starflow_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dynoinc/starflow"
	"github.com/lithammer/shortuuid/v4"
)

// MemoryStore is an in-memory implementation of the Store interface.
type MemoryStore struct {
	mu      sync.RWMutex
	scripts map[string][]byte
	runs    map[string]*starflow.Run
	events  map[string][]*starflow.Event
}

// NewMemoryStore creates a new MemoryStore.
func NewMemoryStore(t *testing.T) *MemoryStore {
	return &MemoryStore{
		scripts: make(map[string][]byte),
		runs:    make(map[string]*starflow.Run),
		events:  make(map[string][]*starflow.Event),
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
func (s *MemoryStore) CreateRun(ctx context.Context, scriptHash string, input []byte) (string, error) {
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

// ClaimRun attempts to mark a run as RUNNING with worker id and lease. Returns true if successful.
func (s *MemoryStore) ClaimRun(ctx context.Context, runID string, workerID string, leaseUntil time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	run, exists := s.runs[runID]
	if !exists {
		return false, fmt.Errorf("run with ID %s not found", runID)
	}

	// Check if run is already claimed and lease is still valid
	if run.WorkerID != "" && run.LeaseUntil != nil && time.Now().Before(*run.LeaseUntil) {
		return false, nil
	}

	// Claim the run
	run.Status = starflow.RunStatusRunning
	run.WorkerID = workerID
	run.LeaseUntil = &leaseUntil
	run.UpdatedAt = time.Now()

	return true, nil
}

// RecordEvent records an event. It succeeds only if run.NextEventID==expectedNextID.
// On success the store increments NextEventID by one.
func (s *MemoryStore) RecordEvent(ctx context.Context, runID string, nextEventID int64, event *starflow.Event) (int64, error) {
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
	s.events[runID] = append(s.events[runID], event)

	// Update the runs
	if event.Type == starflow.EventTypeReturn {
		if returnEvent, ok := event.AsReturnEvent(); ok && returnEvent.Error != nil {
			storedRun.Status = starflow.RunStatusFailed
			storedRun.Error = returnEvent.Error
		}
	}

	storedRun.NextEventID++
	storedRun.UpdatedAt = time.Now()
	return storedRun.NextEventID, nil
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

// FinishRun updates the output of a run and typically sets status to COMPLETED.
func (s *MemoryStore) FinishRun(ctx context.Context, runID string, output []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	run, exists := s.runs[runID]
	if !exists {
		return fmt.Errorf("run with ID %s not found", runID)
	}

	run.Output = output
	run.Status = starflow.RunStatusCompleted
	run.UpdatedAt = time.Now()
	return nil
}

func TestMemoryStore_Suite(t *testing.T) {
	runStoreSuite(t, func(t *testing.T) starflow.Store { return NewMemoryStore(t) })
}
