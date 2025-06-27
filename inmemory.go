package starflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

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
func (s *InMemoryStore) SaveScript(ctx context.Context, scriptHash string, content []byte) error {
	hash := sha256.Sum256(content)
	computedHash := hex.EncodeToString(hash[:])

	if computedHash != scriptHash {
		return fmt.Errorf("content hash %s does not match provided scriptHash %s", computedHash, scriptHash)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.scripts[scriptHash] = content
	return nil
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

// ClaimRuns finds runs that are either in RunStatusPending or in RunStatusRunning with an expired lease,
// records a ClaimEvent for each, and returns the updated runs.
func (s *InMemoryStore) ClaimRuns(ctx context.Context, workerID string, leaseUntil time.Time) ([]*Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var claimedRuns []*Run
	now := time.Now()

	for _, run := range s.runs {
		if run.Status == RunStatusPending ||
			(run.Status == RunStatusRunning && run.LeasedUntil.Before(now)) {

			// Create and record the claim event.
			claimEvent := events.NewClaimEvent(workerID, leaseUntil)
			event := &events.Event{
				Timestamp: now,
				Metadata:  claimEvent,
			}
			s.events[run.ID] = append(s.events[run.ID], event)

			// Update the run state.
			run.Status = RunStatusRunning
			run.LeasedBy = workerID
			run.LeasedUntil = leaseUntil
			run.NextEventID++
			run.UpdatedAt = now

			// Add a copy to the result.
			runCopy := *run
			claimedRuns = append(claimedRuns, &runCopy)
		}
	}

	return claimedRuns, nil
}

// RecordEvent records an event.
// It succeeds only if run.NextEventID==expectedNextID and the run is in the correct state for the event type.
// On success the store increments NextEventID by one.
func (s *InMemoryStore) RecordEvent(ctx context.Context, runID string, nextEventID int64, eventMetadata events.EventMetadata) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Handle START event: create the run if it does not exist
	if eventMetadata.EventType() == events.EventTypeStart {
		if _, exists := s.runs[runID]; exists {
			return 0, fmt.Errorf("run with ID %s already exists", runID)
		}
		startEvent, ok := eventMetadata.(events.StartEvent)
		if !ok {
			return 0, fmt.Errorf("invalid start event metadata")
		}
		if _, exists := s.scripts[startEvent.ScriptHash()]; !exists {
			return 0, fmt.Errorf("script with hash %s not found", startEvent.ScriptHash())
		}
		now := time.Now()
		run := &Run{
			ID:          runID,
			ScriptHash:  startEvent.ScriptHash(),
			Input:       startEvent.Input(),
			Status:      RunStatusPending,
			NextEventID: 0,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		s.runs[runID] = run
		s.events[runID] = make([]*events.Event, 0)
		// Add the START event as the first event
		event := &events.Event{
			Timestamp: now,
			Metadata:  eventMetadata,
		}
		s.events[runID] = append(s.events[runID], event)
		run.NextEventID++
		run.UpdatedAt = now
		return run.NextEventID, nil
	}

	// For all other events, run must exist
	storedRun, exists := s.runs[runID]
	if !exists {
		return 0, fmt.Errorf("run with ID %s not found", runID)
	}

	// Only allow START as the first event
	if storedRun.NextEventID == 0 && eventMetadata.EventType() != events.EventTypeStart {
		return 0, fmt.Errorf("first event for run %s must be START", runID)
	}

	// Check optimistic concurrency precondition
	if storedRun.NextEventID != nextEventID {
		return 0, ErrConcurrentUpdate
	}

	// State transition checks
	switch eventMetadata.EventType() {
	case events.EventTypeClaim:
		if !(storedRun.Status == RunStatusPending ||
			(storedRun.Status == RunStatusRunning && storedRun.LeasedUntil.Before(time.Now())) ||
			(storedRun.Status == RunStatusRunning && storedRun.LeasedBy == eventMetadata.(events.ClaimEvent).WorkerID())) {
			return 0, errors.New("invalid state transition: claim event not allowed from current state")
		}
	case events.EventTypeYield:
		if storedRun.Status != RunStatusRunning {
			return 0, errors.New("invalid state transition: yield event only allowed from running state")
		}
	case events.EventTypeReturn, events.EventTypeCall:
		if storedRun.Status != RunStatusRunning {
			return 0, errors.New("invalid state transition: return event only allowed from running state")
		}
	case events.EventTypeFinish:
		if storedRun.Status != RunStatusRunning {
			return 0, errors.New("invalid state transition: finish event only allowed from running state")
		}
	case events.EventTypeResume:
		resumeEvent, ok := eventMetadata.(events.ResumeEvent)
		if !ok {
			return 0, errors.New("invalid resume event metadata")
		}
		signalID := resumeEvent.SignalID()
		if storedRun.Status != RunStatusYielded {
			return 0, errors.New("invalid state transition: resume event only allowed from yielded state")
		}
		runIDForSignal, signalExists := s.yields[signalID]
		if !signalExists || runIDForSignal != runID {
			return 0, errors.New("invalid resume event: signal ID does not exist")
		}
	}

	// Add event to the list
	event := &events.Event{
		Timestamp: time.Now(),
		Metadata:  eventMetadata,
	}

	s.events[runID] = append(s.events[runID], event)

	// Update the runs
	switch eventMetadata.EventType() {
	case events.EventTypeYield:
		if yieldEvent, ok := event.Metadata.(events.YieldEvent); ok {
			storedRun.Status = RunStatusYielded
			s.yields[yieldEvent.SignalID()] = runID
		}
	case events.EventTypeFinish:
		if finishEvent, ok := event.Metadata.(events.FinishEvent); ok {
			output, err := finishEvent.Output()
			if err != nil {
				storedRun.Status = RunStatusFailed
				storedRun.Error = err
			} else {
				storedRun.Status = RunStatusCompleted
				storedRun.Output = output
			}
		}
	case events.EventTypeClaim:
		if claimEvent, ok := event.Metadata.(events.ClaimEvent); ok {
			storedRun.Status = RunStatusRunning
			storedRun.LeasedBy = claimEvent.WorkerID()
			storedRun.LeasedUntil = claimEvent.Until()
		}
	case events.EventTypeResume:
		if resumeEvent, ok := event.Metadata.(events.ResumeEvent); ok {
			storedRun.Status = RunStatusPending
			delete(s.yields, resumeEvent.SignalID())
		}
	}

	storedRun.NextEventID++
	storedRun.UpdatedAt = time.Now()
	return storedRun.NextEventID, nil
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
