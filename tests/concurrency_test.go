package starflow_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dynoinc/starflow"
)

// TestRecordEventConcurrent ensures optimistic locking prevents two workers from writing the same event index.
func TestRecordEventConcurrent(t *testing.T) {
	store := NewSQLiteStore(t)

	// Create trivial run
	runID, err := store.CreateRun("scriptHash", nil)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	evt := &starflow.Event{Timestamp: time.Now(), Type: starflow.EventTypeCall, FunctionName: "fn"}

	var wg sync.WaitGroup
	wg.Add(2)
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		go func(idx int) {
			defer wg.Done()
			errs[idx] = store.RecordEvent(runID, evt)
		}(i)
	}
	wg.Wait()

	// Exactly one goroutine should succeed, the other must get ErrConcurrentUpdate.
	var success, concurrent int
	for _, e := range errs {
		if e == nil {
			success++
		} else if e == starflow.ErrConcurrentUpdate {
			concurrent++
		} else {
			t.Fatalf("unexpected error: %v", e)
		}
	}
	if success != 1 || concurrent != 1 {
		t.Fatalf("expected 1 success and 1 ErrConcurrentUpdate, got success=%d concurrent=%d", success, concurrent)
	}

	// Verify next_event_id incremented to 1.
	run, err := store.GetRun(runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.NextEventID != 1 {
		t.Fatalf("expected next_event_id=1, got %d", run.NextEventID)
	}
}

// TestClaimRunLease verifies that a lease prevents other workers until it expires.
func TestClaimRunLease(t *testing.T) {
	store := NewSQLiteStore(t)

	runID, err := store.CreateRun("scriptHash", nil)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	ctx := context.Background()
	lease1 := time.Now().Add(20 * time.Millisecond)
	ok, err := store.ClaimRun(ctx, runID, "worker1", lease1)
	if err != nil || !ok {
		t.Fatalf("first claim failed: %v", err)
	}

	// Second worker should fail while lease active
	ok, err = store.ClaimRun(ctx, runID, "worker2", time.Now().Add(20*time.Millisecond))
	if err != nil {
		t.Fatalf("second claim error: %v", err)
	}
	if ok {
		t.Fatalf("expected second claim to fail while lease active")
	}

	// Wait until lease expires
	time.Sleep(25 * time.Millisecond)

	ok, err = store.ClaimRun(ctx, runID, "worker2", time.Now().Add(20*time.Millisecond))
	if err != nil || !ok {
		t.Fatalf("second claim after expiry should succeed, ok=%v err=%v", ok, err)
	}
}
