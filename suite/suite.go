package suite

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/lithammer/shortuuid/v4"

	"github.com/dynoinc/starflow"
	"github.com/dynoinc/starflow/events"
	testpb "github.com/dynoinc/starflow/suite/proto"
)

// StoreFactory is a function that creates a new store instance for testing
type StoreFactory func(t *testing.T) starflow.Store

// computeScriptHash computes the SHA256 hash of script content
func computeScriptHash(content []byte) string {
	hash := sha256.Sum256(content)
	return hex.EncodeToString(hash[:])
}

// RunStoreSuite runs the complete test suite against a store implementation
func RunStoreSuite(t *testing.T, newStore StoreFactory) {
	t.Helper()
	ctx := t.Context()

	t.Run("CreateRunWithNonExistentScriptHash", func(t *testing.T) {
		s := newStore(t)
		runID := shortuuid.New()
		err := s.CreateRun(ctx, runID, "non-existent-hash", nil)
		require.Error(t, err)
	})

	t.Run("CreateRunWithDuplicateRunID", func(t *testing.T) {
		s := newStore(t)
		content := []byte("print('hello')")
		scriptHash := computeScriptHash(content)
		err := s.SaveScript(ctx, scriptHash, content)
		require.NoError(t, err)

		runID := shortuuid.New()
		err = s.CreateRun(ctx, runID, scriptHash, nil)
		require.NoError(t, err)
		err = s.CreateRun(ctx, runID, scriptHash, nil)
		require.Error(t, err)
	})

	t.Run("ScriptIdempotent", func(t *testing.T) {
		s := newStore(t)
		content := []byte("print('hi')")
		scriptHash := computeScriptHash(content)

		err := s.SaveScript(ctx, scriptHash, content)
		require.NoError(t, err)
		err = s.SaveScript(ctx, scriptHash, content)
		require.NoError(t, err)

		got, err := s.GetScript(t.Context(), scriptHash)
		require.NoError(t, err)
		require.Equal(t, string(content), string(got))
	})

	t.Run("SaveScriptWithMismatchedHash", func(t *testing.T) {
		s := newStore(t)
		content := []byte("print('hi')")
		wrongHash := "wrong-hash"

		err := s.SaveScript(ctx, wrongHash, content)
		require.Error(t, err)
		require.Contains(t, err.Error(), "content hash")
		require.Contains(t, err.Error(), "does not match provided scriptHash")
	})

	t.Run("CreateGetRun", func(t *testing.T) {
		s := newStore(t)
		content := []byte("print('hello')")
		scriptHash := computeScriptHash(content)

		err := s.SaveScript(ctx, scriptHash, content)
		require.NoError(t, err)

		runID := shortuuid.New()
		err = s.CreateRun(ctx, runID, scriptHash, nil)
		require.NoError(t, err)
		run, err := s.GetRun(ctx, runID)
		require.NoError(t, err)
		require.Equal(t, runID, run.ID)
	})

	t.Run("NextEventID", func(t *testing.T) {
		s := newStore(t)
		content := []byte("print('hello')")
		scriptHash := computeScriptHash(content)

		err := s.SaveScript(ctx, scriptHash, content)
		require.NoError(t, err)

		runID := shortuuid.New()
		err = s.CreateRun(ctx, runID, scriptHash, nil)
		require.NoError(t, err)

		run, err := s.GetRun(ctx, runID)
		require.NoError(t, err)
		require.Equal(t, int64(0), run.NextEventID)

		nextEventID, err := s.RecordEvent(ctx, runID, run.NextEventID, events.NewClaimEvent("worker", time.Now().Add(10*time.Second)))
		require.NoError(t, err)
		require.Equal(t, int64(1), nextEventID)

		nextEventID, err = s.RecordEvent(ctx, runID, nextEventID, events.NewCallEvent("fn", nil))
		require.NoError(t, err)
		require.Equal(t, int64(2), nextEventID)

		run, err = s.GetRun(ctx, runID)
		require.NoError(t, err)
		require.Equal(t, int64(2), run.NextEventID)
	})

	t.Run("OptimisticRecordEvent", func(t *testing.T) {
		s := newStore(t)
		content := []byte("print('hello')")
		scriptHash := computeScriptHash(content)

		err := s.SaveScript(ctx, scriptHash, content)
		require.NoError(t, err)

		runID := shortuuid.New()
		err = s.CreateRun(ctx, runID, scriptHash, nil)
		require.NoError(t, err)

		run, err := s.GetRun(ctx, runID)
		require.NoError(t, err)

		_, err = s.RecordEvent(ctx, runID, run.NextEventID, events.NewClaimEvent("worker", time.Now().Add(10*time.Second)))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, runID)
		require.NoError(t, err)

		// First event should succeed
		_, err = s.RecordEvent(ctx, runID, run.NextEventID, events.NewCallEvent("fn", nil))
		require.NoError(t, err)

		// Second event with same NextEventID should fail with ErrConcurrentUpdate
		_, err = s.RecordEvent(ctx, runID, run.NextEventID, events.NewCallEvent("fn", nil))
		require.ErrorIs(t, err, starflow.ErrConcurrentUpdate)
	})

	t.Run("RecordEventWithInvalidRunID", func(t *testing.T) {
		s := newStore(t)
		_, err := s.RecordEvent(ctx, "non-existent-run-id", 0, events.NewCallEvent("fn", nil))
		require.Error(t, err, "recording event with invalid runID should fail")
	})

	t.Run("ReturnEventWithErrorUpdatesRunToFailed", func(t *testing.T) {
		s := newStore(t)
		content := []byte("print('hello')")
		scriptHash := computeScriptHash(content)

		err := s.SaveScript(ctx, scriptHash, content)
		require.NoError(t, err)

		runID := shortuuid.New()
		err = s.CreateRun(ctx, runID, scriptHash, nil)
		require.NoError(t, err)

		run, err := s.GetRun(ctx, runID)
		require.NoError(t, err)

		_, err = s.RecordEvent(ctx, runID, run.NextEventID, events.NewClaimEvent("worker", time.Now().Add(10*time.Second)))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, runID)
		require.NoError(t, err)
		require.Equal(t, int64(1), run.NextEventID)

		testError := errors.New("test error")
		// After Return event with error, run should still be Running
		_, err = s.RecordEvent(ctx, runID, run.NextEventID, events.NewReturnEvent(nil, testError))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, runID)
		require.NoError(t, err)
		require.Equal(t, starflow.RunStatusRunning, run.Status)
		require.Nil(t, run.Error)

		// After Finish event with error, run should be Failed
		_, err = s.RecordEvent(ctx, runID, run.NextEventID, events.NewFinishEvent(nil, testError))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, runID)
		require.NoError(t, err)
		require.Equal(t, starflow.RunStatusFailed, run.Status)
		require.Error(t, run.Error)
		require.Equal(t, testError.Error(), run.Error.Error())
	})

	t.Run("YieldEventUpdatesRunToYielded", func(t *testing.T) {
		s := newStore(t)
		content := []byte("print('hello')")
		scriptHash := computeScriptHash(content)

		err := s.SaveScript(ctx, scriptHash, content)
		require.NoError(t, err)

		runID := shortuuid.New()
		err = s.CreateRun(ctx, runID, scriptHash, nil)
		require.NoError(t, err)

		run, err := s.GetRun(ctx, runID)
		require.NoError(t, err)

		_, err = s.RecordEvent(ctx, runID, run.NextEventID, events.NewClaimEvent("worker", time.Now().Add(10*time.Second)))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, runID)
		require.NoError(t, err)
		require.Equal(t, int64(1), run.NextEventID)

		_, err = s.RecordEvent(ctx, runID, run.NextEventID, events.NewYieldEvent("test-signal-id", runID))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, runID)
		require.NoError(t, err)
		require.Equal(t, starflow.RunStatusYielded, run.Status)
	})

	t.Run("ClaimEventUpdatesRunToRunning", func(t *testing.T) {
		s := newStore(t)
		content := []byte("print('hello')")
		scriptHash := computeScriptHash(content)

		err := s.SaveScript(ctx, scriptHash, content)
		require.NoError(t, err)

		runID := shortuuid.New()
		err = s.CreateRun(ctx, runID, scriptHash, nil)
		require.NoError(t, err)

		run, err := s.GetRun(ctx, runID)
		require.NoError(t, err)

		_, err = s.RecordEvent(ctx, runID, run.NextEventID, events.NewClaimEvent("test-worker", time.Now().Add(10*time.Second)))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, runID)
		require.NoError(t, err)
		require.Equal(t, starflow.RunStatusRunning, run.Status)
	})

	t.Run("ClaimEventWithConcurrentWorkers", func(t *testing.T) {
		s := newStore(t)
		content := []byte("print('hello')")
		scriptHash := computeScriptHash(content)

		err := s.SaveScript(ctx, scriptHash, content)
		require.NoError(t, err)

		runID := shortuuid.New()
		err = s.CreateRun(ctx, runID, scriptHash, nil)
		require.NoError(t, err)

		run, err := s.GetRun(ctx, runID)
		require.NoError(t, err)

		// First claim should succeed
		_, err = s.RecordEvent(ctx, runID, run.NextEventID, events.NewClaimEvent("worker1", time.Now().Add(10*time.Second)))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, runID)
		require.NoError(t, err)

		// Second claim should fail due to invalid state transition
		_, err = s.RecordEvent(ctx, runID, run.NextEventID, events.NewClaimEvent("worker2", time.Now().Add(10*time.Second)))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid state transition: claim event not allowed from current state")

		run, err = s.GetRun(ctx, runID)
		require.NoError(t, err)
		require.Equal(t, starflow.RunStatusRunning, run.Status)
	})

	t.Run("ClaimRunsConditions", func(t *testing.T) {
		s := newStore(t)
		content := []byte("print('hello')")
		scriptHash := computeScriptHash(content)

		err := s.SaveScript(ctx, scriptHash, content)
		require.NoError(t, err)

		// Test 1: Pending run should be claimed and returned
		runID1 := shortuuid.New()
		err = s.CreateRun(ctx, runID1, scriptHash, nil)
		require.NoError(t, err)
		claimedRuns, err := s.ClaimRuns(ctx, "worker1", time.Now().Add(10*time.Second))
		require.NoError(t, err)
		require.Len(t, claimedRuns, 1)
		require.Equal(t, runID1, claimedRuns[0].ID)
		require.Equal(t, starflow.RunStatusRunning, claimedRuns[0].Status)
		require.Equal(t, "worker1", claimedRuns[0].LeasedBy)

		// Test 2: Run with expired lease should be claimed and returned by a new worker
		runID2 := shortuuid.New()
		err = s.CreateRun(ctx, runID2, scriptHash, nil)
		require.NoError(t, err)
		// Manually record a claim event with an expired lease
		run2, err := s.GetRun(ctx, runID2)
		require.NoError(t, err)
		_, err = s.RecordEvent(ctx, runID2, run2.NextEventID, events.NewClaimEvent("worker_old", time.Now().Add(-1*time.Second)))
		require.NoError(t, err)

		claimedRuns, err = s.ClaimRuns(ctx, "worker2", time.Now().Add(10*time.Second))
		require.NoError(t, err)
		require.Len(t, claimedRuns, 1)
		require.Equal(t, runID2, claimedRuns[0].ID)
		require.Equal(t, starflow.RunStatusRunning, claimedRuns[0].Status)
		require.Equal(t, "worker2", claimedRuns[0].LeasedBy)

		// Test 3: Run leased by different worker (not expired) should NOT be claimed
		runID3 := shortuuid.New()
		err = s.CreateRun(ctx, runID3, scriptHash, nil)
		require.NoError(t, err)
		// Manually record a claim event with a valid lease
		run3, err := s.GetRun(ctx, runID3)
		require.NoError(t, err)
		_, err = s.RecordEvent(ctx, runID3, run3.NextEventID, events.NewClaimEvent("worker_valid", time.Now().Add(10*time.Second)))
		require.NoError(t, err)

		claimedRuns, err = s.ClaimRuns(ctx, "worker4", time.Now().Add(10*time.Second))
		require.NoError(t, err)
		require.Len(t, claimedRuns, 0) // No runs should be claimed

		// Verify the status of runID3 remains unchanged
		run3AfterClaim, err := s.GetRun(ctx, runID3)
		require.NoError(t, err)
		require.Equal(t, starflow.RunStatusRunning, run3AfterClaim.Status)
		require.Equal(t, "worker_valid", run3AfterClaim.LeasedBy)
	})

	t.Run("FinishEventUpdatesRunToCompleted", func(t *testing.T) {
		s := newStore(t)
		content := []byte("print('hello')")
		scriptHash := computeScriptHash(content)

		err := s.SaveScript(ctx, scriptHash, content)
		require.NoError(t, err)

		runID := shortuuid.New()
		err = s.CreateRun(ctx, runID, scriptHash, nil)
		require.NoError(t, err)

		run, err := s.GetRun(ctx, runID)
		require.NoError(t, err)

		// Move to Running
		_, err = s.RecordEvent(ctx, runID, run.NextEventID, events.NewClaimEvent("worker", time.Now().Add(10*time.Second)))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, runID)
		require.NoError(t, err)

		output, _ := anypb.New(&testpb.PingResponse{Message: "test output"})
		_, err = s.RecordEvent(ctx, runID, run.NextEventID, events.NewFinishEvent(output, nil))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, runID)
		require.NoError(t, err)
		require.Equal(t, starflow.RunStatusCompleted, run.Status)
		require.NotNil(t, run.Output)
	})

	t.Run("FinishEventWithErrorUpdatesRunToFailed", func(t *testing.T) {
		s := newStore(t)
		content := []byte("print('hello')")
		scriptHash := computeScriptHash(content)

		err := s.SaveScript(ctx, scriptHash, content)
		require.NoError(t, err)

		runID := shortuuid.New()
		err = s.CreateRun(ctx, runID, scriptHash, nil)
		require.NoError(t, err)

		run, err := s.GetRun(ctx, runID)
		require.NoError(t, err)

		// Move to Running
		_, err = s.RecordEvent(ctx, runID, run.NextEventID, events.NewClaimEvent("worker", time.Now().Add(10*time.Second)))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, runID)
		require.NoError(t, err)

		testError := errors.New("test finish error")
		_, err = s.RecordEvent(ctx, runID, run.NextEventID, events.NewFinishEvent(nil, testError))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, runID)
		require.NoError(t, err)
		require.Equal(t, starflow.RunStatusFailed, run.Status)
		require.Error(t, run.Error)
		require.Equal(t, testError.Error(), run.Error.Error())
	})

	t.Run("GetEvents", func(t *testing.T) {
		s := newStore(t)
		content := []byte("print('hello')")
		scriptHash := computeScriptHash(content)

		err := s.SaveScript(ctx, scriptHash, content)
		require.NoError(t, err)

		runID := shortuuid.New()
		err = s.CreateRun(ctx, runID, scriptHash, nil)
		require.NoError(t, err)

		run, err := s.GetRun(ctx, runID)
		require.NoError(t, err)

		// Move to Running
		_, err = s.RecordEvent(ctx, runID, run.NextEventID, events.NewClaimEvent("worker", time.Now().Add(10*time.Second)))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, runID)
		require.NoError(t, err)

		// Record multiple events
		_, err = s.RecordEvent(ctx, runID, run.NextEventID, events.NewCallEvent("fn1", nil))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, runID)
		require.NoError(t, err)

		_, err = s.RecordEvent(ctx, runID, run.NextEventID, events.NewCallEvent("fn2", nil))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, runID)
		require.NoError(t, err)

		_, err = s.RecordEvent(ctx, runID, run.NextEventID, events.NewReturnEvent(nil, nil))
		require.NoError(t, err)

		// Get all events
		runEvents, err := s.GetEvents(ctx, runID)
		require.NoError(t, err)
		require.Len(t, runEvents, 4)
		require.Equal(t, events.EventTypeClaim, runEvents[0].Type())
		require.Equal(t, events.EventTypeCall, runEvents[1].Type())
		require.Equal(t, events.EventTypeCall, runEvents[2].Type())
		require.Equal(t, events.EventTypeReturn, runEvents[3].Type())
	})

	t.Run("SignalInvariants", func(t *testing.T) {
		s := newStore(t)
		content := []byte("print('hello')")
		scriptHash := computeScriptHash(content)

		err := s.SaveScript(ctx, scriptHash, content)
		require.NoError(t, err)

		// Test 1: Signaling with non-existent run ID succeeds silently
		output, _ := anypb.New(&testpb.PingResponse{Message: "test output"})
		_, err = s.RecordEvent(ctx, "non-existent-run-id", 0, events.NewResumeEvent("non-existent-signal-id", output))
		require.Error(t, err, "signaling with non-existent run ID should return error")

		// Test 2: Signaling with non-existent signal ID succeeds silently
		runID := shortuuid.New()
		err = s.CreateRun(ctx, runID, scriptHash, nil)
		require.NoError(t, err)

		// Move to Running
		run, err := s.GetRun(ctx, runID)
		require.NoError(t, err)
		_, err = s.RecordEvent(ctx, runID, run.NextEventID, events.NewClaimEvent("worker", time.Now().Add(10*time.Second)))
		require.NoError(t, err)

		// Should be in running state, so yield event is valid
		run, err = s.GetRun(ctx, runID)
		require.NoError(t, err)
		require.Equal(t, starflow.RunStatusRunning, run.Status, "run status should be running before yield")

		// Test 4: Signaling a yielded run with valid signal ID
		run, err = s.GetRun(ctx, runID)
		require.NoError(t, err)

		// Create a yield event to set up a valid signal
		_, err = s.RecordEvent(ctx, runID, run.NextEventID, events.NewYieldEvent("test-signal-id", runID))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, runID)
		require.NoError(t, err)
		require.Equal(t, starflow.RunStatusYielded, run.Status)

		// Signal the yielded run (resume event)
		_, err = s.RecordEvent(ctx, runID, run.NextEventID, events.NewResumeEvent("test-signal-id", output))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, runID)
		require.NoError(t, err)
		require.Equal(t, starflow.RunStatusPending, run.Status, "yielded run should be updated to RunStatusPending after signal/resume event")
	})
}
