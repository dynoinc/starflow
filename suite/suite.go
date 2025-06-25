package suite

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/dynoinc/starflow"
	"github.com/dynoinc/starflow/events"
	testpb "github.com/dynoinc/starflow/suite/proto"
)

// StoreFactory is a function that creates a new store instance for testing
type StoreFactory func(t *testing.T) starflow.Store

// RunStoreSuite runs the complete test suite against a store implementation
func RunStoreSuite(t *testing.T, newStore StoreFactory) {
	t.Helper()
	s := newStore(t)
	ctx := t.Context()

	sh, err := s.SaveScript(ctx, []byte("print('hello')"))
	require.NoError(t, err)

	t.Run("CreateRunWithNonExistentScriptHash", func(t *testing.T) {
		_, err := s.CreateRun(ctx, "non-existent-hash", nil)
		require.Error(t, err)
	})

	t.Run("ScriptIdempotent", func(t *testing.T) {
		content := []byte("print('hi')")
		h1, err := s.SaveScript(ctx, content)
		require.NoError(t, err)
		h2, err := s.SaveScript(ctx, content)
		require.NoError(t, err)
		require.Equal(t, h1, h2)
		got, err := s.GetScript(t.Context(), h1)
		require.NoError(t, err)
		require.Equal(t, string(content), string(got))
	})

	t.Run("CreateGetRun", func(t *testing.T) {
		id, err := s.CreateRun(ctx, sh, nil)
		require.NoError(t, err)
		run, err := s.GetRun(ctx, id)
		require.NoError(t, err)
		require.Equal(t, id, run.ID)
	})

	t.Run("NextEventID", func(t *testing.T) {
		id, err := s.CreateRun(ctx, sh, nil)
		require.NoError(t, err)

		run, err := s.GetRun(ctx, id)
		require.NoError(t, err)
		require.Equal(t, int64(0), run.NextEventID)

		nextEventID, err := s.RecordEvent(ctx, id, run.NextEventID, events.NewCallEvent("fn", nil))
		require.NoError(t, err)
		require.Equal(t, int64(1), nextEventID)
	})

	t.Run("OptimisticRecordEvent", func(t *testing.T) {
		id, err := s.CreateRun(ctx, sh, nil)
		require.NoError(t, err)

		run, err := s.GetRun(ctx, id)
		require.NoError(t, err)

		_, err = s.RecordEvent(ctx, id, run.NextEventID, events.NewCallEvent("fn", nil))
		require.NoError(t, err)

		_, err = s.RecordEvent(ctx, id, run.NextEventID, events.NewCallEvent("fn", nil))
		require.Error(t, err)
		require.Equal(t, err, starflow.ErrConcurrentUpdate)
	})

	t.Run("RecordEventWithInvalidRunID", func(t *testing.T) {
		_, err := s.RecordEvent(ctx, "non-existent-run-id", 0, events.NewCallEvent("fn", nil))
		require.Error(t, err, "recording event with invalid runID should fail")
	})

	t.Run("ReturnEventWithErrorUpdatesRunToFailed", func(t *testing.T) {
		id, err := s.CreateRun(ctx, sh, nil)
		require.NoError(t, err)

		run, err := s.GetRun(ctx, id)
		require.NoError(t, err)

		testError := errors.New("test error")
		_, err = s.RecordEvent(ctx, id, run.NextEventID, events.NewReturnEvent(nil, testError))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, id)
		require.NoError(t, err)
		require.Equal(t, starflow.RunStatusFailed, run.Status)
		require.Error(t, run.Error)
		require.Equal(t, testError.Error(), run.Error.Error())
	})

	t.Run("YieldEventUpdatesRunToYielded", func(t *testing.T) {
		id, err := s.CreateRun(ctx, sh, nil)
		require.NoError(t, err)

		run, err := s.GetRun(ctx, id)
		require.NoError(t, err)

		_, err = s.RecordEvent(ctx, id, run.NextEventID, events.NewYieldEvent("test-signal-id", id))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, id)
		require.NoError(t, err)
		require.Equal(t, starflow.RunStatusYielded, run.Status)
	})

	t.Run("ClaimEventUpdatesRunToRunning", func(t *testing.T) {
		id, err := s.CreateRun(ctx, sh, nil)
		require.NoError(t, err)

		run, err := s.GetRun(ctx, id)
		require.NoError(t, err)

		_, err = s.RecordEvent(ctx, id, run.NextEventID, events.NewClaimEvent("test-worker"))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, id)
		require.NoError(t, err)
		require.Equal(t, starflow.RunStatusRunning, run.Status)
	})

	t.Run("ClaimEventWithConcurrentWorkers", func(t *testing.T) {
		id, err := s.CreateRun(ctx, sh, nil)
		require.NoError(t, err)

		run, err := s.GetRun(ctx, id)
		require.NoError(t, err)

		// First claim should succeed
		_, err = s.RecordEvent(ctx, id, run.NextEventID, events.NewClaimEvent("worker1"))
		require.NoError(t, err)

		// Second claim should fail due to optimistic concurrency
		_, err = s.RecordEvent(ctx, id, run.NextEventID, events.NewClaimEvent("worker2"))
		require.Error(t, err)
		require.Equal(t, starflow.ErrConcurrentUpdate, err)

		run, err = s.GetRun(ctx, id)
		require.NoError(t, err)
		require.Equal(t, starflow.RunStatusRunning, run.Status)
	})

	t.Run("FinishEventUpdatesRunToCompleted", func(t *testing.T) {
		id, err := s.CreateRun(ctx, sh, nil)
		require.NoError(t, err)

		run, err := s.GetRun(ctx, id)
		require.NoError(t, err)

		output, _ := anypb.New(&testpb.PingResponse{Message: "test output"})
		_, err = s.RecordEvent(ctx, id, run.NextEventID, events.NewFinishEvent(output))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, id)
		require.NoError(t, err)
		require.Equal(t, starflow.RunStatusCompleted, run.Status)
		require.NotNil(t, run.Output)
	})

	t.Run("GetEvents", func(t *testing.T) {
		id, err := s.CreateRun(ctx, sh, nil)
		require.NoError(t, err)

		run, err := s.GetRun(ctx, id)
		require.NoError(t, err)

		// Record multiple events
		_, err = s.RecordEvent(ctx, id, run.NextEventID, events.NewCallEvent("fn1", nil))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, id)
		require.NoError(t, err)

		_, err = s.RecordEvent(ctx, id, run.NextEventID, events.NewCallEvent("fn2", nil))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, id)
		require.NoError(t, err)

		_, err = s.RecordEvent(ctx, id, run.NextEventID, events.NewReturnEvent(nil, nil))
		require.NoError(t, err)

		// Get all events
		runEvents, err := s.GetEvents(ctx, id)
		require.NoError(t, err)
		require.Len(t, runEvents, 3)
		require.Equal(t, events.EventTypeCall, runEvents[0].Type())
		require.Equal(t, events.EventTypeCall, runEvents[1].Type())
		require.Equal(t, events.EventTypeReturn, runEvents[2].Type())
	})

	t.Run("SignalInvariants", func(t *testing.T) {
		// Test 1: Signaling with non-existent run ID succeeds silently
		output, _ := anypb.New(&testpb.PingResponse{Message: "test output"})
		err := s.Signal(ctx, "non-existent-run-id", "non-existent-signal-id", output)
		require.NoError(t, err, "signaling with non-existent run ID should succeed silently")

		// Test 2: Signaling with non-existent signal ID succeeds silently
		id, err := s.CreateRun(ctx, sh, nil)
		require.NoError(t, err)

		err = s.Signal(ctx, id, "non-existent-signal-id", output)
		require.NoError(t, err, "signaling with non-existent signal ID should succeed silently")

		// Test 3: Signaling a valid run updates status to RunStatusPending
		run, err := s.GetRun(ctx, id)
		require.NoError(t, err)
		require.Equal(t, starflow.RunStatusPending, run.Status, "run status should be updated to RunStatusPending after signal")

		// Test 4: Signaling a yielded run with valid signal ID
		run, err = s.GetRun(ctx, id)
		require.NoError(t, err)

		// Create a yield event to set up a valid signal
		_, err = s.RecordEvent(ctx, id, run.NextEventID, events.NewYieldEvent("test-signal-id", id))
		require.NoError(t, err)

		run, err = s.GetRun(ctx, id)
		require.NoError(t, err)
		require.Equal(t, starflow.RunStatusYielded, run.Status)

		// Signal the yielded run
		err = s.Signal(ctx, id, "test-signal-id", output)
		require.NoError(t, err)

		run, err = s.GetRun(ctx, id)
		require.NoError(t, err)
		require.Equal(t, starflow.RunStatusPending, run.Status, "yielded run should be updated to RunStatusPending after signal")
	})
}
