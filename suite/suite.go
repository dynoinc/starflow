package suite

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lithammer/shortuuid/v4"

	"github.com/dynoinc/starflow"
)

// Test data structures for JSON-based suite tests
type TestResponse struct {
	Message string `json:"message"`
}

// StoreFactory is a function that creates a starflow.New store instance for testing
type StoreFactory func(t *testing.T) starflow.Store

// RunStoreSuite runs the complete test suite against a store implementation
func RunStoreSuite(t *testing.T, newStore StoreFactory) {
	t.Helper()
	ctx := t.Context()

	// Start event is only allowed for starflow.New runs
	t.Run("StartEventOnlyForNewRuns", func(t *testing.T) {
		s := newStore(t)
		runID := shortuuid.New()

		// First start event should succeed
		_, err := s.RecordEvent(ctx, runID, 0, starflow.NewStartEvent("script-hash", nil))
		require.NoError(t, err)

		// Second start event on same runID should fail
		_, err = s.RecordEvent(ctx, runID, 0, starflow.NewStartEvent("script-hash", nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists")
	})

	// All other events are only allowed for existing runs
	t.Run("OtherEventsOnlyForExistingRuns", func(t *testing.T) {
		s := newStore(t)
		nonExistentRunID := shortuuid.New()

		// Try to record a non-start event for non-existent run
		_, err := s.RecordEvent(ctx, nonExistentRunID, 0, starflow.NewCallEvent("fn", nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	// Nothing is allowed after a finish event
	t.Run("NothingAllowedAfterFinish", func(t *testing.T) {
		s := newStore(t)
		runID := shortuuid.New()

		// Create a run and finish it
		_, err := s.RecordEvent(ctx, runID, 0, starflow.NewStartEvent("script-hash", nil))
		require.NoError(t, err)
		_, err = s.RecordEvent(ctx, runID, 1, starflow.NewFinishEvent(nil, nil))
		require.NoError(t, err)

		// Try to record another event after finish
		_, err = s.RecordEvent(ctx, runID, 2, starflow.NewCallEvent("fn", nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "has already finished")
	})

	// Event ID must be sequential except for resume events
	t.Run("SequentialEventIDExceptResume", func(t *testing.T) {
		s := newStore(t)
		runID := shortuuid.New()

		// Create a run
		_, err := s.RecordEvent(ctx, runID, 0, starflow.NewStartEvent("script-hash", nil))
		require.NoError(t, err)

		// Try to record with wrong event ID
		_, err = s.RecordEvent(ctx, runID, 5, starflow.NewCallEvent("fn", nil))
		require.ErrorIs(t, err, starflow.ErrConcurrentUpdate)

		// Correct sequential ID should work
		_, err = s.RecordEvent(ctx, runID, 1, starflow.NewCallEvent("fn", nil))
		require.NoError(t, err)
	})

	// Only yield events can be resumed
	t.Run("OnlyYieldEventsCanBeResumed", func(t *testing.T) {
		s := newStore(t)
		runID := shortuuid.New()
		output := TestResponse{Message: "test"}

		// Create a run with a call event
		_, err := s.RecordEvent(ctx, runID, 0, starflow.NewStartEvent("script-hash", nil))
		require.NoError(t, err)
		_, err = s.RecordEvent(ctx, runID, 1, starflow.NewCallEvent("fn", nil))
		require.NoError(t, err)

		// Try to resume after a call event (should fail)
		_, err = s.RecordEvent(ctx, runID, 2, starflow.NewResumeEvent("signal", output))
		require.Error(t, err)
		require.Contains(t, err.Error(), "not in yielded state")
	})

	// The signal ID must match the yield event
	t.Run("SignalIDMustMatchYieldEvent", func(t *testing.T) {
		s := newStore(t)
		runID := shortuuid.New()
		output := TestResponse{Message: "test"}

		// Create a run with call then yield
		_, err := s.RecordEvent(ctx, runID, 0, starflow.NewStartEvent("script-hash", nil))
		require.NoError(t, err)
		_, err = s.RecordEvent(ctx, runID, 1, starflow.NewCallEvent("fn", nil))
		require.NoError(t, err)
		_, err = s.RecordEvent(ctx, runID, 2, starflow.NewYieldEvent("signal1", runID))
		require.NoError(t, err)

		// Try to resume with wrong signal ID
		_, err = s.RecordEvent(ctx, runID, 3, starflow.NewResumeEvent("signal2", output))
		require.Error(t, err)
		require.Contains(t, err.Error(), "signal ID mismatch")

		// Correct signal ID should work
		_, err = s.RecordEvent(ctx, runID, 3, starflow.NewResumeEvent("signal1", output))
		require.NoError(t, err)
	})

	// Only call events can be yielded
	t.Run("OnlyCallEventsCanBeYielded", func(t *testing.T) {
		s := newStore(t)
		runID := shortuuid.New()

		// Create a run with just start event
		_, err := s.RecordEvent(ctx, runID, 0, starflow.NewStartEvent("script-hash", nil))
		require.NoError(t, err)

		// Try to yield after start event (should fail)
		_, err = s.RecordEvent(ctx, runID, 1, starflow.NewYieldEvent("signal", runID))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid event type")
	})

	// Only call events can be returned
	t.Run("OnlyCallEventsCanBeReturned", func(t *testing.T) {
		s := newStore(t)
		runID := shortuuid.New()

		// Create a run with just start event
		_, err := s.RecordEvent(ctx, runID, 0, starflow.NewStartEvent("script-hash", nil))
		require.NoError(t, err)

		// Try to return after start event (should fail)
		_, err = s.RecordEvent(ctx, runID, 1, starflow.NewReturnEvent(nil, nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid event type")
	})

	// Only yield or return events are allowed after a call event
	t.Run("OnlyYieldOrReturnAfterCall", func(t *testing.T) {
		s := newStore(t)
		runID := shortuuid.New()

		// Create a run with call event
		_, err := s.RecordEvent(ctx, runID, 0, starflow.NewStartEvent("script-hash", nil))
		require.NoError(t, err)
		_, err = s.RecordEvent(ctx, runID, 1, starflow.NewCallEvent("fn", nil))
		require.NoError(t, err)

		// Try to record another call event after call (should fail)
		_, err = s.RecordEvent(ctx, runID, 2, starflow.NewCallEvent("fn2", nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid event type")

		// Try to record finish event after call (should fail)
		_, err = s.RecordEvent(ctx, runID, 2, starflow.NewFinishEvent(nil, nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid event type")

		// Yield should work
		runID2 := shortuuid.New()
		_, err = s.RecordEvent(ctx, runID2, 0, starflow.NewStartEvent("script-hash", nil))
		require.NoError(t, err)
		_, err = s.RecordEvent(ctx, runID2, 1, starflow.NewCallEvent("fn", nil))
		require.NoError(t, err)
		_, err = s.RecordEvent(ctx, runID2, 2, starflow.NewYieldEvent("signal", runID2))
		require.NoError(t, err)

		// Return should work
		runID3 := shortuuid.New()
		_, err = s.RecordEvent(ctx, runID3, 0, starflow.NewStartEvent("script-hash", nil))
		require.NoError(t, err)
		_, err = s.RecordEvent(ctx, runID3, 1, starflow.NewCallEvent("fn", nil))
		require.NoError(t, err)
		_, err = s.RecordEvent(ctx, runID3, 2, starflow.NewReturnEvent(nil, nil))
		require.NoError(t, err)
	})

	// Only resume events are allowed after a yield event
	t.Run("OnlyResumeAfterYield", func(t *testing.T) {
		s := newStore(t)
		runID := shortuuid.New()

		// Create a run with call then yield
		_, err := s.RecordEvent(ctx, runID, 0, starflow.NewStartEvent("script-hash", nil))
		require.NoError(t, err)
		_, err = s.RecordEvent(ctx, runID, 1, starflow.NewCallEvent("fn", nil))
		require.NoError(t, err)
		_, err = s.RecordEvent(ctx, runID, 2, starflow.NewYieldEvent("signal", runID))
		require.NoError(t, err)

		// Try to record call event after yield (should fail)
		_, err = s.RecordEvent(ctx, runID, 3, starflow.NewCallEvent("fn2", nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid event type")

		// Try to record return event after yield (should fail)
		_, err = s.RecordEvent(ctx, runID, 3, starflow.NewReturnEvent(nil, nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid event type")

		// Try to record finish event after yield (should fail)
		_, err = s.RecordEvent(ctx, runID, 3, starflow.NewFinishEvent(nil, nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid event type")
	})

	// GetEvents should return all events in order
	t.Run("GetEvents", func(t *testing.T) {
		s := newStore(t)
		runID := shortuuid.New()

		// Create a run with multiple events
		_, err := s.RecordEvent(ctx, runID, 0, starflow.NewStartEvent("script-hash", nil))
		require.NoError(t, err)
		_, err = s.RecordEvent(ctx, runID, 1, starflow.NewCallEvent("fn1", nil))
		require.NoError(t, err)
		_, err = s.RecordEvent(ctx, runID, 2, starflow.NewReturnEvent(nil, nil))
		require.NoError(t, err)
		_, err = s.RecordEvent(ctx, runID, 3, starflow.NewFinishEvent(nil, nil))
		require.NoError(t, err)

		// Get all events
		runEvents, err := s.GetEvents(ctx, runID)
		require.NoError(t, err)
		require.Len(t, runEvents, 4)
		require.Equal(t, starflow.EventTypeStart, runEvents[0].Type())
		require.Equal(t, starflow.EventTypeCall, runEvents[1].Type())
		require.Equal(t, starflow.EventTypeReturn, runEvents[2].Type())
		require.Equal(t, starflow.EventTypeFinish, runEvents[3].Type())

		// Test GetEvents for non-existent run
		runEvents, err = s.GetEvents(ctx, "non-existent-run")
		require.NoError(t, err)
		require.Empty(t, runEvents)
	})

	// Resume events can ignore sequential event ID requirement
	t.Run("ResumeEventsIgnoreSequentialEventID", func(t *testing.T) {
		s := newStore(t)
		runID := shortuuid.New()
		output := TestResponse{Message: "test"}

		// Create a run with call then yield
		_, err := s.RecordEvent(ctx, runID, 0, starflow.NewStartEvent("script-hash", nil))
		require.NoError(t, err)
		_, err = s.RecordEvent(ctx, runID, 1, starflow.NewCallEvent("fn", nil))
		require.NoError(t, err)
		_, err = s.RecordEvent(ctx, runID, 2, starflow.NewYieldEvent("signal", runID))
		require.NoError(t, err)

		// Resume event should work even with wrong nextEventID (this is the exception to sequential requirement)
		_, err = s.RecordEvent(ctx, runID, 999, starflow.NewResumeEvent("signal", output))
		require.NoError(t, err)
	})

	// RecordEvent should reject empty runID
	t.Run("RejectEmptyRunID", func(t *testing.T) {
		s := newStore(t)
		_, err := s.RecordEvent(ctx, "", 0, starflow.NewStartEvent("script-hash", nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "runID must not be empty")
	})
}
