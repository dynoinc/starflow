package starflow_test

import (
	"testing"
	"time"

	"github.com/dynoinc/starflow"
	"github.com/stretchr/testify/require"
)

type storeFactory func(t *testing.T) starflow.Store

func runStoreSuite(t *testing.T, newStore storeFactory) {
	t.Helper()

	ctx := t.Context()
	t.Run("SaveScript_Idempotent", func(t *testing.T) {
		s := newStore(t)
		content := []byte("print('hi')")
		h1, _ := s.SaveScript(ctx, content)
		h2, _ := s.SaveScript(ctx, content)
		if h1 != h2 {
			t.Fatalf("different hashes")
		}
		if got, _ := s.GetScript(t.Context(), h1); string(got) != string(content) {
			t.Fatalf("content mismatch")
		}
	})

	t.Run("CreateGetRun", func(t *testing.T) {
		s := newStore(t)
		id, _ := s.CreateRun(ctx, "hash", nil)
		run, _ := s.GetRun(ctx, id)
		if run.ID != id {
			t.Fatalf("id mismatch")
		}
	})

	t.Run("OptimisticRecordEvent", func(t *testing.T) {
		s := newStore(t)
		id, err := s.CreateRun(ctx, "h", nil)
		require.NoError(t, err)

		run, err := s.GetRun(ctx, id)
		require.NoError(t, err)

		evt := &starflow.Event{Timestamp: time.Now(), Type: starflow.EventTypeCall, FunctionName: "fn"}
		err = s.RecordEvent(ctx, run, evt)
		require.NoError(t, err)

		err = s.RecordEvent(ctx, run, evt)
		require.Error(t, err)
		require.Equal(t, err, starflow.ErrConcurrentUpdate)
	})

	t.Run("LeaseClaim", func(t *testing.T) {
		s := newStore(t)
		id, err := s.CreateRun(ctx, "x", nil)
		require.NoError(t, err)

		if ok, _ := s.ClaimRun(ctx, id, "w1", time.Now().Add(20*time.Millisecond)); !ok {
			t.Fatalf("claim1 fail")
		}
		if ok, _ := s.ClaimRun(ctx, id, "w2", time.Now().Add(20*time.Millisecond)); ok {
			t.Fatalf("claim2 should fail")
		}
		time.Sleep(25 * time.Millisecond)
		if ok, _ := s.ClaimRun(ctx, id, "w2", time.Now().Add(20*time.Millisecond)); !ok {
			t.Fatalf("claim after lease")
		}
	})
}
