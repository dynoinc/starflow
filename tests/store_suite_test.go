package starflow_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dynoinc/starflow"
)

type storeFactory func(t *testing.T) starflow.Store

func runStoreSuite(t *testing.T, newStore storeFactory) {
	t.Helper()

	t.Run("SaveScript_Idempotent", func(t *testing.T) {
		s := newStore(t)
		content := []byte("print('hi')")
		h1, _ := s.SaveScript(content)
		h2, _ := s.SaveScript(content)
		if h1 != h2 {
			t.Fatalf("different hashes")
		}
		if got, _ := s.GetScript(h1); string(got) != string(content) {
			t.Fatalf("content mismatch")
		}
	})

	t.Run("CreateGetRun", func(t *testing.T) {
		s := newStore(t)
		id, _ := s.CreateRun("hash", nil)
		run, _ := s.GetRun(id)
		if run.ID != id {
			t.Fatalf("id mismatch")
		}
	})

	t.Run("OptimisticRecordEvent", func(t *testing.T) {
		s := newStore(t)
		id, _ := s.CreateRun("h", nil)
		evt := &starflow.Event{Timestamp: time.Now(), Type: starflow.EventTypeCall, FunctionName: "fn"}
		var wg sync.WaitGroup
		errs := make([]error, 2)
		wg.Add(2)
		for i := 0; i < 2; i++ {
			go func(k int) { defer wg.Done(); errs[k] = s.RecordEvent(id, 0, evt) }(i)
		}
		wg.Wait()
		var ok, concurrent int
		for _, e := range errs {
			if e == nil {
				ok++
			} else if e == starflow.ErrConcurrentUpdate {
				concurrent++
			}
		}
		if ok != 1 || concurrent != 1 {
			t.Fatalf("optimistic lock failed")
		}
	})

	t.Run("LeaseClaim", func(t *testing.T) {
		s := newStore(t)
		id, _ := s.CreateRun("x", nil)
		if ok, _ := s.ClaimRun(context.Background(), id, "w1", time.Now().Add(20*time.Millisecond)); !ok {
			t.Fatalf("claim1 fail")
		}
		if ok, _ := s.ClaimRun(context.Background(), id, "w2", time.Now().Add(20*time.Millisecond)); ok {
			t.Fatalf("claim2 should fail")
		}
		time.Sleep(25 * time.Millisecond)
		if ok, _ := s.ClaimRun(context.Background(), id, "w2", time.Now().Add(20*time.Millisecond)); !ok {
			t.Fatalf("claim after lease")
		}
	})
}
