package starflow

import (
	"context"
	"errors"
	"time"

	"github.com/lithammer/shortuuid/v4"
	"google.golang.org/protobuf/proto"
)

// Worker executes pending workflow runs in the background.
type Worker[Input proto.Message, Output proto.Message] struct {
	wf   *Workflow[Input, Output]
	poll time.Duration
}

// NewWorker creates a worker for a workflow with the given poll interval.
func (w *Workflow[Input, Output]) NewWorker(poll time.Duration) *Worker[Input, Output] {
	if poll == 0 {
		poll = time.Second
	}
	return &Worker[Input, Output]{wf: w, poll: poll}
}

// ProcessOnce processes all runs that are in PENDING or WAITING state exactly once.
func (wk *Worker[Input, Output]) ProcessOnce(ctx context.Context) {
	now := time.Now()
	runs, err := wk.wf.store.ListRuns(ctx, RunStatusPending, RunStatusWaiting)
	if err != nil {
		return
	}
	for _, r := range runs {
		// If waiting, ensure wake time reached
		if r.Status == RunStatusWaiting {
			if r.WakeAt == nil || r.WakeAt.After(now) {
				continue // not yet
			}
		}

		leaseUntil := time.Now().Add(5 * time.Second)
		workerID := "worker" + shortuuid.New()
		ok, err := wk.wf.store.ClaimRun(ctx, r.ID, workerID, leaseUntil)
		if err != nil || !ok {
			continue
		}
		// Execute the run; outcome handling is inside execute/resumeRun.
		_, err = wk.wf.resumeRun(ctx, r.ID)
		if errors.Is(err, ErrConcurrentUpdate) {
			continue
		}
	}
}

// Start begins a background goroutine that polls for and executes pending runs.
func (wk *Worker[Input, Output]) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(wk.poll)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				wk.ProcessOnce(ctx)
			}
		}
	}()
}
