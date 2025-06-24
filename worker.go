package starflow

import (
	"context"
	"time"

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
			// transition to pending first
			_ = wk.wf.store.UpdateRunStatus(ctx, r.ID, RunStatusPending)
			_ = wk.wf.store.UpdateRunWakeUp(ctx, r.ID, nil)
		}

		// Attempt to claim the run by setting it to RUNNING; ignore if already claimed.
		if err := wk.wf.store.UpdateRunStatus(ctx, r.ID, RunStatusRunning); err != nil {
			continue
		}
		// Execute the run; outcome handling is inside execute/resumeRun.
		_, _ = wk.wf.resumeRun(ctx, r.ID)
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
