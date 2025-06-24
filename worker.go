package starflow

import (
	"context"
	"sync"
	"time"

	"github.com/lithammer/shortuuid/v4"
	"google.golang.org/protobuf/proto"
)

// Worker executes pending workflow runs in the background.
type Worker[Input proto.Message, Output proto.Message] struct {
	wf       *Workflow[Input, Output]
	workerID string
	poll     time.Duration
}

// NewWorker creates a worker for a workflow with the given poll interval.
func (w *Workflow[Input, Output]) NewWorker(poll time.Duration) *Worker[Input, Output] {
	if poll == 0 {
		poll = time.Second
	}
	return &Worker[Input, Output]{wf: w, workerID: shortuuid.New(), poll: poll}
}

// ProcessOnce processes all runs that are in PENDING or WAITING state exactly once.
func (wk *Worker[Input, Output]) ProcessOnce(ctx context.Context) {
	runs, err := wk.wf.store.ListRuns(ctx, RunStatusPending)
	if err != nil {
		return
	}

	var wg sync.WaitGroup
	for _, r := range runs {
		wg.Add(1)
		go func(run *Run) {
			defer wg.Done()
			leaseUntil := time.Now().Add(5 * time.Second)
			ok, err := wk.wf.store.ClaimRun(ctx, run.ID, wk.workerID, leaseUntil)
			if err != nil || !ok {
				return
			}

			_, _ = wk.wf.resumeRun(ctx, run.ID)
		}(r)
	}
	wg.Wait()
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
