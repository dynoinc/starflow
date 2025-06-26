package starflow

import (
	"context"
	"fmt"
	"log/slog"
	"path"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/lithammer/shortuuid/v4"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/dynoinc/starflow/events"

	_ "google.golang.org/protobuf/types/known/anypb"
	_ "google.golang.org/protobuf/types/known/durationpb"
	_ "google.golang.org/protobuf/types/known/emptypb"
	_ "google.golang.org/protobuf/types/known/structpb"
	_ "google.golang.org/protobuf/types/known/timestamppb"
	_ "google.golang.org/protobuf/types/known/typepb"
	_ "google.golang.org/protobuf/types/known/wrapperspb"
)

type registeredFn struct {
	fn          func(ctx context.Context, req proto.Message) (proto.Message, error)
	reqType     proto.Message
	resType     proto.Message
	name        string
	retryPolicy backoff.BackOff
}

// Option configures behaviour of a registered function.
type Option func(*registeredFn)

// WithName overrides the automatically derived name for the function.
// The name must be in the format "module.funcname".
func WithName(name string) Option {
	return func(rf *registeredFn) {
		// Validate that the name is in the correct format
		if !strings.Contains(name, ".") {
			panic(fmt.Sprintf("function name must be in format 'module.funcname', got: %s", name))
		}
		rf.name = name
	}
}

// WithRetryPolicy specifies a retry policy for the function.
func WithRetryPolicy(b backoff.BackOff) Option {
	return func(rf *registeredFn) {
		rf.retryPolicy = b
	}
}

// WorkerOption configures behaviour of a worker.
type WorkerOption func(*Worker[proto.Message, proto.Message])

// WithPollInterval sets the poll interval for the worker.
func WithPollInterval(interval time.Duration) WorkerOption {
	return func(w *Worker[proto.Message, proto.Message]) {
		w.pollDuration = interval
	}
}

// WithLeaseDuration sets the lease duration for workflow runs.
// The lease will be renewed at half this duration.
func WithLeaseDuration(duration time.Duration) WorkerOption {
	return func(w *Worker[proto.Message, proto.Message]) {
		w.leaseDuration = duration
		w.leaseRenewalRate = duration / 2
	}
}

// WithLeaseRenewalRate sets the lease renewal rate.
// This should be less than the lease duration to ensure continuous renewal.
func WithLeaseRenewalRate(rate time.Duration) WorkerOption {
	return func(w *Worker[proto.Message, proto.Message]) {
		w.leaseRenewalRate = rate
	}
}

// customProtoRegistry combines the global registry with custom proto files
type customProtoRegistry struct {
	globalRegistry *protoregistry.Files
	customFiles    map[string]protoreflect.FileDescriptor
}

func newCustomProtoRegistry() *customProtoRegistry {
	return &customProtoRegistry{
		globalRegistry: protoregistry.GlobalFiles,
		customFiles:    make(map[string]protoreflect.FileDescriptor),
	}
}

// Worker executes pending workflow runs in the background.
// It polls the store for claimable runs and processes them using registered functions.
// The worker is parameterized by the input and output types which must be protobuf messages.
type Worker[Input proto.Message, Output proto.Message] struct {
	store    Store
	workerID string

	registry      map[string]registeredFn
	types         map[string]proto.Message
	protoRegistry *customProtoRegistry

	pollDuration     time.Duration
	leaseDuration    time.Duration
	leaseRenewalRate time.Duration
}

// NewWorker creates a worker for a workflow with the given poll interval.
func NewWorker[Input proto.Message, Output proto.Message](store Store, opts ...WorkerOption) *Worker[Input, Output] {
	worker := &Worker[Input, Output]{
		store:    store,
		workerID: shortuuid.New(),

		registry:      make(map[string]registeredFn),
		types:         make(map[string]proto.Message),
		protoRegistry: newCustomProtoRegistry(),

		// Default lease management - 30 second lease, renew every 15 seconds
		pollDuration:     time.Second,
		leaseDuration:    30 * time.Second,
		leaseRenewalRate: 15 * time.Second,
	}

	for _, opt := range opts {
		opt((*Worker[proto.Message, proto.Message])(worker))
	}

	return worker
}

// Register registers a Go function to be callable from Starlark using generics and reflection.
// The function must have the signature: func(ctx context.Context, req ReqType) (ResType, error)
// where ReqType and ResType implement proto.Message.
//
// The function will be automatically named based on its package and function name,
// or you can override this using the WithName option.
func Register[Input proto.Message, Output proto.Message, Req proto.Message, Res proto.Message](
	w *Worker[Input, Output],
	fn func(ctx context.Context, req Req) (Res, error),
	opts ...Option,
) {
	// Create instances to get the types (not zero values)
	var req Req
	var res Res

	// Use reflection to create actual instances
	reqType := reflect.TypeOf(req).Elem()
	resType := reflect.TypeOf(res).Elem()

	reqInstance := reflect.New(reqType).Interface().(proto.Message)
	resInstance := reflect.New(resType).Interface().(proto.Message)

	// Wrap the typed function to match the expected signature
	wrappedFn := func(ctx context.Context, reqMsg proto.Message) (proto.Message, error) {
		// Cast the proto.Message to the specific type
		typedReq, ok := reqMsg.(Req)
		if !ok {
			return nil, fmt.Errorf("invalid request type: expected %T, got %T", req, reqMsg)
		}

		return fn(ctx, typedReq)
	}

	reg := registeredFn{
		fn:      wrappedFn,
		reqType: reqInstance,
		resType: resInstance,
	}
	for _, o := range opts {
		o(&reg)
	}

	if reg.name == "" {
		full := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
		function := strings.TrimPrefix(path.Ext(full), ".")
		module := path.Base(strings.TrimSuffix(full, path.Ext(full)))
		reg.name = module + "." + function
	}

	w.registry[reg.name] = reg
	w.types[reg.name] = reqInstance
	w.types[reg.name+"_response"] = resInstance
}

// RegisterProto registers a proto file descriptor with the worker's proto registry.
// This allows Starlark scripts to access the proto definitions.
func (w *Worker[Input, Output]) RegisterProto(fileDescriptor protoreflect.FileDescriptor) {
	w.protoRegistry.customFiles[fileDescriptor.Path()] = fileDescriptor
}

// ProcessOnce processes all runs that are in PENDING or WAITING state exactly once.
// This method is useful for manual processing or testing scenarios.
// For continuous processing, use Start() instead.
func (w *Worker[Input, Output]) ProcessOnce(ctx context.Context) {
	runs, err := w.store.ClaimableRuns(ctx)
	if err != nil {
		return
	}

	var wg sync.WaitGroup
	for _, run := range runs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.processRun(ctx, run); err != nil {
				slog.Error("failed to process run", "run_id", run.ID, "error", err)
			}
		}()
	}
	wg.Wait()
}

func (w *Worker[Input, Output]) processRun(ctx context.Context, run *Run) error {
	leaseCtx, leaseCancel := context.WithCancel(ctx)
	defer leaseCancel()

	recorder := newRecorder(w.store, run)

	// Initial lease claim
	// This will update run.LeasedUntil in the store
	if err := recorder.recordEvent(
		ctx,
		events.NewClaimEvent(w.workerID, time.Now().Add(w.leaseDuration)),
	); err != nil {
		return fmt.Errorf("failed to claim run: %w", err)
	}

	// Use a channel to signal completion or error from the main execution
	done := make(chan error, 1)
	go func() {
		_, err := runThread(leaseCtx, w, run, recorder)
		done <- err
	}()

	// Set up a timer for lease management
	timer := time.NewTimer(w.leaseRenewalRate)
	defer timer.Stop()

	for {
		select {
		case err := <-done:
			// Main execution finished, return its error (or nil)
			return err
		case <-timer.C:
			// Time to renew the lease
			// This will update run.LeasedUntil in the store again
			if err := recorder.recordEvent(
				ctx,
				events.NewClaimEvent(w.workerID, time.Now().Add(w.leaseDuration)),
			); err != nil {
				// If renewal fails, cancel the lease context and return the error
				leaseCancel()
				return fmt.Errorf("failed to renew lease: %w", err)
			}
			// Reset the timer for the next renewal
			timer.Reset(w.leaseRenewalRate)
		case <-ctx.Done():
			// Worker context cancelled, stop processing
			return ctx.Err()
		}
	}
}

// Start begins a background goroutine that polls for and executes pending runs.
func (w *Worker[Input, Output]) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(w.pollDuration)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.ProcessOnce(ctx)
			}
		}
	}()
}

// GetLeaseDuration returns the current lease duration.
func (w *Worker[Input, Output]) GetLeaseDuration() time.Duration {
	return w.leaseDuration
}

// GetLeaseRenewalRate returns the current lease renewal rate.
func (w *Worker[Input, Output]) GetLeaseRenewalRate() time.Duration {
	return w.leaseRenewalRate
}

// RegisteredNames returns the registered function names (for testing/debugging)
func (w *Worker[Input, Output]) RegisteredNames() []string {
	names := make([]string, 0, len(w.registry))
	for name := range w.registry {
		names = append(names, name)
	}
	return names
}

func (r *customProtoRegistry) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	// Try custom files first
	for _, fd := range r.customFiles {
		if desc := fd.Messages().ByName(name.Name()); desc != nil {
			return desc, nil
		}
		if desc := fd.Enums().ByName(name.Name()); desc != nil {
			return desc, nil
		}
		if desc := fd.Services().ByName(name.Name()); desc != nil {
			return desc, nil
		}
	}
	// Then try global registry
	return r.globalRegistry.FindDescriptorByName(name)
}

func (r *customProtoRegistry) FindFileByPath(path string) (protoreflect.FileDescriptor, error) {
	if fd, ok := r.customFiles[path]; ok {
		return fd, nil
	}
	return r.globalRegistry.FindFileByPath(path)
}

type eventRecorder struct {
	runID string
	store Store

	mu          sync.Mutex
	nextEventID int64
}

func newRecorder(store Store, run *Run) *eventRecorder {
	return &eventRecorder{
		runID:       run.ID,
		nextEventID: run.NextEventID,
		store:       store,
	}
}

func (r *eventRecorder) recordEvent(ctx context.Context, event events.EventMetadata) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	nextEventID, err := r.store.RecordEvent(ctx, r.runID, r.nextEventID, event)
	if err != nil {
		return fmt.Errorf("failed to record event: %w", err)
	}

	r.nextEventID = nextEventID
	return nil
}
