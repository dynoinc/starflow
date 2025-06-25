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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

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
type Worker[Input proto.Message, Output proto.Message] struct {
	store    Store
	workerID string
	poll     time.Duration

	tracer        trace.Tracer
	registry      map[string]registeredFn
	types         map[string]proto.Message
	protoRegistry protodesc.Resolver
}

// NewWorker creates a worker for a workflow with the given poll interval.
func NewWorker[Input proto.Message, Output proto.Message](store Store, poll time.Duration) *Worker[Input, Output] {
	if poll == 0 {
		poll = time.Second
	}

	// Create a new proto registry that includes well-known types
	protoRegistry := newCustomProtoRegistry()

	return &Worker[Input, Output]{
		store:    store,
		workerID: shortuuid.New(),
		poll:     poll,

		tracer:        otel.Tracer("starflow.worker"),
		registry:      make(map[string]registeredFn),
		types:         make(map[string]proto.Message),
		protoRegistry: protoRegistry,
	}
}

// Register registers a Go function to be callable from Starlark using generics and reflection.
// The function must have the signature: func(ctx context.Context, req ReqType) (ResType, error)
// where ReqType and ResType implement proto.Message.
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
func (w *Worker[Input, Output]) RegisterProto(fileDescriptor protoreflect.FileDescriptor) error {
	if reg, ok := w.protoRegistry.(*customProtoRegistry); ok {
		reg.customFiles[fileDescriptor.Path()] = fileDescriptor
		return nil
	}
	return fmt.Errorf("protoRegistry is not a customProtoRegistry")
}

// ProcessOnce processes all runs that are in PENDING or WAITING state exactly once.
func (w *Worker[Input, Output]) ProcessOnce(ctx context.Context) {
	runs, err := w.store.ListRunsForClaiming(ctx, 30*time.Second)
	if err != nil {
		return
	}

	var wg sync.WaitGroup
	for _, r := range runs {
		wg.Add(1)
		go func(run *Run) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			// Try to claim the run by recording a ClaimEvent
			_, err := w.store.RecordEvent(ctx, run.ID, run.NextEventID, ClaimEvent{})
			if err != nil {
				// Another worker claimed it first
				return
			}

			// Get the updated run with the new NextEventID
			updatedRun, err := w.store.GetRun(ctx, run.ID)
			if err != nil {
				slog.Error("failed to get updated run", "run_id", run.ID, "error", err)
				return
			}

			if _, err := runThread(ctx, w, updatedRun); err != nil {
				slog.Error("failed to resume run", "run_id", run.ID, "error", err)
			}
		}(r)
	}
	wg.Wait()
}

// Start begins a background goroutine that polls for and executes pending runs.
func (w *Worker[Input, Output]) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(w.poll)
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
