package starflow_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/dynoinc/starflow"
	"github.com/dynoinc/starflow/events"
	starflowsuite "github.com/dynoinc/starflow/suite"
	testpb "github.com/dynoinc/starflow/suite/proto"
)

// WorkflowTestSuite provides a clean testing environment for starflow workflows.
type WorkflowTestSuite struct {
	suite.Suite
	store  starflow.Store
	client *starflow.Client[*testpb.PingRequest, *testpb.PingResponse]
}

// SetupTest initializes a fresh client for each test.
func (s *WorkflowTestSuite) SetupTest() {
	s.store = starflow.NewInMemoryStore()
	s.client = starflow.NewClient[*testpb.PingRequest, *testpb.PingResponse](s.store)
	s.client.RegisterProto(testpb.File_suite_proto_ping_proto)

	// Register a standard ping function for tests
	starflow.RegisterFunc(s.client, s.pingPong, starflow.WithName("test.PingPong"))
}

// Helper: pingPong is a standard test function
func (s *WorkflowTestSuite) pingPong(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
	return &testpb.PingResponse{Message: "pong: " + req.Message}, nil
}

// Helper: runScript executes a workflow script and returns the result
func (s *WorkflowTestSuite) runScript(script string, input *testpb.PingRequest) (*testpb.PingResponse, error) {
	runID := fmt.Sprintf("test-run-%d", time.Now().UnixNano())
	return s.client.Run(context.Background(), runID, []byte(script), input)
}

// Helper: mustRunScript executes a script and requires it to succeed
func (s *WorkflowTestSuite) mustRunScript(script string, input *testpb.PingRequest) *testpb.PingResponse {
	output, err := s.runScript(script, input)
	s.Require().NoError(err)
	return output
}

// Helper: getEvents retrieves events for a run (returns empty slice if not found)
func (s *WorkflowTestSuite) getEvents(runID string) []*events.Event {
	eventList, err := s.client.GetEvents(context.Background(), runID)
	if err != nil {
		return []*events.Event{} // Return empty slice for non-existent runs
	}
	return eventList
}

// Helper: expectEvents verifies event sequence
func (s *WorkflowTestSuite) expectEvents(runID string, expectedTypes ...events.EventType) {
	actualEvents := s.getEvents(runID)
	s.Require().Len(actualEvents, len(expectedTypes), "Event count mismatch")

	for i, expectedType := range expectedTypes {
		s.Require().Equal(expectedType, actualEvents[i].Type(), "Event %d type mismatch", i)
	}
}

// Helper: registerFunction adds a function to the client
func (s *WorkflowTestSuite) registerFunction(name string, fn interface{}) {
	switch f := fn.(type) {
	case func(context.Context, *testpb.PingRequest) (*testpb.PingResponse, error):
		starflow.RegisterFunc(s.client, f, starflow.WithName(name))
	default:
		s.T().Fatalf("Unsupported function type: %T", fn)
	}
}

// Basic workflow execution
func (s *WorkflowTestSuite) TestBasicWorkflowExecution() {
	script := `
load("proto", "proto")

def main(ctx, input):
    ping_proto = proto.file("suite/proto/ping.proto")
    return test.PingPong(ctx=ctx, req=ping_proto.PingRequest(message=input.message))
`
	output := s.mustRunScript(script, &testpb.PingRequest{Message: "hello"})
	s.Equal("pong: hello", output.Message)
}

// Function registration with custom names
func (s *WorkflowTestSuite) TestFunctionRegistration() {
	customFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		return &testpb.PingResponse{Message: "custom: " + req.Message}, nil
	}
	s.registerFunction("custom.Function", customFn)

	script := `
load("proto", "proto")

def main(ctx, input):
    ping_proto = proto.file("suite/proto/ping.proto")
    return custom.Function(ctx=ctx, req=ping_proto.PingRequest(message=input.message))
`
	output := s.mustRunScript(script, &testpb.PingRequest{Message: "test"})
	s.Equal("custom: test", output.Message)
}

// Protocol buffer integration
func (s *WorkflowTestSuite) TestProtocolBufferIntegration() {
	script := `
load("proto", "proto")

def main(ctx, input):
    ping_proto = proto.file("suite/proto/ping.proto")
    # Test creating protobuf messages in Starlark
    request = ping_proto.PingRequest(message="constructed in starlark")
    return test.PingPong(ctx=ctx, req=request)
`
	output := s.mustRunScript(script, &testpb.PingRequest{Message: "ignored"})
	s.Equal("pong: constructed in starlark", output.Message)
}

// Well-known protobuf types
func (s *WorkflowTestSuite) TestWellKnownTypes() {
	script := `
load("proto", "proto")

def main(ctx, input):
    wrappers_proto = proto.file("google/protobuf/wrappers.proto")
    ping_proto = proto.file("suite/proto/ping.proto")
    
    # Use StringValue wrapper
    string_val = wrappers_proto.StringValue(value="wrapped: " + input.message)
    return ping_proto.PingResponse(message=string_val.value)
`
	output := s.mustRunScript(script, &testpb.PingRequest{Message: "test"})
	s.Equal("wrapped: test", output.Message)
}

// Error handling - function errors
func (s *WorkflowTestSuite) TestFunctionErrorHandling() {
	failingFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		return nil, fmt.Errorf("intentional failure: %s", req.Message)
	}
	s.registerFunction("test.FailingFunction", failingFn)

	script := `
load("proto", "proto")

def main(ctx, input):
    ping_proto = proto.file("suite/proto/ping.proto")
    return test.FailingFunction(ctx=ctx, req=ping_proto.PingRequest(message=input.message))
`
	_, err := s.runScript(script, &testpb.PingRequest{Message: "fail"})
	s.Require().Error(err)
	s.Contains(err.Error(), "intentional failure: fail")
}

// Error handling - panic recovery
func (s *WorkflowTestSuite) TestPanicRecovery() {
	panicFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		panic("test panic")
	}
	s.registerFunction("test.PanicFunction", panicFn)

	script := `
load("proto", "proto")

def main(ctx, input):
    ping_proto = proto.file("suite/proto/ping.proto")
    return test.PanicFunction(ctx=ctx, req=ping_proto.PingRequest(message=input.message))
`
	_, err := s.runScript(script, &testpb.PingRequest{Message: "panic"})
	s.Require().Error(err)
	s.Contains(err.Error(), "panic")
}

// Script validation - syntax errors
func (s *WorkflowTestSuite) TestScriptSyntaxValidation() {
	invalidScript := `
load("proto", "proto")

def main(ctx, input)  # Missing colon
    return proto.file("suite/proto/ping.proto").PingResponse(message="test")
`
	_, err := s.runScript(invalidScript, &testpb.PingRequest{Message: "test"})
	s.Require().Error(err)
	s.Contains(err.Error(), "syntax error")
}

// Script validation - missing main function
func (s *WorkflowTestSuite) TestScriptMainFunctionValidation() {
	scriptWithoutMain := `
load("proto", "proto")

def helper_function(ctx, input):
    return proto.file("suite/proto/ping.proto").PingResponse(message="test")
`
	_, err := s.runScript(scriptWithoutMain, &testpb.PingRequest{Message: "test"})
	s.Require().Error(err)
	s.Contains(err.Error(), "main function")
}

// Retry policy functionality
func (s *WorkflowTestSuite) TestRetryPolicy() {
	attempts := 0
	retryFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		attempts++
		if attempts < 3 {
			return nil, fmt.Errorf("transient error")
		}
		return &testpb.PingResponse{Message: "success after retries"}, nil
	}

	policy := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Millisecond), 3)
	starflow.RegisterFunc(s.client, retryFn, starflow.WithName("test.RetryFunction"), starflow.WithRetryPolicy(policy))

	script := `
load("proto", "proto")

def main(ctx, input):
    ping_proto = proto.file("suite/proto/ping.proto")
    return test.RetryFunction(ctx=ctx, req=ping_proto.PingRequest(message=input.message))
`
	output := s.mustRunScript(script, &testpb.PingRequest{Message: "retry"})
	s.Equal("success after retries", output.Message)
	s.Equal(3, attempts, "Expected exactly 3 attempts")
}

// Starlark math module integration
func (s *WorkflowTestSuite) TestStarlarkMathModule() {
	script := `
load("proto", "proto")
load("math", "sqrt")

def main(ctx, input):
    result = sqrt(16)
    ping_proto = proto.file("suite/proto/ping.proto")
    return ping_proto.PingResponse(message=str(result))
`
	output := s.mustRunScript(script, &testpb.PingRequest{Message: "math"})
	s.Equal("4.0", output.Message)
}

// Deterministic time function
func (s *WorkflowTestSuite) TestDeterministicTimeFunction() {
	script := `
load("proto", "proto")  
load("time", "now")

def main(ctx, input):
    timestamp = now(ctx=ctx)
    ping_proto = proto.file("suite/proto/ping.proto")
    return ping_proto.PingResponse(message="timestamp: " + str(timestamp))
`
	output := s.mustRunScript(script, &testpb.PingRequest{Message: "time"})
	s.Contains(output.Message, "timestamp:")
}

// Deterministic random function
func (s *WorkflowTestSuite) TestDeterministicRandomFunction() {
	script := `
load("proto", "proto")
load("rand", "int")

def main(ctx, input):
    random_num = int(ctx=ctx, max=100)
    ping_proto = proto.file("suite/proto/ping.proto")
    return ping_proto.PingResponse(message="random: " + str(random_num))
`
	output := s.mustRunScript(script, &testpb.PingRequest{Message: "random"})
	s.Contains(output.Message, "random:")
}

// Sleep function with duration
func (s *WorkflowTestSuite) TestSleepFunction() {
	script := `
load("proto", "proto")
load("time", "sleep")

def main(ctx, input):
    duration_proto = proto.file("google/protobuf/duration.proto")
    sleep(ctx=ctx, duration=duration_proto.Duration(seconds=0, nanos=1000000))  # 1ms
    ping_proto = proto.file("suite/proto/ping.proto")
    return ping_proto.PingResponse(message="slept")
`
	output := s.mustRunScript(script, &testpb.PingRequest{Message: "sleep"})
	s.Equal("slept", output.Message)
}

// Multiple function calls in sequence
func (s *WorkflowTestSuite) TestMultipleFunctionCalls() {
	firstFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		return &testpb.PingResponse{Message: "first: " + req.Message}, nil
	}
	secondFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		return &testpb.PingResponse{Message: "second: " + req.Message}, nil
	}

	s.registerFunction("test.FirstFunction", firstFn)
	s.registerFunction("test.SecondFunction", secondFn)

	script := `
load("proto", "proto")

def main(ctx, input):
    ping_proto = proto.file("suite/proto/ping.proto")
    
    first_result = test.FirstFunction(ctx=ctx, req=ping_proto.PingRequest(message=input.message))
    second_result = test.SecondFunction(ctx=ctx, req=ping_proto.PingRequest(message=first_result.message))
    
    return ping_proto.PingResponse(message="final: " + second_result.message)
`
	output := s.mustRunScript(script, &testpb.PingRequest{Message: "chained"})
	s.Equal("final: second: first: chained", output.Message)
}

// Yield and resume functionality
func (s *WorkflowTestSuite) TestYieldAndResume() {
	var capturedRunID, capturedCID string
	yieldFn := func(ctx context.Context, req *testpb.PingRequest) (*testpb.PingResponse, error) {
		var err error
		capturedRunID, capturedCID, err = starflow.NewYieldError(ctx)
		return nil, err
	}

	// Create separate client for this test to manage yield/resume
	store := starflow.NewInMemoryStore()
	client := starflow.NewClient[*testpb.PingRequest, *testpb.PingResponse](store)
	client.RegisterProto(testpb.File_suite_proto_ping_proto)
	starflow.RegisterFunc(client, yieldFn, starflow.WithName("test.YieldFunction"))

	script := `
load("proto", "proto")

def main(ctx, input):
    ping_proto = proto.file("suite/proto/ping.proto")
    test.YieldFunction(ctx=ctx, req=ping_proto.PingRequest(message=input.message))
    return ping_proto.PingResponse(message="resumed")
`

	runID := "yield-test-run"

	// First run should yield (and return YieldError)
	_, err := client.Run(context.Background(), runID, []byte(script), &testpb.PingRequest{Message: "test"})
	s.Require().Error(err)

	// Check that it's a yield error
	var yieldErr *starflow.YieldError
	s.Require().ErrorAs(err, &yieldErr)

	// Resume with signal
	resumeOutput, err := anypb.New(&testpb.PingResponse{Message: "signal_value"})
	s.Require().NoError(err)

	err = client.Signal(context.Background(), capturedRunID, capturedCID, resumeOutput)
	s.Require().NoError(err)

	// Now run again - it should complete
	output, err := client.Run(context.Background(), runID, []byte(script), &testpb.PingRequest{Message: "test"})
	s.Require().NoError(err)
	s.Equal("resumed", output.Message)
}

// Event recording and retrieval
func (s *WorkflowTestSuite) TestEventRecording() {
	script := `
load("proto", "proto")

def main(ctx, input):
    ping_proto = proto.file("suite/proto/ping.proto")
    return test.PingPong(ctx=ctx, req=ping_proto.PingRequest(message=input.message))
`
	runID := "event-test-run"
	_, err := s.client.Run(context.Background(), runID, []byte(script), &testpb.PingRequest{Message: "events"})
	s.Require().NoError(err)

	// Verify basic event sequence
	s.expectEvents(runID,
		events.EventTypeStart,
		events.EventTypeCall,
		events.EventTypeReturn,
		events.EventTypeFinish,
	)
}

// In order for 'go test' to run this suite, we need to create a normal test function that calls suite.Run
func TestWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(WorkflowTestSuite))
}

// Store implementation compliance test
func TestInMemoryStore(t *testing.T) {
	starflowsuite.RunStoreSuite(t, func(t *testing.T) starflow.Store {
		return starflow.NewInMemoryStore()
	})
}
