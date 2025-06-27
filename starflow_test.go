package starflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// Test data structures for JSON-based tests
type PingRequest struct {
	Message string `json:"message"`
}

type PingResponse struct {
	Message string `json:"message"`
}

type ComplexRequest struct {
	Text   string            `json:"text"`
	Number int               `json:"number"`
	Flag   bool              `json:"flag"`
	Tags   []string          `json:"tags"`
	Meta   map[string]string `json:"meta"`
}

type ComplexResponse struct {
	Result string         `json:"result"`
	Data   map[string]any `json:"data"`
}

// WorkflowTestSuite provides a clean testing environment for starflow workflows.
type WorkflowTestSuite struct {
	suite.Suite
	store  Store
	client *Client[PingRequest, PingResponse]
}

// SetupTest initializes a fresh client for each test.
func (s *WorkflowTestSuite) SetupTest() {
	s.store = NewInMemoryStore()
	s.client = NewClient[PingRequest, PingResponse](s.store)

	// Register a standard ping function for tests
	RegisterFunc(s.client, s.pingPong, WithName("test.PingPong"))
}

// Helper: pingPong is a standard test function
func (s *WorkflowTestSuite) pingPong(ctx context.Context, req PingRequest) (PingResponse, error) {
	return PingResponse{Message: "pong: " + req.Message}, nil
}

// Helper: runScript executes a workflow script and returns the result
func (s *WorkflowTestSuite) runScript(script string, input PingRequest) (PingResponse, error) {
	runID := fmt.Sprintf("test-run-%d", time.Now().UnixNano())
	return s.client.Run(context.Background(), runID, []byte(script), input)
}

// Helper: mustRunScript executes a script and requires it to succeed
func (s *WorkflowTestSuite) mustRunScript(script string, input PingRequest) PingResponse {
	output, err := s.runScript(script, input)
	s.Require().NoError(err)
	return output
}

// Helper: getEvents retrieves events for a run (returns empty slice if not found)
func (s *WorkflowTestSuite) getEvents(runID string) []*Event {
	eventList, err := s.client.GetEvents(context.Background(), runID)
	if err != nil {
		return []*Event{} // Return empty slice for non-existent runs
	}
	return eventList
}

// Helper: expectEvents verifies event sequence
func (s *WorkflowTestSuite) expectEvents(runID string, expectedTypes ...EventType) {
	actualEvents := s.getEvents(runID)
	s.Require().Len(actualEvents, len(expectedTypes), "Event count mismatch")

	for i, expectedType := range expectedTypes {
		s.Require().Equal(expectedType, actualEvents[i].Type(), "Event %d type mismatch", i)
	}
}

// Helper: registerFunction adds a function to the client
func (s *WorkflowTestSuite) registerFunction(name string, fn any) {
	switch f := fn.(type) {
	case func(context.Context, PingRequest) (PingResponse, error):
		RegisterFunc(s.client, f, WithName(name))
	default:
		s.T().Fatalf("Unsupported function type: %T", fn)
	}
}

// Basic workflow execution with JSON
func (s *WorkflowTestSuite) TestBasicWorkflowExecution() {
	script := `
def main(ctx, input):
    return test.PingPong(ctx=ctx, req={"message": input["message"]})
`
	output := s.mustRunScript(script, PingRequest{Message: "hello"})
	s.Equal("pong: hello", output.Message)
}

// Test function registration with custom names
func (s *WorkflowTestSuite) TestFunctionRegistration() {
	customFn := func(ctx context.Context, req PingRequest) (PingResponse, error) {
		return PingResponse{Message: "custom: " + req.Message}, nil
	}
	s.registerFunction("custom.Function", customFn)

	script := `
def main(ctx, input):
    return custom.Function(ctx=ctx, req={"message": input["message"]})
`
	output := s.mustRunScript(script, PingRequest{Message: "test"})
	s.Equal("custom: test", output.Message)
}

// Test JSON integration with various data types
func (s *WorkflowTestSuite) TestJSONDataTypes() {
	script := `
def main(ctx, input):
    # Test various JSON data types
    data = {
        "string": "hello",
        "number": 42,
        "float": 3.14,
        "bool": True,
        "null": None,
        "array": [1, 2, 3],
        "object": {"nested": "value"}
    }
    
    # Use original input message
    data["original"] = input["message"]
    
    return {"message": "data processed", "data": data}
`
	output := s.mustRunScript(script, PingRequest{Message: "test"})
	s.Equal("data processed", output.Message)
}

// Test with nil/None values
func (s *WorkflowTestSuite) TestNilAndNoneHandling() {
	nilTestFn := func(ctx context.Context, req PingRequest) (PingResponse, error) {
		if req.Message == "" {
			return PingResponse{}, nil // Return zero value for empty message (nil input)
		}
		return PingResponse{Message: "not nil: " + req.Message}, nil
	}
	s.registerFunction("test.NilTest", nilTestFn)

	script := `
def main(ctx, input):
    if input["message"] == "test_nil":
        # Test calling with nil/None
        result = test.NilTest(ctx=ctx, req=None)
        return {"message": "nil result: " + (result["message"] if result["message"] else "empty")}
    else:
        result = test.NilTest(ctx=ctx, req={"message": input["message"]})
        return {"message": result["message"]}
`

	// Test with None
	output := s.mustRunScript(script, PingRequest{Message: "test_nil"})
	s.Equal("nil result: empty", output.Message)

	// Test with actual value
	output = s.mustRunScript(script, PingRequest{Message: "hello"})
	s.Equal("not nil: hello", output.Message)
}

// Test with basic primitive types
func (s *WorkflowTestSuite) TestBasicTypes() {
	// Test function that handles different types
	typeTestFn := func(ctx context.Context, req any) (any, error) {
		switch v := req.(type) {
		case string:
			return "string: " + v, nil
		case float64: // JSON numbers are float64
			return fmt.Sprintf("number: %.0f", v), nil
		case bool:
			return fmt.Sprintf("bool: %t", v), nil
		case map[string]any:
			if msg, ok := v["message"].(string); ok {
				return map[string]any{"result": "object: " + msg}, nil
			}
			return map[string]any{"result": "object: unknown"}, nil
		case []any:
			return fmt.Sprintf("array: %d items", len(v)), nil
		default:
			return "unknown type", nil
		}
	}

	client := NewClient[any, any](s.store)
	RegisterFunc(client, typeTestFn, WithName("test.TypeTest"))

	// Test string
	script := `
def main(ctx, input):
    return test.TypeTest(ctx=ctx, req="hello world")
`
	result, err := client.Run(context.Background(), "test-string", []byte(script), "input")
	s.Require().NoError(err)
	s.Equal("string: hello world", result)

	// Test number
	script = `
def main(ctx, input):
    return test.TypeTest(ctx=ctx, req=42)
`
	result, err = client.Run(context.Background(), "test-number", []byte(script), 0)
	s.Require().NoError(err)
	s.Equal("number: 42", result)

	// Test boolean
	script = `
def main(ctx, input):
    return test.TypeTest(ctx=ctx, req=True)
`
	result, err = client.Run(context.Background(), "test-bool", []byte(script), false)
	s.Require().NoError(err)
	s.Equal("bool: true", result)

	// Test array
	script = `
def main(ctx, input):
    return test.TypeTest(ctx=ctx, req=[1, 2, 3, 4, 5])
`
	result, err = client.Run(context.Background(), "test-array", []byte(script), []int{})
	s.Require().NoError(err)
	s.Equal("array: 5 items", result)
}

// Test complex nested structures
func (s *WorkflowTestSuite) TestComplexStructures() {
	complexFn := func(ctx context.Context, req ComplexRequest) (ComplexResponse, error) {
		return ComplexResponse{
			Result: fmt.Sprintf("processed: %s (%d)", req.Text, req.Number),
			Data: map[string]any{
				"flag":     req.Flag,
				"tagCount": len(req.Tags),
				"meta":     req.Meta,
			},
		}, nil
	}

	client := NewClient[ComplexRequest, ComplexResponse](s.store)
	RegisterFunc(client, complexFn, WithName("test.Complex"))

	script := `
def main(ctx, input):
    request = {
        "text": input["text"],
        "number": input["number"],
        "flag": input["flag"],
        "tags": input["tags"],
        "meta": input["meta"]
    }
    
    result = test.Complex(ctx=ctx, req=request)
    
    # Return modified response
    result["data"]["processed"] = True
    return result
`

	input := ComplexRequest{
		Text:   "hello world",
		Number: 42,
		Flag:   true,
		Tags:   []string{"tag1", "tag2", "tag3"},
		Meta:   map[string]string{"key1": "value1", "key2": "value2"},
	}

	output, err := client.Run(context.Background(), "test-complex", []byte(script), input)
	s.Require().NoError(err)

	s.Equal("processed: hello world (42)", output.Result)
	s.Equal(true, output.Data["flag"])
	s.Equal(float64(3), output.Data["tagCount"]) // JSON numbers are float64
	s.Equal(true, output.Data["processed"])

	meta, ok := output.Data["meta"].(map[string]any)
	s.Require().True(ok)
	s.Equal("value1", meta["key1"])
	s.Equal("value2", meta["key2"])
}

// Error handling - function errors
func (s *WorkflowTestSuite) TestFunctionErrorHandling() {
	failingFn := func(ctx context.Context, req PingRequest) (PingResponse, error) {
		return PingResponse{}, fmt.Errorf("intentional failure: %s", req.Message)
	}
	s.registerFunction("test.FailingFunction", failingFn)

	script := `
def main(ctx, input):
    return test.FailingFunction(ctx=ctx, req={"message": input["message"]})
`
	_, err := s.runScript(script, PingRequest{Message: "fail"})
	s.Require().Error(err)
	s.Contains(err.Error(), "intentional failure: fail")
}

// Error handling - panic recovery
func (s *WorkflowTestSuite) TestPanicRecovery() {
	panicFn := func(ctx context.Context, req PingRequest) (PingResponse, error) {
		panic("test panic")
	}
	s.registerFunction("test.PanicFunction", panicFn)

	script := `
def main(ctx, input):
    return test.PanicFunction(ctx=ctx, req={"message": input["message"]})
`
	_, err := s.runScript(script, PingRequest{Message: "panic"})
	s.Require().Error(err)
	s.Contains(err.Error(), "panic")
}

// Script validation - syntax errors
func (s *WorkflowTestSuite) TestScriptSyntaxValidation() {
	invalidScript := `
def main(ctx, input)  # Missing colon
    return {"message": "test"}
`
	_, err := s.runScript(invalidScript, PingRequest{Message: "test"})
	s.Require().Error(err)
	s.Contains(err.Error(), "syntax error")
}

// Script validation - missing main function
func (s *WorkflowTestSuite) TestScriptMainFunctionValidation() {
	scriptWithoutMain := `
def helper_function(ctx, input):
    return {"message": "test"}
`
	_, err := s.runScript(scriptWithoutMain, PingRequest{Message: "test"})
	s.Require().Error(err)
	s.Contains(err.Error(), "main function")
}

// Retry policy functionality
func (s *WorkflowTestSuite) TestRetryPolicy() {
	attempts := 0
	retryFn := func(ctx context.Context, req PingRequest) (PingResponse, error) {
		attempts++
		if attempts < 3 {
			return PingResponse{}, fmt.Errorf("transient error")
		}
		return PingResponse{Message: "success after retries"}, nil
	}

	policy := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Millisecond), 3)
	RegisterFunc(s.client, retryFn, WithName("test.RetryFunction"), WithRetryPolicy(policy))

	script := `
def main(ctx, input):
    return test.RetryFunction(ctx=ctx, req={"message": input["message"]})
`
	output := s.mustRunScript(script, PingRequest{Message: "retry"})
	s.Equal("success after retries", output.Message)
	s.Equal(3, attempts, "Expected exactly 3 attempts")
}

// Starlark math module integration
func (s *WorkflowTestSuite) TestStarlarkMathModule() {
	script := `
load("math", "sqrt")

def main(ctx, input):
    result = sqrt(16)
    return {"message": str(result)}
`
	output := s.mustRunScript(script, PingRequest{Message: "math"})
	s.Equal("4.0", output.Message)
}

// Deterministic time function
func (s *WorkflowTestSuite) TestDeterministicTimeFunction() {
	script := `
load("time", "now")

def main(ctx, input):
    timestamp = now(ctx=ctx)
    return {"message": "timestamp: " + timestamp}
`
	output := s.mustRunScript(script, PingRequest{Message: "time"})
	s.Contains(output.Message, "timestamp:")
}

// Deterministic random function
func (s *WorkflowTestSuite) TestDeterministicRandomFunction() {
	script := `
load("rand", "int")

def main(ctx, input):
    random_num = int(ctx=ctx, max=100)
    return {"message": "random: " + str(random_num)}
`
	output := s.mustRunScript(script, PingRequest{Message: "random"})
	s.Contains(output.Message, "random:")
}

// Sleep function with duration object
func (s *WorkflowTestSuite) TestSleepFunction() {
	script := `
load("time", "sleep", "millisecond")

def main(ctx, input):
    sleep(ctx=ctx, duration=millisecond)  # 1ms
    return {"message": "slept"}
`
	output := s.mustRunScript(script, PingRequest{Message: "sleep"})
	s.Equal("slept", output.Message)
}

// Sleep function with parsed duration
func (s *WorkflowTestSuite) TestSleepFunctionWithParsedDuration() {
	script := `
load("time", "sleep", "parse_duration")

def main(ctx, input):
    sleep(ctx=ctx, duration=parse_duration("1ms"))  # 1ms as parsed duration
    return {"message": "slept"}
`
	output := s.mustRunScript(script, PingRequest{Message: "sleep"})
	s.Equal("slept", output.Message)
}

// Sleep function with Duration object from time module
func (s *WorkflowTestSuite) TestSleepFunctionWithDuration() {
	script := `
load("time", "sleep", "parse_duration", "millisecond")

def main(ctx, input):
    # Sleep using parsed duration
    duration1 = parse_duration("1ms")
    sleep(ctx=ctx, duration=duration1)
    
    # Sleep using duration constant
    sleep(ctx=ctx, duration=millisecond)
    
    return {"message": "slept with duration objects"}
`
	output := s.mustRunScript(script, PingRequest{Message: "duration-sleep"})
	s.Equal("slept with duration objects", output.Message)
}

// Sleep function should reject non-Duration types
func (s *WorkflowTestSuite) TestSleepFunctionRejectsNonDuration() {
	script := `
load("time", "sleep")

def main(ctx, input):
    sleep(ctx=ctx, duration=1.0)  # This should fail - not a Duration object
    return {"message": "should not reach here"}
`
	_, err := s.client.Run(context.Background(), "test-reject-non-duration", []byte(script), PingRequest{Message: "test"})
	s.Require().Error(err)
	s.Contains(err.Error(), "duration must be a Duration object from the time module")
}

// Multiple function calls in sequence
func (s *WorkflowTestSuite) TestMultipleFunctionCalls() {
	firstFn := func(ctx context.Context, req PingRequest) (PingResponse, error) {
		return PingResponse{Message: "first: " + req.Message}, nil
	}
	secondFn := func(ctx context.Context, req PingRequest) (PingResponse, error) {
		return PingResponse{Message: "second: " + req.Message}, nil
	}

	s.registerFunction("test.FirstFunction", firstFn)
	s.registerFunction("test.SecondFunction", secondFn)

	script := `
def main(ctx, input):
    first_result = test.FirstFunction(ctx=ctx, req={"message": input["message"]})
    second_result = test.SecondFunction(ctx=ctx, req={"message": first_result["message"]})
    
    return {"message": "final: " + second_result["message"]}
`
	output := s.mustRunScript(script, PingRequest{Message: "chained"})
	s.Equal("final: second: first: chained", output.Message)
}

// Yield and resume functionality
func (s *WorkflowTestSuite) TestYieldAndResume() {
	var capturedRunID, capturedCID string
	yieldFn := func(ctx context.Context, req PingRequest) (PingResponse, error) {
		var err error
		capturedRunID, capturedCID, err = NewYieldError(ctx)
		return PingResponse{}, err
	}

	// Create separate client for this test to manage yield/resume
	store := NewInMemoryStore()
	client := NewClient[PingRequest, PingResponse](store)
	RegisterFunc(client, yieldFn, WithName("test.YieldFunction"))

	script := `
def main(ctx, input):
    test.YieldFunction(ctx=ctx, req={"message": input["message"]})
    return {"message": "resumed"}
`

	runID := "yield-test-run"

	// First run should yield (and return YieldError)
	_, err := client.Run(context.Background(), runID, []byte(script), PingRequest{Message: "test"})
	s.Require().Error(err)

	// Check that it's a yield error
	var yieldErr *YieldError
	s.Require().ErrorAs(err, &yieldErr)

	// Resume with signal
	resumeOutput := PingResponse{Message: "signal_value"}
	err = client.Signal(context.Background(), capturedRunID, capturedCID, resumeOutput)
	s.Require().NoError(err)

	// Now run again - it should complete
	output, err := client.Run(context.Background(), runID, []byte(script), PingRequest{Message: "test"})
	s.Require().NoError(err)
	s.Equal("resumed", output.Message)
}

// Event recording and retrieval
func (s *WorkflowTestSuite) TestEventRecording() {
	script := `
def main(ctx, input):
    return test.PingPong(ctx=ctx, req={"message": input["message"]})
`
	runID := "event-test-run"
	_, err := s.client.Run(context.Background(), runID, []byte(script), PingRequest{Message: "events"})
	s.Require().NoError(err)

	// Verify basic event sequence
	s.expectEvents(runID,
		EventTypeStart,
		EventTypeCall,
		EventTypeReturn,
		EventTypeFinish,
	)
}

// Test JSON encoding/decoding in scripts
func (s *WorkflowTestSuite) TestJSONEncodingDecoding() {
	script := `
load("json", "encode", "decode")

def main(ctx, input):
    # Create complex data structure
    data = {
        "input": input,
        "processing": {
            "step1": "encode to JSON",
            "step2": "decode from JSON"
        },
        "numbers": [1, 2, 3.14, 42],
        "flags": [True, False, None]
    }
    
    # Encode to JSON string
    json_str = encode(data)
    
    # Decode back from JSON  
    decoded = decode(json_str)
    
    # Verify round-trip works
    return {
        "message": "JSON test: " + decoded["input"]["message"]
    }
`
	output := s.mustRunScript(script, PingRequest{Message: "json-test"})
	s.Equal("JSON test: json-test", output.Message)
}

// Test resuming a yielded run with a different script should fail
func (s *WorkflowTestSuite) TestResumeWithDifferentScriptShouldFail() {
	yieldFn := func(ctx context.Context, req PingRequest) (PingResponse, error) {
		_, _, err := NewYieldError(ctx)
		return PingResponse{}, err
	}

	store := NewInMemoryStore()
	client := NewClient[PingRequest, PingResponse](store)
	RegisterFunc(client, yieldFn, WithName("test.YieldFunction"))

	script1 := `
def main(ctx, input):
    test.YieldFunction(ctx=ctx, req={"message": input["message"]})
    return {"message": "resumed from script1"}
`
	script2 := `
def main(ctx, input):
    test.YieldFunction(ctx=ctx, req={"message": input["message"]})
    return {"message": "resumed from script2"}
`

	runID := "resume-different-script-test"
	input := PingRequest{Message: "test"}

	// First run: should yield
	_, err := client.Run(context.Background(), runID, []byte(script1), input)
	s.Require().Error(err)

	// Get the signal ID from the yield event
	eventsList, err := client.GetEvents(context.Background(), runID)
	s.Require().NoError(err)
	var signalID string
	for _, ev := range eventsList {
		if y, ok := ev.Metadata.(YieldEvent); ok {
			signalID = y.SignalID()
		}
	}
	s.Require().NotEmpty(signalID)

	// Resume with signal (should succeed)
	resumeOutput := PingResponse{Message: "signal_value"}
	err = client.Signal(context.Background(), runID, signalID, resumeOutput)
	s.Require().NoError(err)

	// Now try to run again with a different script (should fail due to event mismatch)
	_, err = client.Run(context.Background(), runID, []byte(script2), input)
	s.Require().Error(err)
	s.Contains(err.Error(), "event mismatch")
}

// Comprehensive test with multiple yield points, time functions, retries, and event verification
func (s *WorkflowTestSuite) TestComplexWorkflowWithMultipleYieldsAndRetries() {
	// Create a separate client for this complex test with flexible types
	store := NewInMemoryStore()
	client := NewClient[map[string]any, map[string]any](store)

	// Track function call attempts for retry testing
	var checkpointAttempts int
	var dataProcessingAttempts int
	var finalValidationAttempts int

	// Checkpoint function that yields for external validation
	checkpointFn := func(ctx context.Context, req map[string]any) (map[string]any, error) {
		checkpointAttempts++
		if checkpointAttempts < 2 {
			return nil, fmt.Errorf("checkpoint service temporarily unavailable")
		}

		// Yield for external validation
		_, _, err := NewYieldError(ctx)
		return nil, err
	}

	// Data processing function that yields for approval
	dataProcessingFn := func(ctx context.Context, req map[string]any) (map[string]any, error) {
		dataProcessingAttempts++
		if dataProcessingAttempts < 3 {
			return nil, fmt.Errorf("data processing queue full")
		}

		// Yield for approval
		_, _, err := NewYieldError(ctx)
		return nil, err
	}

	// Final validation function that succeeds after retries
	finalValidationFn := func(ctx context.Context, req map[string]any) (map[string]any, error) {
		finalValidationAttempts++
		if finalValidationAttempts < 4 {
			return nil, fmt.Errorf("validation service overloaded")
		}

		return map[string]any{"message": "validation_complete"}, nil
	}

	// Register functions with retry policies
	checkpointPolicy := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Millisecond), 2)
	dataProcessingPolicy := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Millisecond), 3)
	finalValidationPolicy := backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Millisecond), 4)

	RegisterFunc(client, checkpointFn, WithName("test.Checkpoint"), WithRetryPolicy(checkpointPolicy))
	RegisterFunc(client, dataProcessingFn, WithName("test.DataProcessing"), WithRetryPolicy(dataProcessingPolicy))
	RegisterFunc(client, finalValidationFn, WithName("test.FinalValidation"), WithRetryPolicy(finalValidationPolicy))

	// Complex workflow script with multiple yield points and time/random functions
	script := `
load("time", "now", "sleep", "parse_duration")
load("rand", "int")

def main(ctx, input):
    # Record workflow start time
    start_time = now(ctx=ctx)
    
    # Generate a random session ID
    session_id = int(ctx=ctx, max=10000)
    
    # Phase 1: Initial checkpoint (will retry then yield)
    checkpoint_result = test.Checkpoint(ctx=ctx, req={"message": "session_" + str(session_id)})
    
    # Simulate processing delay
    sleep(ctx=ctx, duration=parse_duration("1ms"))
    
    # Phase 2: Data processing (will retry then yield)  
    processing_result = test.DataProcessing(ctx=ctx, req={"message": "data_from_checkpoint"})
    
    # Another processing delay
    sleep(ctx=ctx, duration=parse_duration("2ms"))
    
    # Generate another random value for final validation
    validation_id = int(ctx=ctx, max=1000)
    
    # Phase 3: Final validation (will retry but not yield)
    final_result = test.FinalValidation(ctx=ctx, req={"message": "validate_" + str(validation_id)})
    
    # Record completion time
    end_time = now(ctx=ctx)
    
    return {
        "message": "workflow_complete",
        "session_id": session_id,
        "validation_id": validation_id,
        "start_time": start_time,
        "end_time": end_time,
        "final_result": final_result["message"]
    }
`

	runID := "complex-workflow-test"
	input := map[string]any{"message": "complex_test"}

	// Phase 1: Initial run should yield at checkpoint
	_, err := client.Run(context.Background(), runID, []byte(script), input)
	s.Require().Error(err)
	var yieldErr *YieldError
	s.Require().ErrorAs(err, &yieldErr)

	// Get events after first yield
	events1, err := client.GetEvents(context.Background(), runID)
	s.Require().NoError(err)

	// Find the checkpoint yield signal ID
	var checkpointSignalID string
	for _, ev := range events1 {
		if y, ok := ev.Metadata.(YieldEvent); ok {
			checkpointSignalID = y.SignalID()
			break
		}
	}
	s.Require().NotEmpty(checkpointSignalID)

	// Resume checkpoint with approval
	checkpointApproval := map[string]any{"message": "checkpoint_approved"}
	err = client.Signal(context.Background(), runID, checkpointSignalID, checkpointApproval)
	s.Require().NoError(err)

	// Phase 2: Continue execution, should yield at data processing
	_, err = client.Run(context.Background(), runID, []byte(script), input)
	s.Require().Error(err)
	s.Require().ErrorAs(err, &yieldErr)

	// Get events after second yield
	events2, err := client.GetEvents(context.Background(), runID)
	s.Require().NoError(err)

	// Find the data processing yield signal ID
	var dataProcessingSignalID string
	for i := len(events2) - 1; i >= 0; i-- {
		if y, ok := events2[i].Metadata.(YieldEvent); ok {
			dataProcessingSignalID = y.SignalID()
			break
		}
	}
	s.Require().NotEmpty(dataProcessingSignalID)
	s.Require().NotEqual(checkpointSignalID, dataProcessingSignalID)

	// Resume data processing with approval
	dataProcessingApproval := map[string]any{"message": "data_processing_approved"}
	err = client.Signal(context.Background(), runID, dataProcessingSignalID, dataProcessingApproval)
	s.Require().NoError(err)

	// Phase 3: Final execution should complete successfully
	output, err := client.Run(context.Background(), runID, []byte(script), input)
	s.Require().NoError(err)
	s.Equal("workflow_complete", output["message"])
	s.Equal("validation_complete", output["final_result"])

	// Verify all retry attempts happened as expected
	// The retry policies handle failures at the infrastructure level
	s.Equal(3, checkpointAttempts, "Expected exactly 2 checkpoint attempts (with retries), got %d", checkpointAttempts)
	s.Equal(4, dataProcessingAttempts, "Expected exactly 3 data processing attempts (with retries), got %d", dataProcessingAttempts)
	s.Equal(4, finalValidationAttempts, "Expected exactly 4 final validation attempts (with retries), got %d", finalValidationAttempts)

	// Get final event trace for comprehensive verification
	finalEvents, err := client.GetEvents(context.Background(), runID)
	s.Require().NoError(err)

	// Verify the exact event sequence
	expectedEventTypes := []EventType{
		EventTypeStart,   // 1. Workflow starts
		EventTypeTimeNow, // 2. Record start time
		EventTypeRandInt, // 3. Generate session ID
		EventTypeCall,    // 4. Call test.Checkpoint (succeeds after retries, yields)
		EventTypeYield,   // 5. Yield for checkpoint approval
		EventTypeResume,  // 6. Resume with checkpoint approval
		EventTypeSleep,   // 7. Sleep 1ms
		EventTypeCall,    // 8. Call test.DataProcessing (succeeds after retries, yields)
		EventTypeYield,   // 9. Yield for data processing approval
		EventTypeResume,  // 10. Resume with data processing approval
		EventTypeSleep,   // 11. Sleep 2ms
		EventTypeRandInt, // 12. Generate validation ID
		EventTypeCall,    // 13. Call test.FinalValidation (succeeds after retries)
		EventTypeReturn,  // 14. Return from test.FinalValidation
		EventTypeTimeNow, // 15. Record end time
		EventTypeFinish,  // 16. Workflow completes
	}

	s.Require().Len(finalEvents, len(expectedEventTypes), "Event count mismatch")

	for i, expectedType := range expectedEventTypes {
		actualType := finalEvents[i].Type()
		s.Require().Equal(expectedType, actualType, "Event %d type mismatch: expected %v, got %v", i, expectedType, actualType)
	}

	// Verify specific event details for critical events
	var checkpointCallIdx, dataProcessingCallIdx, finalValidationCallIdx int
	var checkpointYieldSignal, dataProcessingYieldSignal string

	for i, ev := range finalEvents {
		switch ev.Type() {
		case EventTypeCall:
			if call, ok := ev.Metadata.(CallEvent); ok {
				switch call.FunctionName() {
				case "test.Checkpoint":
					checkpointCallIdx = i
				case "test.DataProcessing":
					dataProcessingCallIdx = i
				case "test.FinalValidation":
					finalValidationCallIdx = i
				}
			}
		case EventTypeYield:
			if yield, ok := ev.Metadata.(YieldEvent); ok {
				if checkpointYieldSignal == "" {
					checkpointYieldSignal = yield.SignalID()
				} else {
					dataProcessingYieldSignal = yield.SignalID()
				}
			}
		}
	}

	// Verify the function calls are at the expected positions
	s.Equal(3, checkpointCallIdx, "Checkpoint call should be at index 3")
	s.Equal(7, dataProcessingCallIdx, "Data processing call should be at index 7")
	s.Equal(12, finalValidationCallIdx, "Final validation call should be at index 12")

	// Verify yield/resume signal IDs match
	checkpointResumeEvent := finalEvents[5].Metadata.(ResumeEvent)
	s.Equal(checkpointYieldSignal, checkpointResumeEvent.SignalID(), "Checkpoint yield/resume signal IDs should match")

	dataProcessingResumeEvent := finalEvents[9].Metadata.(ResumeEvent)
	s.Equal(dataProcessingYieldSignal, dataProcessingResumeEvent.SignalID(), "Data processing yield/resume signal IDs should match")

	// Verify the final validation call succeeded
	finalValidationReturn := finalEvents[13].Metadata.(ReturnEvent)
	_, err = finalValidationReturn.Output()
	s.Require().NoError(err, "Final validation should succeed")
}

// Test Starlark time module with official functions
func (s *WorkflowTestSuite) TestStarlarkTimeModule() {
	script := `
load("time", "parse_duration", "from_timestamp", "second", "minute", "hour")

def main(ctx, input):
    # Test duration constants
    one_second = second
    one_minute = minute
    one_hour = hour
    
    # Test parse_duration
    duration = parse_duration("5s")
    
    # Test from_timestamp
    timestamp = from_timestamp(1640995200)  # 2022-01-01 00:00:00 UTC
    
    return {
        "message": "Time module: duration=" + str(type(duration)) + ", timestamp=" + str(type(timestamp)) + ", constants=" + str(one_second) + " " + str(one_minute) + " " + str(one_hour)
    }
`
	output := s.mustRunScript(script, PingRequest{Message: "time-module"})
	s.Contains(output.Message, "Time module:")
	s.Contains(output.Message, "duration")
	s.Contains(output.Message, "time")
	s.Contains(output.Message, "1s")
}

// Test that context from client is passed to registered functions
func (s *WorkflowTestSuite) TestContextPassedToRegisteredFunctions() {
	// Register a function that extracts information from the context
	contextTestFn := func(ctx context.Context, req PingRequest) (PingResponse, error) {
		// Extract run ID from context to verify it's the correct context
		runID, ok := GetRunID(ctx)
		if !ok {
			return PingResponse{}, fmt.Errorf("no run ID found in context")
		}

		// Check for the test value in context
		testValue := ctx.Value(WorkflowTestSuite{})
		if testValue == nil {
			return PingResponse{}, fmt.Errorf("no WorkflowTestSuite found in context")
		}

		return PingResponse{Message: fmt.Sprintf("context_test_passed_runid_%s", runID)}, nil
	}

	s.registerFunction("test.ContextTest", contextTestFn)

	script := `
def main(ctx, input):
    result = test.ContextTest(ctx=ctx, req={"message": input["message"]})
    return {"message": result["message"]}
`

	runID := "context-test-run-123"
	ctx := context.WithValue(context.Background(), WorkflowTestSuite{}, "fake-value")
	output, err := s.client.Run(ctx, runID, []byte(script), PingRequest{Message: "test"})
	s.Require().NoError(err)
	s.Contains(output.Message, "context_test_passed_runid_"+runID)
}

func TestWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(WorkflowTestSuite))
}

func TestInMemoryStoreBasics(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	// Test basic append functionality
	event1 := &Event{
		Timestamp: time.Now(),
		Metadata:  NewStartEvent("script-hash", nil),
	}

	// Serialize event to bytes
	event1Data, err := json.Marshal(event1)
	require.NoError(t, err)

	version, err := store.AppendEvent(ctx, "run1", 0, event1Data)
	require.NoError(t, err)
	require.Equal(t, 1, version)

	// Test concurrent update detection
	event2 := &Event{
		Timestamp: time.Now(),
		Metadata:  NewCallEvent("fn", nil),
	}

	// Serialize event to bytes
	event2Data, err := json.Marshal(event2)
	require.NoError(t, err)

	// Should fail with wrong version
	_, err = store.AppendEvent(ctx, "run1", 0, event2Data)
	require.ErrorIs(t, err, ErrConcurrentUpdate)

	// Should succeed with correct version
	version, err = store.AppendEvent(ctx, "run1", 1, event2Data)
	require.NoError(t, err)
	require.Equal(t, 2, version)

	// Test GetEvents
	eventDataList, err := store.GetEvents(ctx, "run1")
	require.NoError(t, err)
	require.Len(t, eventDataList, 2)

	// Deserialize events to verify types
	var retrievedEvent1, retrievedEvent2 Event
	err = json.Unmarshal(eventDataList[0], &retrievedEvent1)
	require.NoError(t, err)
	err = json.Unmarshal(eventDataList[1], &retrievedEvent2)
	require.NoError(t, err)

	require.Equal(t, EventTypeStart, retrievedEvent1.Type())
	require.Equal(t, EventTypeCall, retrievedEvent2.Type())
}

func TestValidateInvariants(t *testing.T) {
	t.Run("EmptyRunID", func(t *testing.T) {
		err := validateInvariants("", nil, NewStartEvent("script-hash", nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "runID must not be empty")
	})

	t.Run("StartEventOnlyForNewRuns", func(t *testing.T) {
		// Start event with no previous events should succeed
		err := validateInvariants("run1", nil, NewStartEvent("script-hash", nil))
		require.NoError(t, err)

		// Start event with existing events should fail
		lastEvent := NewStartEvent("script-hash", nil)
		err = validateInvariants("run1", lastEvent, NewStartEvent("script-hash", nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists")
	})

	t.Run("OtherEventsOnlyForExistingRuns", func(t *testing.T) {
		// Non-start event with no previous events should fail
		err := validateInvariants("run1", nil, NewCallEvent("fn", nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("NothingAllowedAfterFinish", func(t *testing.T) {
		finishEvent := NewFinishEvent(nil, nil)

		// Try to record another event after finish
		err := validateInvariants("run1", finishEvent, NewCallEvent("fn", nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "has already finished")
	})

	t.Run("OnlyYieldEventsCanBeResumed", func(t *testing.T) {
		callEvent := NewCallEvent("fn", nil)

		// Try to resume after a call event (should fail)
		err := validateInvariants("run1", callEvent, NewResumeEvent("signal", "output"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "not in yielded state")
	})

	t.Run("SignalIDMustMatchYieldEvent", func(t *testing.T) {
		yieldEvent := NewYieldEvent("signal1", "run1")

		// Try to resume with wrong signal ID
		err := validateInvariants("run1", yieldEvent, NewResumeEvent("signal2", "output"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "signal ID mismatch")

		// Correct signal ID should work
		err = validateInvariants("run1", yieldEvent, NewResumeEvent("signal1", "output"))
		require.NoError(t, err)
	})

	t.Run("OnlyCallEventsCanBeYielded", func(t *testing.T) {
		startEvent := NewStartEvent("script-hash", nil)

		// Try to yield after start event (should fail)
		err := validateInvariants("run1", startEvent, NewYieldEvent("signal", "run1"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid event type")
	})

	t.Run("OnlyCallEventsCanBeReturned", func(t *testing.T) {
		startEvent := NewStartEvent("script-hash", nil)

		// Try to return after start event (should fail)
		err := validateInvariants("run1", startEvent, NewReturnEvent(nil, nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid event type")
	})

	t.Run("OnlyYieldOrReturnAfterCall", func(t *testing.T) {
		callEvent := NewCallEvent("fn", nil)

		// Try to record another call event after call (should fail)
		err := validateInvariants("run1", callEvent, NewCallEvent("fn2", nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid event type")

		// Try to record finish event after call (should fail)
		err = validateInvariants("run1", callEvent, NewFinishEvent(nil, nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid event type")

		// Yield should work
		err = validateInvariants("run1", callEvent, NewYieldEvent("signal", "run1"))
		require.NoError(t, err)

		// Return should work
		err = validateInvariants("run1", callEvent, NewReturnEvent(nil, nil))
		require.NoError(t, err)
	})

	t.Run("OnlyResumeAfterYield", func(t *testing.T) {
		yieldEvent := NewYieldEvent("signal", "run1")

		// Try to record call event after yield (should fail)
		err := validateInvariants("run1", yieldEvent, NewCallEvent("fn2", nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid event type")

		// Try to record return event after yield (should fail)
		err = validateInvariants("run1", yieldEvent, NewReturnEvent(nil, nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid event type")

		// Try to record finish event after yield (should fail)
		err = validateInvariants("run1", yieldEvent, NewFinishEvent(nil, nil))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid event type")

		// Resume should work
		err = validateInvariants("run1", yieldEvent, NewResumeEvent("signal", "output"))
		require.NoError(t, err)
	})

	t.Run("ValidTransitions", func(t *testing.T) {
		// Start -> Call
		startEvent := NewStartEvent("script-hash", nil)
		err := validateInvariants("run1", startEvent, NewCallEvent("fn", nil))
		require.NoError(t, err)

		// Start -> Finish
		err = validateInvariants("run1", startEvent, NewFinishEvent(nil, nil))
		require.NoError(t, err)

		// Call -> Return
		callEvent := NewCallEvent("fn", nil)
		err = validateInvariants("run1", callEvent, NewReturnEvent(nil, nil))
		require.NoError(t, err)

		// Call -> Yield
		err = validateInvariants("run1", callEvent, NewYieldEvent("signal", "run1"))
		require.NoError(t, err)

		// Return -> Call
		returnEvent := NewReturnEvent(nil, nil)
		err = validateInvariants("run1", returnEvent, NewCallEvent("fn2", nil))
		require.NoError(t, err)

		// Return -> Finish
		err = validateInvariants("run1", returnEvent, NewFinishEvent(nil, nil))
		require.NoError(t, err)

		// Resume -> Call
		resumeEvent := NewResumeEvent("signal", "output")
		err = validateInvariants("run1", resumeEvent, NewCallEvent("fn2", nil))
		require.NoError(t, err)

		// Resume -> Finish
		err = validateInvariants("run1", resumeEvent, NewFinishEvent(nil, nil))
		require.NoError(t, err)
	})
}

func TestYieldError(t *testing.T) {
	ctx := withRunID(context.Background(), "run1")
	_, _, err := NewYieldError(ctx)
	require.True(t, errors.Is(err, &YieldError{}))

	var yErr *YieldError
	require.ErrorAs(t, err, &yErr)
}
