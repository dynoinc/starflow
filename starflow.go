package starflow

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lithammer/shortuuid/v4"
	"google.golang.org/protobuf/types/known/anypb"
)

// Context key for runID
type runIDKey struct{}

// WithRunID adds runID to the context
func WithRunID(ctx context.Context, runID string) context.Context {
	return context.WithValue(ctx, runIDKey{}, runID)
}

// GetRunID extracts runID from context
func GetRunID(ctx context.Context) (string, bool) {
	runID, ok := ctx.Value(runIDKey{}).(string)
	return runID, ok
}

type YieldError struct {
	cid   string
	runID string
}

func (e *YieldError) Error() string {
	return fmt.Sprintf("yield error: %s (run: %s)", e.cid, e.runID)
}

func NewYieldError(ctx context.Context) (string, error) {
	runID, ok := GetRunID(ctx)
	if !ok {
		return "", fmt.Errorf("runID not found in context")
	}
	cid := shortuuid.New()
	return cid, &YieldError{cid: cid, runID: runID}
}

// RunStatus represents the status of a workflow run.
type RunStatus string

const (
	// RunStatusRunning indicates that the workflow is currently running.
	RunStatusRunning RunStatus = "RUNNING"
	// RunStatusCompleted indicates that the workflow has completed successfully.
	RunStatusCompleted RunStatus = "COMPLETED"
	// RunStatusFailed indicates that the workflow has failed.
	RunStatusFailed RunStatus = "FAILED"
	// RunStatusPending indicates that the workflow has been created and is waiting to be picked up by a worker.
	RunStatusPending RunStatus = "PENDING"
	// RunStatusYielded indicates that the workflow has yielded and waiting for signal to resume.
	RunStatusYielded RunStatus = "YIELDED"
)

// Run represents a single execution of a workflow.
type Run struct {
	// Fixed on creation
	ID         string
	ScriptHash string
	Input      *anypb.Any
	CreatedAt  time.Time

	// Updated at each event
	Status      RunStatus
	NextEventID int64
	UpdatedAt   time.Time

	// Set when finished.
	Output *anypb.Any
	Error  error
}

// EventType represents the type of an event in the execution history.
type EventType string

const (
	EventTypeCall    EventType = "CALL"
	EventTypeReturn  EventType = "RETURN"
	EventTypeSleep   EventType = "SLEEP"
	EventTypeTimeNow EventType = "TIME_NOW"
	EventTypeRandInt EventType = "RAND_INT"
	EventTypeYield   EventType = "YIELD"
	EventTypeResume  EventType = "RESUME"
	EventTypeFinish  EventType = "FINISH"
	EventTypeClaim   EventType = "CLAIM"
)

// EventMetadata interface for different event types
type EventMetadata interface {
	EventType() EventType
}

// CallEvent metadata
// Use constructor: NewCallEvent
// Accessors: FunctionName(), Input()
type CallEvent struct {
	functionName string
	input        *anypb.Any
}

func NewCallEvent(functionName string, input *anypb.Any) CallEvent {
	return CallEvent{functionName: functionName, input: input}
}

func (c CallEvent) EventType() EventType { return EventTypeCall }
func (c CallEvent) FunctionName() string { return c.functionName }
func (c CallEvent) Input() *anypb.Any    { return c.input }

// MarshalJSON implements custom JSON marshaling
func (c CallEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"functionName": c.functionName,
		"input":        c.input,
	})
}

// UnmarshalJSON implements custom JSON unmarshaling
func (c *CallEvent) UnmarshalJSON(data []byte) error {
	var aux struct {
		FunctionName string     `json:"functionName"`
		Input        *anypb.Any `json:"input"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	c.functionName = aux.FunctionName
	c.input = aux.Input
	return nil
}

// ReturnEvent metadata
// Use constructor: NewReturnEvent
// Accessor: Output() returns (output, error)
type ReturnEvent struct {
	output *anypb.Any
	err    error
}

func NewReturnEvent(output *anypb.Any, err error) ReturnEvent {
	return ReturnEvent{output: output, err: err}
}

func (r ReturnEvent) EventType() EventType        { return EventTypeReturn }
func (r ReturnEvent) Output() (*anypb.Any, error) { return r.output, r.err }

// MarshalJSON implements custom JSON marshaling
func (r ReturnEvent) MarshalJSON() ([]byte, error) {
	var errStr string
	if r.err != nil {
		errStr = r.err.Error()
	}
	return json.Marshal(map[string]interface{}{
		"output": r.output,
		"error":  errStr,
	})
}

// UnmarshalJSON implements custom JSON unmarshaling
func (r *ReturnEvent) UnmarshalJSON(data []byte) error {
	var aux struct {
		Output *anypb.Any `json:"output"`
		Error  string     `json:"error"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	r.output = aux.Output
	if aux.Error != "" {
		r.err = fmt.Errorf("%s", aux.Error)
	}
	return nil
}

// SleepEvent
// Use constructor: NewSleepEvent
// Accessor: WakeupAt()
type SleepEvent struct {
	wakeupAt time.Time
}

func NewSleepEvent(wakeupAt time.Time) SleepEvent {
	return SleepEvent{wakeupAt: wakeupAt}
}

func (s SleepEvent) EventType() EventType { return EventTypeSleep }
func (s SleepEvent) WakeupAt() time.Time  { return s.wakeupAt }

// MarshalJSON implements custom JSON marshaling
func (s SleepEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"wakeupAt": s.wakeupAt,
	})
}

// UnmarshalJSON implements custom JSON unmarshaling
func (s *SleepEvent) UnmarshalJSON(data []byte) error {
	var aux struct {
		WakeupAt time.Time `json:"wakeupAt"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	s.wakeupAt = aux.WakeupAt
	return nil
}

// TimeNowEvent
// Use constructor: NewTimeNowEvent
// Accessor: Timestamp()
type TimeNowEvent struct {
	timestamp time.Time
}

func NewTimeNowEvent(timestamp time.Time) TimeNowEvent {
	return TimeNowEvent{timestamp: timestamp}
}

func (t TimeNowEvent) EventType() EventType { return EventTypeTimeNow }
func (t TimeNowEvent) Timestamp() time.Time { return t.timestamp }

// MarshalJSON implements custom JSON marshaling
func (t TimeNowEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"timestamp": t.timestamp,
	})
}

// UnmarshalJSON implements custom JSON unmarshaling
func (t *TimeNowEvent) UnmarshalJSON(data []byte) error {
	var aux struct {
		Timestamp time.Time `json:"timestamp"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	t.timestamp = aux.Timestamp
	return nil
}

// RandIntEvent
// Use constructor: NewRandIntEvent
// Accessors: Max(), Result()
type RandIntEvent struct {
	max    int64
	result int64
}

func NewRandIntEvent(max, result int64) RandIntEvent {
	return RandIntEvent{max: max, result: result}
}

func (r RandIntEvent) EventType() EventType { return EventTypeRandInt }
func (r RandIntEvent) Max() int64           { return r.max }
func (r RandIntEvent) Result() int64        { return r.result }

// MarshalJSON implements custom JSON marshaling
func (r RandIntEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"max":    r.max,
		"result": r.result,
	})
}

// UnmarshalJSON implements custom JSON unmarshaling
func (r *RandIntEvent) UnmarshalJSON(data []byte) error {
	var aux struct {
		Max    int64 `json:"max"`
		Result int64 `json:"result"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	r.max = aux.Max
	r.result = aux.Result
	return nil
}

// YieldEvent
// Use constructor: NewYieldEvent
// Accessors: SignalID(), RunID()
type YieldEvent struct {
	signalID string
	runID    string
}

func NewYieldEvent(signalID, runID string) YieldEvent {
	return YieldEvent{signalID: signalID, runID: runID}
}

func (y YieldEvent) EventType() EventType { return EventTypeYield }
func (y YieldEvent) SignalID() string     { return y.signalID }
func (y YieldEvent) RunID() string        { return y.runID }

// MarshalJSON implements custom JSON marshaling
func (y YieldEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"signalID": y.signalID,
		"runID":    y.runID,
	})
}

// UnmarshalJSON implements custom JSON unmarshaling
func (y *YieldEvent) UnmarshalJSON(data []byte) error {
	var aux struct {
		SignalID string `json:"signalID"`
		RunID    string `json:"runID"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	y.signalID = aux.SignalID
	y.runID = aux.RunID
	return nil
}

// ResumeEvent
// Use constructor: NewResumeEvent
// Accessors: SignalID(), Output()
type ResumeEvent struct {
	signalID string
	output   *anypb.Any
}

func NewResumeEvent(signalID string, output *anypb.Any) ResumeEvent {
	return ResumeEvent{signalID: signalID, output: output}
}

func (r ResumeEvent) EventType() EventType { return EventTypeResume }
func (r ResumeEvent) SignalID() string     { return r.signalID }
func (r ResumeEvent) Output() *anypb.Any   { return r.output }

// MarshalJSON implements custom JSON marshaling
func (r ResumeEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"signalID": r.signalID,
		"output":   r.output,
	})
}

// UnmarshalJSON implements custom JSON unmarshaling
func (r *ResumeEvent) UnmarshalJSON(data []byte) error {
	var aux struct {
		SignalID string     `json:"signalID"`
		Output   *anypb.Any `json:"output"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	r.signalID = aux.SignalID
	r.output = aux.Output
	return nil
}

// FinishEvent
// Use constructor: NewFinishEvent
// Accessor: Output()
type FinishEvent struct {
	output *anypb.Any
}

func NewFinishEvent(output *anypb.Any) FinishEvent {
	return FinishEvent{output: output}
}

func (f FinishEvent) EventType() EventType { return EventTypeFinish }
func (f FinishEvent) Output() *anypb.Any   { return f.output }

// MarshalJSON implements custom JSON marshaling
func (f FinishEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"output": f.output,
	})
}

// UnmarshalJSON implements custom JSON unmarshaling
func (f *FinishEvent) UnmarshalJSON(data []byte) error {
	var aux struct {
		Output *anypb.Any `json:"output"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	f.output = aux.Output
	return nil
}

// ClaimEvent
// Use constructor: NewClaimEvent
// Accessor: WorkerID()
type ClaimEvent struct {
	workerID string
}

func NewClaimEvent(workerID string) ClaimEvent {
	return ClaimEvent{workerID: workerID}
}

func (c ClaimEvent) EventType() EventType { return EventTypeClaim }
func (c ClaimEvent) WorkerID() string     { return c.workerID }

// MarshalJSON implements custom JSON marshaling
func (c ClaimEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"workerID": c.workerID,
	})
}

// UnmarshalJSON implements custom JSON unmarshaling
func (c *ClaimEvent) UnmarshalJSON(data []byte) error {
	var aux struct {
		WorkerID string `json:"workerID"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	c.workerID = aux.WorkerID
	return nil
}

// Event represents a single event in the execution history of a run.
type Event struct {
	Timestamp time.Time
	Type      EventType
	Metadata  EventMetadata
}
