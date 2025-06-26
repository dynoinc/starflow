package events

import (
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
)

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

type EventMetadata interface {
	EventType() EventType
}

// CallEvent metadata
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

func (c CallEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"functionName": c.functionName,
		"input":        c.input,
	})
}

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
type ReturnEvent struct {
	output *anypb.Any
	err    error
}

func NewReturnEvent(output *anypb.Any, err error) ReturnEvent {
	return ReturnEvent{output: output, err: err}
}

func (r ReturnEvent) EventType() EventType        { return EventTypeReturn }
func (r ReturnEvent) Output() (*anypb.Any, error) { return r.output, r.err }

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
type SleepEvent struct {
	wakeupAt time.Time
}

func NewSleepEvent(wakeupAt time.Time) SleepEvent {
	return SleepEvent{wakeupAt: wakeupAt}
}

func (s SleepEvent) EventType() EventType { return EventTypeSleep }
func (s SleepEvent) WakeupAt() time.Time  { return s.wakeupAt }

func (s SleepEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"wakeupAt": s.wakeupAt,
	})
}

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
type TimeNowEvent struct {
	timestamp time.Time
}

func NewTimeNowEvent(timestamp time.Time) TimeNowEvent {
	return TimeNowEvent{timestamp: timestamp}
}

func (t TimeNowEvent) EventType() EventType { return EventTypeTimeNow }
func (t TimeNowEvent) Timestamp() time.Time { return t.timestamp }

func (t TimeNowEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"timestamp": t.timestamp,
	})
}

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
type RandIntEvent struct {
	result int64
}

func NewRandIntEvent(result int64) RandIntEvent {
	return RandIntEvent{result: result}
}

func (r RandIntEvent) EventType() EventType { return EventTypeRandInt }
func (r RandIntEvent) Result() int64        { return r.result }

func (r RandIntEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"result": r.result,
	})
}

func (r *RandIntEvent) UnmarshalJSON(data []byte) error {
	var aux struct {
		Result int64 `json:"result"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	r.result = aux.Result
	return nil
}

// YieldEvent
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

func (y YieldEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"signalID": y.signalID,
		"runID":    y.runID,
	})
}

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

func (r ResumeEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"signalID": r.signalID,
		"output":   r.output,
	})
}

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
type FinishEvent struct {
	output *anypb.Any
	err    error
}

func NewFinishEvent(output *anypb.Any, err error) FinishEvent {
	return FinishEvent{output: output, err: err}
}

func (f FinishEvent) EventType() EventType        { return EventTypeFinish }
func (f FinishEvent) Output() (*anypb.Any, error) { return f.output, f.err }

func (f FinishEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"output": f.output,
		"error":  f.err,
	})
}

func (f *FinishEvent) UnmarshalJSON(data []byte) error {
	var aux struct {
		Output *anypb.Any `json:"output"`
		Error  string     `json:"error"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	f.output = aux.Output
	if aux.Error != "" {
		f.err = fmt.Errorf("%s", aux.Error)
	}
	return nil
}

// ClaimEvent
type ClaimEvent struct {
	workerID string
	until    time.Time
}

func NewClaimEvent(workerID string, until time.Time) ClaimEvent {
	return ClaimEvent{workerID: workerID, until: until}
}

func (c ClaimEvent) EventType() EventType { return EventTypeClaim }
func (c ClaimEvent) WorkerID() string     { return c.workerID }
func (c ClaimEvent) Until() time.Time     { return c.until }

func (c ClaimEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"workerID": c.workerID,
		"until":    c.until,
	})
}

func (c *ClaimEvent) UnmarshalJSON(data []byte) error {
	var aux struct {
		WorkerID string    `json:"workerID"`
		Until    time.Time `json:"until"`
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
	Metadata  EventMetadata
}

func (e Event) Type() EventType {
	return e.Metadata.EventType()
}
