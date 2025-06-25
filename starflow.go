package starflow

import (
	"fmt"
	"time"

	"github.com/lithammer/shortuuid/v4"
	"google.golang.org/protobuf/types/known/anypb"
)

type YieldError struct {
	cid string
}

func (e *YieldError) Error() string {
	return fmt.Sprintf("yield error: %s", e.cid)
}

func NewYieldError() (string, error) {
	cid := shortuuid.New()
	return cid, &YieldError{cid: cid}
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
type CallEvent struct {
	FunctionName string
	Input        *anypb.Any
}

func (c CallEvent) EventType() EventType { return EventTypeCall }

// ReturnEvent metadata
type ReturnEvent struct {
	Output *anypb.Any
	Error  error
}

func (r ReturnEvent) EventType() EventType { return EventTypeReturn }

type SleepEvent struct {
	WakeupAt time.Time
}

func (s SleepEvent) EventType() EventType { return EventTypeSleep }

type TimeNowEvent struct {
	Timestamp time.Time
}

func (t TimeNowEvent) EventType() EventType { return EventTypeTimeNow }

type RandIntEvent struct {
	Max    int64
	Result int64
}

func (r RandIntEvent) EventType() EventType { return EventTypeRandInt }

type YieldEvent struct {
	SignalID string
}

func (y YieldEvent) EventType() EventType { return EventTypeYield }

type ResumeEvent struct {
	SignalID string
	Output   *anypb.Any
}

func (r ResumeEvent) EventType() EventType { return EventTypeResume }

type FinishEvent struct {
	Output *anypb.Any
}

func (f FinishEvent) EventType() EventType { return EventTypeFinish }

type ClaimEvent struct {
	WorkerID string
}

func (c ClaimEvent) EventType() EventType { return EventTypeClaim }

// Event represents a single event in the execution history of a run.
type Event struct {
	Timestamp time.Time
	Type      EventType
	Metadata  EventMetadata
}

// Helper methods for type-safe access
func (e Event) AsCallEvent() (CallEvent, bool) {
	callEvent, ok := e.Metadata.(CallEvent)
	return callEvent, ok
}

func (e Event) AsReturnEvent() (ReturnEvent, bool) {
	returnEvent, ok := e.Metadata.(ReturnEvent)
	return returnEvent, ok
}

func (e Event) AsSleepEvent() (SleepEvent, bool) {
	returnEvent, ok := e.Metadata.(SleepEvent)
	return returnEvent, ok
}

func (e Event) AsTimeNowEvent() (TimeNowEvent, bool) {
	timeNowEvent, ok := e.Metadata.(TimeNowEvent)
	return timeNowEvent, ok
}

func (e Event) AsRandIntEvent() (RandIntEvent, bool) {
	randIntEvent, ok := e.Metadata.(RandIntEvent)
	return randIntEvent, ok
}

func (e Event) AsYieldEvent() (YieldEvent, bool) {
	yieldEvent, ok := e.Metadata.(YieldEvent)
	return yieldEvent, ok
}

func (e Event) AsResumeEvent() (ResumeEvent, bool) {
	resumeEvent, ok := e.Metadata.(ResumeEvent)
	return resumeEvent, ok
}

func (e Event) AsFinishEvent() (FinishEvent, bool) {
	finishEvent, ok := e.Metadata.(FinishEvent)
	return finishEvent, ok
}

func (e Event) AsClaimEvent() (ClaimEvent, bool) {
	claimEvent, ok := e.Metadata.(ClaimEvent)
	return claimEvent, ok
}
