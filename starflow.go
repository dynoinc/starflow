package starflow

import (
	"time"
)

// RunStatus represents the status of a workflow run.
type RunStatus string

const (
	// RunStatusRunning indicates that the workflow is currently running.
	RunStatusRunning RunStatus = "RUNNING"
	// RunStatusCompleted indicates that the workflow has completed successfully.
	RunStatusCompleted RunStatus = "COMPLETED"
	// RunStatusFailed indicates that the workflow has failed.
	RunStatusFailed RunStatus = "FAILED"
)

// Run represents a single execution of a workflow.
type Run struct {
	ID         string
	ScriptHash string
	Status     RunStatus
	Input      []byte
	Output     []byte
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// EventType represents the type of an event in the execution history.
type EventType string

const (
	// EventTypeCall indicates a call to a Go function from Starlark.
	EventTypeCall EventType = "CALL"
	// EventTypeReturn indicates a return from a Go function.
	EventTypeReturn EventType = "RETURN"
)

// Event represents a single event in the execution history of a run.
type Event struct {
	Timestamp    time.Time
	Type         EventType
	FunctionName string
	Input        []byte
	Output       []byte
	Error        string
}
