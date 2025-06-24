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
	// RunStatusPending indicates that the workflow has been created and is waiting to be picked up by a worker.
	RunStatusPending RunStatus = "PENDING"
)

// Run represents a single execution of a workflow.
type Run struct {
	// Fixed on creation
	ID         string
	ScriptHash string
	Input      []byte
	CreatedAt  time.Time

	// Updated at each event
	Status      RunStatus
	NextEventID int64
	UpdatedAt   time.Time

	// Set when finished.
	Output []byte
	Error  string

	// Set when claimed by a worker
	WorkerID   string
	LeaseUntil *time.Time
}

// EventType represents the type of an event in the execution history.
type EventType string

const (
	EventTypeCall   EventType = "CALL"
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
