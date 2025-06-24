package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/dynoinc/starflow"
	"github.com/lib/pq"
	"github.com/lithammer/shortuuid/v4"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// Store implements the starflow.Store interface using PostgreSQL
type Store struct {
	db *sql.DB
}

// New creates a new PostgreSQL store
func New(dsn string) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	store := &Store{db: db}
	if err := store.migrate(); err != nil {
		return nil, fmt.Errorf("failed to migrate database: %w", err)
	}

	return store, nil
}

// Close closes the database connection
func (s *Store) Close() error {
	return s.db.Close()
}

// migrate creates the necessary tables
func (s *Store) migrate() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS scripts (
			hash VARCHAR(64) PRIMARY KEY,
			content BYTEA NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS runs (
			id VARCHAR(255) PRIMARY KEY,
			script_hash VARCHAR(64) NOT NULL,
			input BYTEA,
			status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
			next_event_id BIGINT NOT NULL DEFAULT 0,
			worker_id VARCHAR(255),
			lease_until TIMESTAMP WITH TIME ZONE,
			output BYTEA,
			error_message TEXT,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS events (
			id BIGSERIAL PRIMARY KEY,
			run_id VARCHAR(255) NOT NULL,
			timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			type VARCHAR(20) NOT NULL,
			metadata JSONB NOT NULL,
			FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS yields (
			signal_id VARCHAR(255) PRIMARY KEY,
			run_id VARCHAR(255) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status)`,
		`CREATE INDEX IF NOT EXISTS idx_runs_worker_id ON runs(worker_id)`,
		`CREATE INDEX IF NOT EXISTS idx_events_run_id ON events(run_id)`,
		`CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp)`,
	}

	for _, query := range queries {
		if _, err := s.db.Exec(query); err != nil {
			return fmt.Errorf("failed to execute migration query: %w", err)
		}
	}

	return nil
}

// SaveScript persists the Starlark script content
func (s *Store) SaveScript(ctx context.Context, content []byte) (string, error) {
	hash := s.hashContent(content)

	query := `INSERT INTO scripts (hash, content) VALUES ($1, $2) 
			  ON CONFLICT (hash) DO NOTHING`

	_, err := s.db.ExecContext(ctx, query, hash, content)
	if err != nil {
		return "", fmt.Errorf("failed to save script: %w", err)
	}

	return hash, nil
}

// GetScript retrieves a script by its hash
func (s *Store) GetScript(ctx context.Context, scriptHash string) ([]byte, error) {
	var content []byte
	query := `SELECT content FROM scripts WHERE hash = $1`

	err := s.db.QueryRowContext(ctx, query, scriptHash).Scan(&content)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("script with hash %s not found", scriptHash)
		}
		return nil, fmt.Errorf("failed to get script: %w", err)
	}

	return content, nil
}

// CreateRun creates a new run record
func (s *Store) CreateRun(ctx context.Context, scriptHash string, input *anypb.Any) (string, error) {
	runID := s.generateID()

	var inputBytes []byte
	if input != nil {
		var err error
		inputBytes, err = proto.Marshal(input)
		if err != nil {
			return "", fmt.Errorf("failed to marshal input: %w", err)
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Check if script hash exists
	var exists bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM scripts WHERE hash = $1)`, scriptHash).Scan(&exists)
	if err != nil {
		return "", fmt.Errorf("failed to check script existence: %w", err)
	}
	if !exists {
		return "", fmt.Errorf("script with hash %s not found", scriptHash)
	}

	query := `INSERT INTO runs (id, script_hash, input, status, next_event_id) 
			  VALUES ($1, $2, $3, $4, $5)`

	_, err = tx.ExecContext(ctx, query, runID, scriptHash, inputBytes, starflow.RunStatusPending, 0)
	if err != nil {
		return "", fmt.Errorf("failed to create run: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return "", fmt.Errorf("failed to commit transaction: %w", err)
	}

	return runID, nil
}

// GetRun retrieves the details of a specific run
func (s *Store) GetRun(ctx context.Context, runID string) (*starflow.Run, error) {
	query := `SELECT id, script_hash, input, status, next_event_id, worker_id, 
			  lease_until, output, error_message, created_at, updated_at 
			  FROM runs WHERE id = $1`

	var run starflow.Run
	var inputBytes, outputBytes []byte
	var errorMsg sql.NullString
	var leaseUntil sql.NullTime
	var workerID sql.NullString

	err := s.db.QueryRowContext(ctx, query, runID).Scan(
		&run.ID, &run.ScriptHash, &inputBytes, &run.Status, &run.NextEventID,
		&workerID, &leaseUntil, &outputBytes, &errorMsg, &run.CreatedAt, &run.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("run with ID %s not found", runID)
		}
		return nil, fmt.Errorf("failed to get run: %w", err)
	}

	// Convert input bytes to anypb.Any
	if inputBytes != nil {
		run.Input = &anypb.Any{}
		if err := proto.Unmarshal(inputBytes, run.Input); err != nil {
			return nil, fmt.Errorf("failed to unmarshal input: %w", err)
		}
	}

	// Convert output bytes to anypb.Any
	if outputBytes != nil {
		run.Output = &anypb.Any{}
		if err := proto.Unmarshal(outputBytes, run.Output); err != nil {
			return nil, fmt.Errorf("failed to unmarshal output: %w", err)
		}
	}

	// Convert worker ID
	if workerID.Valid {
		run.WorkerID = workerID.String
	}

	// Convert lease time
	if leaseUntil.Valid {
		run.LeaseUntil = &leaseUntil.Time
	}

	// Convert error message
	if errorMsg.Valid {
		run.Error = errors.New(errorMsg.String)
	}

	return &run, nil
}

// ListRuns returns runs matching the specified statuses
func (s *Store) ListRuns(ctx context.Context, statuses ...starflow.RunStatus) ([]*starflow.Run, error) {
	var query string
	var args []interface{}

	if len(statuses) == 0 {
		query = `SELECT id, script_hash, input, status, next_event_id, worker_id, 
				 lease_until, output, error_message, created_at, updated_at FROM runs`
	} else {
		query = `SELECT id, script_hash, input, status, next_event_id, worker_id, 
				 lease_until, output, error_message, created_at, updated_at 
				 FROM runs WHERE status = ANY($1)`
		args = append(args, pq.Array(statuses))
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list runs: %w", err)
	}
	defer rows.Close()

	var runs []*starflow.Run
	for rows.Next() {
		var run starflow.Run
		var inputBytes, outputBytes []byte
		var errorMsg sql.NullString
		var leaseUntil sql.NullTime
		var workerID sql.NullString

		err := rows.Scan(
			&run.ID, &run.ScriptHash, &inputBytes, &run.Status, &run.NextEventID,
			&workerID, &leaseUntil, &outputBytes, &errorMsg, &run.CreatedAt, &run.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan run: %w", err)
		}

		// Convert input bytes to anypb.Any
		if inputBytes != nil {
			run.Input = &anypb.Any{}
			if err := proto.Unmarshal(inputBytes, run.Input); err != nil {
				return nil, fmt.Errorf("failed to unmarshal input: %w", err)
			}
		}

		// Convert output bytes to anypb.Any
		if outputBytes != nil {
			run.Output = &anypb.Any{}
			if err := proto.Unmarshal(outputBytes, run.Output); err != nil {
				return nil, fmt.Errorf("failed to unmarshal output: %w", err)
			}
		}

		// Convert worker ID
		if workerID.Valid {
			run.WorkerID = workerID.String
		}

		// Convert lease time
		if leaseUntil.Valid {
			run.LeaseUntil = &leaseUntil.Time
		}

		// Convert error message
		if errorMsg.Valid {
			run.Error = errors.New(errorMsg.String)
		}

		runs = append(runs, &run)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating runs: %w", err)
	}

	return runs, nil
}

// ClaimRun attempts to claim a run
func (s *Store) ClaimRun(ctx context.Context, runID string, workerID string, leaseUntil time.Time) (bool, error) {
	query := `UPDATE runs SET status = $1, worker_id = $2, lease_until = $3, updated_at = NOW()
			  WHERE id = $4 AND (worker_id IS NULL OR lease_until IS NULL OR lease_until < NOW())`

	result, err := s.db.ExecContext(ctx, query, starflow.RunStatusRunning, workerID, leaseUntil, runID)
	if err != nil {
		return false, fmt.Errorf("failed to claim run: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return rowsAffected > 0, nil
}

// RecordEvent records an event
func (s *Store) RecordEvent(ctx context.Context, runID string, nextEventID int64, eventMetadata starflow.EventMetadata) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Check optimistic concurrency
	var currentNextEventID int64
	err = tx.QueryRowContext(ctx, "SELECT next_event_id FROM runs WHERE id = $1", runID).Scan(&currentNextEventID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("run with ID %s not found", runID)
		}
		return 0, fmt.Errorf("failed to get current next event ID: %w", err)
	}

	if currentNextEventID != nextEventID {
		return 0, starflow.ErrConcurrentUpdate
	}

	// Serialize event metadata
	metadataBytes, err := json.Marshal(eventMetadata)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal event metadata: %w", err)
	}

	// Insert event
	eventQuery := `INSERT INTO events (run_id, type, metadata) VALUES ($1, $2, $3)`
	_, err = tx.ExecContext(ctx, eventQuery, runID, eventMetadata.EventType(), metadataBytes)
	if err != nil {
		return 0, fmt.Errorf("failed to insert event: %w", err)
	}

	// Update run based on event type
	switch eventMetadata.EventType() {
	case starflow.EventTypeReturn:
		if returnEvent, ok := eventMetadata.(starflow.ReturnEvent); ok && returnEvent.Error != nil {
			updateQuery := `UPDATE runs SET status = $1, error_message = $2, next_event_id = $3, updated_at = NOW() WHERE id = $4`
			_, err = tx.ExecContext(ctx, updateQuery, starflow.RunStatusFailed, returnEvent.Error.Error(), nextEventID+1, runID)
		} else {
			updateQuery := `UPDATE runs SET next_event_id = $1, updated_at = NOW() WHERE id = $2`
			_, err = tx.ExecContext(ctx, updateQuery, nextEventID+1, runID)
		}
	case starflow.EventTypeYield:
		if yieldEvent, ok := eventMetadata.(starflow.YieldEvent); ok {
			// Update run status
			updateQuery := `UPDATE runs SET status = $1, next_event_id = $2, updated_at = NOW() WHERE id = $3`
			_, err = tx.ExecContext(ctx, updateQuery, starflow.RunStatusYielded, nextEventID+1, runID)
			if err != nil {
				return 0, fmt.Errorf("failed to update run status: %w", err)
			}

			// Insert yield record
			yieldQuery := `INSERT INTO yields (signal_id, run_id) VALUES ($1, $2)`
			_, err = tx.ExecContext(ctx, yieldQuery, yieldEvent.SignalID, runID)
		} else {
			updateQuery := `UPDATE runs SET next_event_id = $1, updated_at = NOW() WHERE id = $2`
			_, err = tx.ExecContext(ctx, updateQuery, nextEventID+1, runID)
		}
	default:
		updateQuery := `UPDATE runs SET next_event_id = $1, updated_at = NOW() WHERE id = $2`
		_, err = tx.ExecContext(ctx, updateQuery, nextEventID+1, runID)
	}

	if err != nil {
		return 0, fmt.Errorf("failed to update run: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nextEventID + 1, nil
}

// Signal signals a run to resume
func (s *Store) Signal(ctx context.Context, cid string, output *anypb.Any) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Get run ID for the signal
	var runID string
	err = tx.QueryRowContext(ctx, "SELECT run_id FROM yields WHERE signal_id = $1", cid).Scan(&runID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("signal with ID %s not found", cid)
		}
		return fmt.Errorf("failed to get run ID for signal: %w", err)
	}

	// Insert resume event
	outputBytes, err := proto.Marshal(output)
	if err != nil {
		return fmt.Errorf("failed to marshal output: %w", err)
	}

	// Create a new anypb.Any from the marshaled bytes for the resume event
	resumeOutput := &anypb.Any{}
	if err := proto.Unmarshal(outputBytes, resumeOutput); err != nil {
		return fmt.Errorf("failed to unmarshal output for resume event: %w", err)
	}

	resumeEvent := starflow.ResumeEvent{SignalID: cid, Output: resumeOutput}
	metadataBytes, err := json.Marshal(resumeEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal resume event: %w", err)
	}

	eventQuery := `INSERT INTO events (run_id, type, metadata) VALUES ($1, $2, $3)`
	_, err = tx.ExecContext(ctx, eventQuery, runID, starflow.EventTypeResume, metadataBytes)
	if err != nil {
		return fmt.Errorf("failed to insert resume event: %w", err)
	}

	// Update run status and clear worker info
	updateQuery := `UPDATE runs SET status = $1, worker_id = NULL, lease_until = NULL, 
					next_event_id = next_event_id + 1, updated_at = NOW() WHERE id = $2`
	_, err = tx.ExecContext(ctx, updateQuery, starflow.RunStatusPending, runID)
	if err != nil {
		return fmt.Errorf("failed to update run: %w", err)
	}

	// Delete yield record
	deleteQuery := `DELETE FROM yields WHERE signal_id = $1`
	_, err = tx.ExecContext(ctx, deleteQuery, cid)
	if err != nil {
		return fmt.Errorf("failed to delete yield record: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetEvents retrieves all events for a specific run
func (s *Store) GetEvents(ctx context.Context, runID string) ([]*starflow.Event, error) {
	query := `SELECT timestamp, type, metadata FROM events WHERE run_id = $1 ORDER BY timestamp`

	rows, err := s.db.QueryContext(ctx, query, runID)
	if err != nil {
		return nil, fmt.Errorf("failed to get events: %w", err)
	}
	defer rows.Close()

	var events []*starflow.Event
	for rows.Next() {
		var event starflow.Event
		var metadataBytes []byte

		err := rows.Scan(&event.Timestamp, &event.Type, &metadataBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to scan event: %w", err)
		}

		// Deserialize metadata based on event type
		event.Metadata, err = s.deserializeEventMetadata(event.Type, metadataBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize event metadata: %w", err)
		}

		events = append(events, &event)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating events: %w", err)
	}

	return events, nil
}

// FinishRun updates the output of a run
func (s *Store) FinishRun(ctx context.Context, runID string, output *anypb.Any) error {
	outputBytes, err := proto.Marshal(output)
	if err != nil {
		return fmt.Errorf("failed to marshal output: %w", err)
	}

	query := `UPDATE runs SET status = $1, output = $2, updated_at = NOW() WHERE id = $3`

	result, err := s.db.ExecContext(ctx, query, starflow.RunStatusCompleted, outputBytes, runID)
	if err != nil {
		return fmt.Errorf("failed to finish run: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("run with ID %s not found", runID)
	}

	return nil
}

// Helper methods
func (s *Store) hashContent(content []byte) string {
	hash := sha256.Sum256(content)
	return hex.EncodeToString(hash[:])
}

func (s *Store) generateID() string {
	return shortuuid.New()
}

func (s *Store) deserializeEventMetadata(eventType starflow.EventType, metadataBytes []byte) (starflow.EventMetadata, error) {
	switch eventType {
	case starflow.EventTypeCall:
		var event starflow.CallEvent
		err := json.Unmarshal(metadataBytes, &event)
		return event, err
	case starflow.EventTypeReturn:
		var event starflow.ReturnEvent
		err := json.Unmarshal(metadataBytes, &event)
		return event, err
	case starflow.EventTypeSleep:
		var event starflow.SleepEvent
		err := json.Unmarshal(metadataBytes, &event)
		return event, err
	case starflow.EventTypeTimeNow:
		var event starflow.TimeNowEvent
		err := json.Unmarshal(metadataBytes, &event)
		return event, err
	case starflow.EventTypeRandInt:
		var event starflow.RandIntEvent
		err := json.Unmarshal(metadataBytes, &event)
		return event, err
	case starflow.EventTypeYield:
		var event starflow.YieldEvent
		err := json.Unmarshal(metadataBytes, &event)
		return event, err
	case starflow.EventTypeResume:
		var event starflow.ResumeEvent
		err := json.Unmarshal(metadataBytes, &event)
		return event, err
	default:
		return nil, fmt.Errorf("unknown event type: %s", eventType)
	}
}
