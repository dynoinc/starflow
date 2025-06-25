package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lithammer/shortuuid/v4"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/dynoinc/starflow"
)

// Store implements the starflow.Store interface using PostgreSQL
type Store struct {
	db *pgxpool.Pool
}

// New creates a new PostgreSQL store
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Test the connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	store := &Store{db: pool}
	if err := store.migrate(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to migrate database: %w", err)
	}

	return store, nil
}

// Close closes the database connection
func (s *Store) Close() error {
	s.db.Close()
	return nil
}

// migrate creates the necessary tables
func (s *Store) migrate(ctx context.Context) error {
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
		if _, err := s.db.Exec(ctx, query); err != nil {
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

	_, err := s.db.Exec(ctx, query, hash, content)
	if err != nil {
		return "", fmt.Errorf("failed to save script: %w", err)
	}

	return hash, nil
}

// GetScript retrieves a script by its hash
func (s *Store) GetScript(ctx context.Context, scriptHash string) ([]byte, error) {
	var content []byte
	query := `SELECT content FROM scripts WHERE hash = $1`

	err := s.db.QueryRow(ctx, query, scriptHash).Scan(&content)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
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

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Check if script hash exists
	var exists bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM scripts WHERE hash = $1)`, scriptHash).Scan(&exists)
	if err != nil {
		return "", fmt.Errorf("failed to check script existence: %w", err)
	}
	if !exists {
		return "", fmt.Errorf("script with hash %s not found", scriptHash)
	}

	query := `INSERT INTO runs (id, script_hash, input, status, next_event_id) 
			  VALUES ($1, $2, $3, $4, $5)`

	_, err = tx.Exec(ctx, query, runID, scriptHash, inputBytes, starflow.RunStatusPending, 0)
	if err != nil {
		return "", fmt.Errorf("failed to create run: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("failed to commit transaction: %w", err)
	}

	return runID, nil
}

// GetRun retrieves the details of a specific run
func (s *Store) GetRun(ctx context.Context, runID string) (*starflow.Run, error) {
	query := `SELECT id, script_hash, input, status, next_event_id, output, error_message, created_at, updated_at 
			  FROM runs WHERE id = $1`

	var run starflow.Run
	var inputBytes, outputBytes []byte
	var errorMsg pgtype.Text

	err := s.db.QueryRow(ctx, query, runID).Scan(
		&run.ID, &run.ScriptHash, &inputBytes, &run.Status, &run.NextEventID,
		&outputBytes, &errorMsg, &run.CreatedAt, &run.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
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

	// Convert error message
	if errorMsg.Valid {
		run.Error = errors.New(errorMsg.String)
	}

	return &run, nil
}

// ClaimableRuns retrieves runs that are either pending or haven't been updated recently.
func (s *Store) ClaimableRuns(ctx context.Context, staleThreshold time.Duration) ([]*starflow.Run, error) {
	staleTime := time.Now().Add(-staleThreshold)

	query := `SELECT id, script_hash, input, status, next_event_id, output, error_message, created_at, updated_at 
			  FROM runs WHERE status = $1 OR (status IN ($2, $3) AND updated_at < $4) ORDER BY created_at DESC`

	rows, err := s.db.Query(ctx, query,
		starflow.RunStatusPending,
		starflow.RunStatusRunning,
		starflow.RunStatusYielded,
		staleTime)
	if err != nil {
		return nil, fmt.Errorf("failed to query runs for claiming: %w", err)
	}
	defer rows.Close()

	var runs []*starflow.Run
	for rows.Next() {
		var run starflow.Run
		var inputBytes, outputBytes []byte
		var errorMsg pgtype.Text

		err := rows.Scan(
			&run.ID, &run.ScriptHash, &inputBytes, &run.Status, &run.NextEventID,
			&outputBytes, &errorMsg, &run.CreatedAt, &run.UpdatedAt,
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

// RecordEvent records an event. It succeeds only if run.NextEventID==expectedNextID.
func (s *Store) RecordEvent(ctx context.Context, runID string, nextEventID int64, eventMetadata starflow.EventMetadata) (int64, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Check optimistic concurrency
	var currentNextEventID int64
	err = tx.QueryRow(ctx, "SELECT next_event_id FROM runs WHERE id = $1", runID).Scan(&currentNextEventID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
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
	_, err = tx.Exec(ctx, eventQuery, runID, eventMetadata.EventType(), metadataBytes)
	if err != nil {
		return 0, fmt.Errorf("failed to insert event: %w", err)
	}

	// Update run based on event type
	switch eventMetadata.EventType() {
	case starflow.EventTypeReturn:
		if returnEvent, ok := eventMetadata.(starflow.ReturnEvent); ok {
			_, retErr := returnEvent.Output()
			if retErr != nil {
				updateQuery := `UPDATE runs SET status = $1, error_message = $2, next_event_id = $3, updated_at = NOW() WHERE id = $4`
				_, err = tx.Exec(ctx, updateQuery, starflow.RunStatusFailed, retErr.Error(), nextEventID+1, runID)
			}
		}
	case starflow.EventTypeYield:
		if yieldEvent, ok := eventMetadata.(starflow.YieldEvent); ok {
			// Update run status
			updateQuery := `UPDATE runs SET status = $1, next_event_id = $2, updated_at = NOW() WHERE id = $3`
			_, err = tx.Exec(ctx, updateQuery, starflow.RunStatusYielded, nextEventID+1, runID)
			if err != nil {
				return 0, fmt.Errorf("failed to update run status: %w", err)
			}

			// Insert yield record
			yieldQuery := `INSERT INTO yields (signal_id, run_id, created_at) VALUES ($1, $2, NOW())`
			_, err = tx.Exec(ctx, yieldQuery, yieldEvent.SignalID(), runID)
		}
	case starflow.EventTypeFinish:
		if finishEvent, ok := eventMetadata.(starflow.FinishEvent); ok {
			output := finishEvent.Output()
			if output != nil {
				outputBytes, marshalErr := proto.Marshal(output)
				if marshalErr != nil {
					return 0, fmt.Errorf("failed to marshal output: %w", marshalErr)
				}
				updateQuery := `UPDATE runs SET status = $1, output = $2, next_event_id = $3, updated_at = NOW() WHERE id = $4`
				_, err = tx.Exec(ctx, updateQuery, starflow.RunStatusCompleted, outputBytes, nextEventID+1, runID)
			}
		}
	case starflow.EventTypeClaim:
		updateQuery := `UPDATE runs SET status = $1, next_event_id = $2, updated_at = NOW() WHERE id = $3`
		_, err = tx.Exec(ctx, updateQuery, starflow.RunStatusRunning, nextEventID+1, runID)
	default:
		updateQuery := `UPDATE runs SET next_event_id = $1, updated_at = NOW() WHERE id = $2`
		_, err = tx.Exec(ctx, updateQuery, nextEventID+1, runID)
	}

	if err != nil {
		return 0, fmt.Errorf("failed to update run: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nextEventID + 1, nil
}

// Signal signals a run to resume
func (s *Store) Signal(ctx context.Context, runID, cid string, output *anypb.Any) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Only proceed if the signal exists
	var actualRunID string
	err = tx.QueryRow(ctx, "SELECT run_id FROM yields WHERE signal_id = $1", cid).Scan(&actualRunID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Invariant: Signaling with non-existent signal ID succeeds silently
			return nil
		}
		return fmt.Errorf("failed to get signal: %w", err)
	}

	// Now check if the run exists
	var currentNextEventID int64
	err = tx.QueryRow(ctx, "SELECT next_event_id FROM runs WHERE id = $1", actualRunID).Scan(&currentNextEventID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Invariant: Signaling with non-existent run ID succeeds silently
			return nil
		}
		return fmt.Errorf("failed to get current next event ID: %w", err)
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

	resumeEvent := starflow.NewResumeEvent(cid, resumeOutput)
	metadataBytes, err := json.Marshal(resumeEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal resume event: %w", err)
	}

	eventQuery := `INSERT INTO events (run_id, type, metadata) VALUES ($1, $2, $3)`
	_, err = tx.Exec(ctx, eventQuery, actualRunID, starflow.EventTypeResume, metadataBytes)
	if err != nil {
		return fmt.Errorf("failed to insert resume event: %w", err)
	}

	// Update run status with the correct next_event_id
	updateQuery := `UPDATE runs SET status = $1, next_event_id = $2, updated_at = NOW() WHERE id = $3`
	_, err = tx.Exec(ctx, updateQuery, starflow.RunStatusPending, currentNextEventID+1, actualRunID)
	if err != nil {
		return fmt.Errorf("failed to update run: %w", err)
	}

	// Delete yield record
	deleteQuery := `DELETE FROM yields WHERE signal_id = $1`
	_, err = tx.Exec(ctx, deleteQuery, cid)
	if err != nil {
		return fmt.Errorf("failed to delete yield record: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetEvents retrieves all events for a specific run
func (s *Store) GetEvents(ctx context.Context, runID string) ([]*starflow.Event, error) {
	query := `SELECT timestamp, type, metadata FROM events WHERE run_id = $1 ORDER BY timestamp`

	rows, err := s.db.Query(ctx, query, runID)
	if err != nil {
		return nil, fmt.Errorf("failed to get events: %w", err)
	}
	defer rows.Close()

	var events []*starflow.Event
	for rows.Next() {
		var event starflow.Event
		var eventType starflow.EventType
		var metadataBytes []byte

		err := rows.Scan(&event.Timestamp, &eventType, &metadataBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to scan event: %w", err)
		}

		// Deserialize metadata based on event type
		event.Metadata, err = s.deserializeEventMetadata(eventType, metadataBytes)
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
	case starflow.EventTypeFinish:
		var event starflow.FinishEvent
		err := json.Unmarshal(metadataBytes, &event)
		return event, err
	case starflow.EventTypeClaim:
		var event starflow.ClaimEvent
		err := json.Unmarshal(metadataBytes, &event)
		return event, err
	default:
		return nil, fmt.Errorf("unknown event type: %s", eventType)
	}
}
