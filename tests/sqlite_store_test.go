package starflow_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dynoinc/starflow"
	"github.com/lithammer/shortuuid/v4"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// SQLiteStore is a Store implementation that uses SQLite for persistence.
type SQLiteStore struct {
	db *sql.DB
}

// NewSQLiteStore creates a new SQLiteStore.
// The dsn is the data source name for the SQLite database.
func NewSQLiteStore(t *testing.T) *SQLiteStore {
	dsn := t.TempDir() + "/starflow.db"
	db, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	require.NoError(t, db.Ping())

	store := &SQLiteStore{db: db}
	require.NoError(t, store.init())
	return store
}

// init creates the necessary tables in the database if they don't exist.
func (s *SQLiteStore) init() error {
	ddl := `
	CREATE TABLE IF NOT EXISTS scripts (
		hash TEXT PRIMARY KEY,
		content BLOB NOT NULL
	);
	CREATE TABLE IF NOT EXISTS runs (
		id TEXT PRIMARY KEY,
		script_hash TEXT NOT NULL,
		input BLOB,
		status TEXT NOT NULL,
		output BLOB,
		error TEXT,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL,
		wake_at DATETIME,
		FOREIGN KEY(script_hash) REFERENCES scripts(hash)
	);
	CREATE TABLE IF NOT EXISTS events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		run_id TEXT NOT NULL,
		timestamp DATETIME NOT NULL,
		type TEXT NOT NULL,
		function_name TEXT NOT NULL,
		correlation_id TEXT,
		input BLOB,
		output BLOB,
		error TEXT,
		FOREIGN KEY(run_id) REFERENCES runs(id)
	);
	`
	_, err := s.db.Exec(ddl)
	return err
}

// SaveScript persists the Starlark script content.
func (s *SQLiteStore) SaveScript(content []byte) (string, error) {
	hash := sha256.Sum256(content)
	hashStr := hex.EncodeToString(hash[:])

	// Use INSERT OR IGNORE to avoid errors if the script already exists.
	_, err := s.db.Exec("INSERT OR IGNORE INTO scripts (hash, content) VALUES (?, ?)", hashStr, content)
	if err != nil {
		return "", fmt.Errorf("failed to save script: %w", err)
	}

	return hashStr, nil
}

// GetScript retrieves a script by its sha256 hash.
func (s *SQLiteStore) GetScript(scriptHash string) ([]byte, error) {
	var content []byte
	err := s.db.QueryRow("SELECT content FROM scripts WHERE hash = ?", scriptHash).Scan(&content)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("script with hash %s not found", scriptHash)
		}
		return nil, fmt.Errorf("failed to get script: %w", err)
	}
	return content, nil
}

// CreateRun creates a new run record for a given script.
func (s *SQLiteStore) CreateRun(scriptHash string, input []byte) (string, error) {
	runID := shortuuid.New()
	now := time.Now()

	_, err := s.db.Exec(
		"INSERT INTO runs (id, script_hash, input, status, output, error, created_at, updated_at, wake_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ",
		runID, scriptHash, input, starflow.RunStatusPending, nil, nil, now, now, nil,
	)
	if err != nil {
		return "", fmt.Errorf("failed to create run: %w", err)
	}

	return runID, nil
}

// GetRun retrieves the details of a specific run.
func (s *SQLiteStore) GetRun(runID string) (*starflow.Run, error) {
	var run starflow.Run
	var inputBytes, outputBytes []byte
	var status string
	var errStr sql.NullString
	var wakeAt sql.NullTime
	err := s.db.QueryRow(
		"SELECT id, script_hash, status, error, input, output, created_at, updated_at, wake_at FROM runs WHERE id = ?",
		runID,
	).Scan(&run.ID, &run.ScriptHash, &status, &errStr, &inputBytes, &outputBytes, &run.CreatedAt, &run.UpdatedAt, &wakeAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("run with ID %s not found", runID)
		}
		return nil, fmt.Errorf("failed to get run: %w", err)
	}
	run.Status = starflow.RunStatus(status)
	if errStr.Valid {
		run.Error = errStr.String
	}
	run.Input = inputBytes
	run.Output = outputBytes
	if wakeAt.Valid {
		run.WakeAt = &wakeAt.Time
	}

	return &run, nil
}

// RecordEvent records an event in the execution history of a run.
func (s *SQLiteStore) RecordEvent(runID string, event *starflow.Event) error {
	_, err := s.db.Exec(
		"INSERT INTO events (run_id, timestamp, type, function_name, correlation_id, input, output, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		runID, event.Timestamp, event.Type, event.FunctionName, event.CorrelationID, event.Input, event.Output, event.Error,
	)
	if err != nil {
		return fmt.Errorf("failed to record event: %w", err)
	}
	return nil
}

// GetEvents retrieves all events for a specific run.
func (s *SQLiteStore) GetEvents(runID string) ([]*starflow.Event, error) {
	rows, err := s.db.Query(
		"SELECT timestamp, type, function_name, correlation_id, input, output, error FROM events WHERE run_id = ? ORDER BY timestamp ASC",
		runID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	var events []*starflow.Event
	for rows.Next() {
		var event starflow.Event
		var inputBytes, outputBytes []byte
		var eventType string
		err := rows.Scan(&event.Timestamp, &eventType, &event.FunctionName, &event.CorrelationID, &inputBytes, &outputBytes, &event.Error)
		if err != nil {
			return nil, fmt.Errorf("failed to scan event: %w", err)
		}
		event.Type = starflow.EventType(eventType)
		event.Input = inputBytes
		event.Output = outputBytes

		events = append(events, &event)
	}

	return events, nil
}

func (s *SQLiteStore) UpdateRunOutput(ctx context.Context, runID string, output []byte) error {
	_, err := s.db.ExecContext(ctx, "UPDATE runs SET output = ?, status = ?, updated_at = ? WHERE id = ?", output, starflow.RunStatusCompleted, time.Now(), runID)
	if err != nil {
		return fmt.Errorf("failed to update run output: %w", err)
	}
	return nil
}

// ListRuns returns all runs whose status matches any of the supplied statuses.
func (s *SQLiteStore) ListRuns(ctx context.Context, statuses ...starflow.RunStatus) ([]*starflow.Run, error) {
	if len(statuses) == 0 {
		return nil, nil
	}

	// Build placeholders
	placeholders := make([]string, len(statuses))
	args := make([]interface{}, len(statuses))
	for i, st := range statuses {
		placeholders[i] = "?"
		args[i] = st
	}

	query := fmt.Sprintf(`SELECT id, script_hash, status, error, input, output, created_at, updated_at, wake_at
		FROM runs WHERE status IN (%s)`, strings.Join(placeholders, ","))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query runs: %w", err)
	}
	defer rows.Close()

	var runs []*starflow.Run
	for rows.Next() {
		var run starflow.Run
		var status string
		var errStr sql.NullString
		var wakeAt sql.NullTime
		if err := rows.Scan(&run.ID, &run.ScriptHash, &status, &errStr, &run.Input, &run.Output, &run.CreatedAt, &run.UpdatedAt, &wakeAt); err != nil {
			return nil, err
		}
		run.Status = starflow.RunStatus(status)
		if errStr.Valid {
			run.Error = errStr.String
		}
		if wakeAt.Valid {
			run.WakeAt = &wakeAt.Time
		}
		runs = append(runs, &run)
	}

	return runs, nil
}

// FindEventByCorrelationID retrieves event and runID for correlation id.
func (s *SQLiteStore) FindEventByCorrelationID(cid string) (string, *starflow.Event, error) {
	row := s.db.QueryRow(`SELECT run_id, timestamp, type, function_name, input, output, error FROM events WHERE correlation_id = ? LIMIT 1`, cid)
	var runID string
	var e starflow.Event
	var eventType string
	if err := row.Scan(&runID, &e.Timestamp, &eventType, &e.FunctionName, &e.Input, &e.Output, &e.Error); err != nil {
		if err == sql.ErrNoRows {
			return "", nil, fmt.Errorf("correlation id %s not found", cid)
		}
		return "", nil, err
	}
	e.Type = starflow.EventType(eventType)
	e.CorrelationID = cid
	return runID, &e, nil
}

// UpdateRunStatusAndRecordEvent executes event insert and status/wake_at update atomically.
func (s *SQLiteStore) UpdateRunStatusAndRecordEvent(ctx context.Context, runID string, status starflow.RunStatus, event *starflow.Event, wakeAt *time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			_ = tx.Commit()
		}
	}()

	now := time.Now()

	if event != nil {
		if _, err = tx.ExecContext(ctx, `INSERT INTO events (run_id, timestamp, type, function_name, correlation_id, input, output, error) VALUES (?,?,?,?,?,?,?,?)`,
			runID, event.Timestamp, event.Type, event.FunctionName, event.CorrelationID, event.Input, event.Output, event.Error); err != nil {
			return err
		}
	}

	setParts := []string{"updated_at = ?"}
	args := []interface{}{now}

	if status != "" {
		setParts = append(setParts, "status = ?")
		args = append(args, status)
	}

	if wakeAt != nil {
		setParts = append(setParts, "wake_at = ?")
		args = append(args, wakeAt)
	}

	args = append(args, runID)

	query := fmt.Sprintf("UPDATE runs SET %s WHERE id = ?", strings.Join(setParts, ", "))
	if _, err = tx.ExecContext(ctx, query, args...); err != nil {
		return err
	}

	return nil
}

// UpdateRunError sets the error message for a run.
func (s *SQLiteStore) UpdateRunError(ctx context.Context, runID string, errMsg string) error {
	_, err := s.db.ExecContext(ctx, "UPDATE runs SET error = ?, status = ?, updated_at = ? WHERE id = ?", errMsg, starflow.RunStatusFailed, time.Now(), runID)
	return err
}
