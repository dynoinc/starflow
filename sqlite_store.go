package starflow

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

// SQLiteStore is a Store implementation that uses SQLite for persistence.
type SQLiteStore struct {
	db *sql.DB
}

// NewSQLiteStore creates a new SQLiteStore.
// The dsn is the data source name for the SQLite database.
func NewSQLiteStore(dsn string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	store := &SQLiteStore{db: db}
	if err := store.init(); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}
	return store, nil
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
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL,
		FOREIGN KEY(script_hash) REFERENCES scripts(hash)
	);
	CREATE TABLE IF NOT EXISTS events (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		run_id TEXT NOT NULL,
		timestamp DATETIME NOT NULL,
		type TEXT NOT NULL,
		function_name TEXT NOT NULL,
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
	runID := uuid.New().String()
	now := time.Now()

	_, err := s.db.Exec(
		"INSERT INTO runs (id, script_hash, input, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
		runID, scriptHash, input, RunStatusRunning, now, now,
	)
	if err != nil {
		return "", fmt.Errorf("failed to create run: %w", err)
	}

	return runID, nil
}

// GetRun retrieves the details of a specific run.
func (s *SQLiteStore) GetRun(runID string) (*Run, error) {
	var run Run
	var inputBytes, outputBytes []byte
	var status string
	err := s.db.QueryRow(
		"SELECT id, script_hash, status, input, output, created_at, updated_at FROM runs WHERE id = ?",
		runID,
	).Scan(&run.ID, &run.ScriptHash, &status, &inputBytes, &outputBytes, &run.CreatedAt, &run.UpdatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("run with ID %s not found", runID)
		}
		return nil, fmt.Errorf("failed to get run: %w", err)
	}
	run.Status = RunStatus(status)
	run.Input = inputBytes
	run.Output = outputBytes

	return &run, nil
}

// RecordEvent records an event in the execution history of a run.
func (s *SQLiteStore) RecordEvent(runID string, event *Event) error {
	_, err := s.db.Exec(
		"INSERT INTO events (run_id, timestamp, type, function_name, input, output, error) VALUES (?, ?, ?, ?, ?, ?, ?)",
		runID, event.Timestamp, event.Type, event.FunctionName, event.Input, event.Output, event.Error,
	)
	if err != nil {
		return fmt.Errorf("failed to record event: %w", err)
	}
	return nil
}

// GetEvents retrieves all events for a specific run.
func (s *SQLiteStore) GetEvents(runID string) ([]*Event, error) {
	rows, err := s.db.Query(
		"SELECT timestamp, type, function_name, input, output, error FROM events WHERE run_id = ? ORDER BY timestamp ASC",
		runID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	var events []*Event
	for rows.Next() {
		var event Event
		var inputBytes, outputBytes []byte
		var eventType string
		err := rows.Scan(&event.Timestamp, &eventType, &event.FunctionName, &inputBytes, &outputBytes, &event.Error)
		if err != nil {
			return nil, fmt.Errorf("failed to scan event: %w", err)
		}
		event.Type = EventType(eventType)
		event.Input = inputBytes
		event.Output = outputBytes

		events = append(events, &event)
	}

	return events, nil
}

func (s *SQLiteStore) UpdateRunStatus(ctx context.Context, runID string, status RunStatus) error {
	_, err := s.db.ExecContext(ctx, "UPDATE runs SET status = ?, updated_at = ? WHERE id = ?", status, time.Now(), runID)
	return err
}

func (s *SQLiteStore) UpdateRunOutput(ctx context.Context, runID string, output []byte) error {
	_, err := s.db.ExecContext(ctx, "UPDATE runs SET output = ?, updated_at = ? WHERE id = ?", output, time.Now(), runID)
	if err != nil {
		return fmt.Errorf("failed to update run output: %w", err)
	}
	return nil
}
