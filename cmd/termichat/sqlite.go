package main

import (
	"context"
	"database/sql"
	"sync"

	_ "modernc.org/sqlite"

	"github.com/dynoinc/starflow"
)

// SQLiteStore implements starflow.Store using SQLite.
type SQLiteStore struct {
	db *sql.DB
	mu sync.Mutex
}

func NewSQLiteStore(dsn string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS events (
		run_id TEXT NOT NULL,
		idx INTEGER NOT NULL,
		event_data BLOB NOT NULL,
		PRIMARY KEY(run_id, idx)
	)`); err != nil {
		return nil, err
	}
	return &SQLiteStore{db: db}, nil
}

func (s *SQLiteStore) AppendEvent(ctx context.Context, runID string, expectedVersion int, eventData []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id = ?`, runID).Scan(&count)
	if err != nil {
		return 0, err
	}
	if count != expectedVersion {
		return 0, starflow.ErrConcurrentUpdate
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO events(run_id, idx, event_data) VALUES (?, ?, ?)`, runID, count, eventData)
	if err != nil {
		return 0, err
	}
	return count + 1, nil
}

func (s *SQLiteStore) GetEvents(ctx context.Context, runID string) ([][]byte, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT event_data FROM events WHERE run_id = ? ORDER BY idx ASC`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events [][]byte
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		events = append(events, data)
	}
	return events, nil
}

func (s *SQLiteStore) GetLastEvent(ctx context.Context, runID string) ([]byte, int, error) {
	row := s.db.QueryRowContext(ctx, `SELECT event_data, idx FROM events WHERE run_id = ? ORDER BY idx DESC LIMIT 1`, runID)
	var data []byte
	var idx int
	err := row.Scan(&data, &idx)
	if err == sql.ErrNoRows {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	return data, idx + 1, nil
}
