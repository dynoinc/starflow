package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dynoinc/starflow"
)

// SQLiteStore implements starflow.Store using SQLite.
type SQLiteStore struct {
	db *sql.DB
	mu sync.Mutex
}

func NewSQLiteStore(db *sql.DB) starflow.Store {
	return &eventPrintingStore{&SQLiteStore{db: db}}
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

type eventPrintingStore struct {
	starflow.Store
}

func (s *eventPrintingStore) AppendEvent(ctx context.Context, runID string, expectedVersion int, eventData []byte) (int, error) {
	var event starflow.Event
	json.Unmarshal(eventData, &event)

	attrs := []any{}
	body := ""

	switch meta := event.Metadata.(type) {
	case starflow.StartEvent:
		attrs = append(attrs, "scriptHash", meta.ScriptHash())
	case starflow.CallEvent:
		attrs = append(attrs, "function", meta.FunctionName())
		if inputJSON, err := json.MarshalIndent(meta.Input(), "", "  "); err == nil {
			body = string(inputJSON)
		} else {
			attrs = append(attrs, "input", meta.Input())
		}
	case starflow.ReturnEvent:
		output, err := meta.Output()
		if err != nil {
			attrs = append(attrs, "error", err)
		} else {
			if outputJSON, marshalErr := json.MarshalIndent(output, "", "  "); marshalErr == nil {
				body = string(outputJSON)
			} else {
				attrs = append(attrs, "output", output)
			}
		}
	case starflow.YieldEvent:
		attrs = append(attrs, "signalID", meta.SignalID())
	case starflow.ResumeEvent:
		attrs = append(attrs, "signalID", meta.SignalID())
	case starflow.SleepEvent:
		attrs = append(attrs, "until", meta.WakeupAt().Format(time.RFC3339))
	case starflow.TimeNowEvent:
		attrs = append(attrs, "timestamp", meta.Timestamp().Format(time.RFC3339))
	case starflow.RandIntEvent:
		attrs = append(attrs, "result", meta.Result())
	case starflow.FinishEvent:
		output, err := meta.Output()
		if err != nil {
			attrs = append(attrs, "error", err)
		} else {
			if outputJSON, marshalErr := json.MarshalIndent(output, "", "  "); marshalErr == nil {
				body = string(outputJSON)
			} else {
				attrs = append(attrs, "output", output)
			}
		}
	}

	slog.Info(string(event.Type()), attrs...)
	if body != "" {
		fmt.Println(body)
	}

	return s.Store.AppendEvent(ctx, runID, expectedVersion, eventData)
}
