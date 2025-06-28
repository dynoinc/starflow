package main

import (
	"database/sql"

	_ "modernc.org/sqlite"
)

func NewSQLiteDB(dsn string) (*sql.DB, error) {
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
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS user_memories (
		timestamp DATETIME NOT NULL,
		memory TEXT NOT NULL
	)`); err != nil {
		return nil, err
	}
	return db, nil
}
