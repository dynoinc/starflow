package main

import (
	"context"
	"database/sql"
	"time"
)

type MemoryRequest struct {
	Memory string `json:"memory"`
}

type MemoryResponse struct {
	Success bool `json:"success"`
}

type LoadMemoryRequest struct {
	Count int `json:"count"`
}

type LoadMemoryResponse struct {
	Memories []string `json:"memories"`
}

func MemoryStore(db *sql.DB) func(context.Context, MemoryRequest) (MemoryResponse, error) {
	return func(ctx context.Context, req MemoryRequest) (MemoryResponse, error) {
		_, err := db.ExecContext(ctx, `INSERT INTO user_memories(timestamp, memory) VALUES (?, ?)`, time.Now(), req.Memory)
		if err != nil {
			return MemoryResponse{Success: false}, err
		}
		return MemoryResponse{Success: true}, nil
	}
}

func MemoryLoad(db *sql.DB) func(context.Context, LoadMemoryRequest) (LoadMemoryResponse, error) {
	return func(ctx context.Context, req LoadMemoryRequest) (LoadMemoryResponse, error) {
		rows, err := db.QueryContext(ctx, `SELECT memory FROM user_memories ORDER BY timestamp DESC LIMIT ?`, req.Count)
		if err != nil {
			return LoadMemoryResponse{}, err
		}
		defer rows.Close()

		var memories []string
		for rows.Next() {
			var memory string
			if err := rows.Scan(&memory); err != nil {
				return LoadMemoryResponse{}, err
			}
			memories = append(memories, memory)
		}

		return LoadMemoryResponse{Memories: memories}, nil
	}
}
