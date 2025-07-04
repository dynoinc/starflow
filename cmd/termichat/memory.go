package main

import (
	"context"
	"database/sql"
	"time"
)

type StoreMemoryRequest struct {
	Memory string `json:"memory"`
}

type StoreMemoryResponse struct {
	Success bool `json:"success"`
}

type RestoreMemoryRequest struct {
	Count int `json:"count"`
}

type RestoreMemoryResponse struct {
	Memories []string `json:"memories"`
}

func MemoryStore(db *sql.DB) func(context.Context, StoreMemoryRequest) (StoreMemoryResponse, error) {
	return func(ctx context.Context, req StoreMemoryRequest) (StoreMemoryResponse, error) {
		_, err := db.ExecContext(ctx, `INSERT INTO user_memories(timestamp, memory) VALUES (?, ?)`, time.Now(), req.Memory)
		if err != nil {
			return StoreMemoryResponse{Success: false}, err
		}
		return StoreMemoryResponse{Success: true}, nil
	}
}

func MemoryRestore(db *sql.DB) func(context.Context, RestoreMemoryRequest) (RestoreMemoryResponse, error) {
	return func(ctx context.Context, req RestoreMemoryRequest) (RestoreMemoryResponse, error) {
		rows, err := db.QueryContext(ctx, `SELECT memory FROM user_memories ORDER BY timestamp DESC LIMIT ?`, req.Count)
		if err != nil {
			return RestoreMemoryResponse{}, err
		}
		defer rows.Close()

		var memories []string
		for rows.Next() {
			var memory string
			if err := rows.Scan(&memory); err != nil {
				return RestoreMemoryResponse{}, err
			}
			memories = append(memories, memory)
		}

		return RestoreMemoryResponse{Memories: memories}, nil
	}
}
