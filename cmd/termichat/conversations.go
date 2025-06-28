package main

import (
	"context"
	"database/sql"
	"time"
)

type ConversationsHistoryInput struct {
	Count int `json:"count"`
}

type Message struct {
	Role    string `json:"role"`
	Message string `json:"message"`
}

type ConversationsHistoryOutput struct {
	Messages []Message `json:"messages"`
}

func ConversationsStore(sqlite *sql.DB, conversationID string, role string, message string) error {
	_, err := sqlite.Exec("INSERT INTO conversations (conversation_id, timestamp, role, message) VALUES (?, ?, ?, ?)", conversationID, time.Now().Unix(), role, message)
	return err
}

func ConversationsHistory(sqlite *sql.DB, conversationID string) func(ctx context.Context, input ConversationsHistoryInput) (*ConversationsHistoryOutput, error) {
	return func(ctx context.Context, input ConversationsHistoryInput) (*ConversationsHistoryOutput, error) {
		rows, err := sqlite.Query("SELECT role, message FROM conversations WHERE conversation_id = ? ORDER BY timestamp DESC LIMIT ?", conversationID, input.Count)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		messages := []Message{}
		for rows.Next() {
			var role, message string
			err = rows.Scan(&role, &message)
			if err != nil {
				return nil, err
			}
			messages = append(messages, Message{Role: role, Message: message})
		}

		return &ConversationsHistoryOutput{Messages: messages}, nil
	}
}
