package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	_ "embed"

	"github.com/chzyer/readline"
	"github.com/dynoinc/starflow"
	"github.com/joho/godotenv"
	"github.com/lmittmann/tint"
)

//go:embed assistant.star
var assistantScript []byte

type Message struct {
	Message string `json:"message"`
}

type Response struct {
	Response string `json:"response"`
}

func main() {
	_ = godotenv.Load()

	logHandler := tint.NewHandler(os.Stdout, &tint.Options{
		Level:      slog.LevelInfo,
		TimeFormat: time.StampMilli,
	})
	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	dbPath := filepath.Join(os.TempDir(), "termichat.sqlite")
	sqlite, err := NewSQLiteDB(dbPath)
	if err != nil {
		panic(err)
	}

	client := starflow.NewClient[Message, Response](NewSQLiteStore(sqlite))
	starflow.RegisterFunc(client, OpenAIComplete, starflow.WithName("openai.complete"))

	historyFile := filepath.Join(os.TempDir(), "termichat_history.txt")
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "👤 ",
		HistoryFile:     historyFile,
		InterruptPrompt: "^C",
		EOFPrompt:       "Goodbye!\n",
	})
	if err != nil {
		panic(err)
	}
	defer rl.Close()

	fmt.Println("Welcome to TermiChat! Type your message and press Enter. Press Ctrl+C to quit.")
	for {
		msg, err := rl.Readline()
		if err == readline.ErrInterrupt {
			if len(msg) == 0 {
				fmt.Println("Goodbye!")
				break
			}
			continue
		} else if err == io.EOF {
			break
		}

		if msg == "" {
			continue
		}

		input := Message{Message: msg}
		runID := fmt.Sprintf("chat-%d", time.Now().UnixNano())
		fmt.Println("")
		resp, err := client.Run(context.Background(), runID, assistantScript, input)
		if err != nil {
			fmt.Println("[error]", err)
			continue
		}

		fmt.Printf("🤖 %s\n\n", resp.Response)
	}
}
