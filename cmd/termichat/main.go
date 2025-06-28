package main

import (
	"context"
	"encoding/json"
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

type eventPrintingStore struct {
	starflow.Store
}

func (s *eventPrintingStore) AppendEvent(ctx context.Context, runID string, expectedVersion int, eventData []byte) (int, error) {
	var event starflow.Event
	json.Unmarshal(eventData, &event)

	attrs := []any{}

	switch meta := event.Metadata.(type) {
	case starflow.StartEvent:
		attrs = append(attrs, "scriptHash", meta.ScriptHash())
	case starflow.CallEvent:
		attrs = append(attrs, "function", meta.FunctionName())
	case starflow.ReturnEvent:
		_, err := meta.Output()
		if err != nil {
			attrs = append(attrs, "error", err)
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
		_, err := meta.Output()
		if err != nil {
			attrs = append(attrs, "error", err)
		}
	}

	slog.Info(string(event.Type()), attrs...)
	return s.Store.AppendEvent(ctx, runID, expectedVersion, eventData)
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
	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		panic(err)
	}
	wrappedStore := &eventPrintingStore{store}
	client := starflow.NewClient[Message, Response](wrappedStore)
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
