package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "embed"

	"github.com/dynoinc/starflow"
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

	summary := ""
	switch meta := event.Metadata.(type) {
	case starflow.StartEvent:
		summary = fmt.Sprintf("scriptHash=%s", meta.ScriptHash())
	case starflow.CallEvent:
		summary = fmt.Sprintf("function=%s", meta.FunctionName())
	case starflow.ReturnEvent:
		_, err := meta.Output()
		if err != nil {
			summary = fmt.Sprintf("error=%v", err)
		}
	case starflow.YieldEvent:
		summary = fmt.Sprintf("signalID=%s", meta.SignalID())
	case starflow.ResumeEvent:
		summary = fmt.Sprintf("signalID=%s", meta.SignalID())
	case starflow.SleepEvent:
		summary = fmt.Sprintf("until=%s", meta.WakeupAt().Format(time.RFC3339))
	case starflow.TimeNowEvent:
		summary = fmt.Sprintf("timestamp=%s", meta.Timestamp().Format(time.RFC3339))
	case starflow.RandIntEvent:
		summary = fmt.Sprintf("result=%d", meta.Result())
	case starflow.FinishEvent:
		_, err := meta.Output()
		if err != nil {
			summary = fmt.Sprintf("error=%v", err)
		}
	}

	fmt.Printf("%s %-8s %s\n", event.Timestamp.Format("15:04:05.000"), event.Type(), summary)
	return s.Store.AppendEvent(ctx, runID, expectedVersion, eventData)
}

func main() {
	dbPath := filepath.Join(os.TempDir(), "termichat.sqlite")
	store, err := NewSQLiteStore(dbPath)
	if err != nil {
		panic(err)
	}
	wrappedStore := &eventPrintingStore{store}
	client := starflow.NewClient[Message, Response](wrappedStore)

	r := bufio.NewReader(os.Stdin)
	fmt.Println("Welcome to TermiChat! Type your message and press Enter. Type 'exit' or leave blank to quit.")
	for {
		fmt.Print("👤 ")
		msg, _ := r.ReadString('\n')
		msg = msg[:len(msg)-1]
		if msg == "" || msg == "exit" {
			fmt.Println("Goodbye!")
			break
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
