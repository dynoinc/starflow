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
	"github.com/lithammer/shortuuid"
	"github.com/lmittmann/tint"
	"github.com/openai/openai-go"
)

//go:embed assistant.star
var assistantScript []byte

func main() {
	_ = godotenv.Load()
	ctx := context.Background()
	conversationID := shortuuid.New()

	if err := starflow.ValidateScript(assistantScript); err != nil {
		panic(err)
	}

	// Logging
	logHandler := tint.NewHandler(os.Stdout, &tint.Options{
		Level:      slog.LevelInfo,
		TimeFormat: time.StampMilli,
	})
	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	// Database (for conversations, memories and events)
	dbPath := filepath.Join(os.TempDir(), "termichat.sqlite")
	sqlite, err := NewSQLiteDB(dbPath)
	if err != nil {
		panic(err)
	}

	// Starflow
	client := starflow.NewClient[string, string](NewSQLiteStore(sqlite))

	// OpenAI client
	openaiClient := openai.NewClient()
	starflow.RegisterFunc(client, func(ctx context.Context, req openai.ChatCompletionNewParams) (*openai.ChatCompletion, error) {
		return openaiClient.Chat.Completions.New(ctx, req)
	}, starflow.WithName("openai.complete"))

	// Conversations
	starflow.RegisterFunc(client, ConversationsHistory(sqlite, conversationID), starflow.WithName("conversations.history"))

	// Memories
	starflow.RegisterFunc(client, MemoryStore(sqlite), starflow.WithName("memory.store"))
	starflow.RegisterFunc(client, MemoryRestore(sqlite), starflow.WithName("memory.restore"))

	// MCP
	clients, err := Start(ctx, os.Getenv("MCP_SERVERS"))
	if err != nil {
		panic(err)
	}
	starflow.RegisterFunc(client, clients.ListServers, starflow.WithName("mcp.list_servers"))
	starflow.RegisterFunc(client, clients.ListTools, starflow.WithName("mcp.list_tools"))
	starflow.RegisterFunc(client, clients.CallTool, starflow.WithName("mcp.call_tool"))
	starflow.RegisterFunc(client, clients.ListResources, starflow.WithName("mcp.list_resources"))
	starflow.RegisterFunc(client, clients.ReadResource, starflow.WithName("mcp.read_resource"))

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

	fmt.Println("Welcome to TermiChat!. Using sqlite at ", dbPath)
	fmt.Println("Type your message and press Enter. Press Ctrl+C to quit.")
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

		ConversationsStore(sqlite, conversationID, "user", msg)
		runID := fmt.Sprintf("chat-%d", time.Now().UnixNano())
		fmt.Println("")
		resp, err := client.Run(context.Background(), runID, assistantScript, msg)
		if err != nil {
			fmt.Println("[error]", err)
			continue
		}

		ConversationsStore(sqlite, conversationID, "assistant", resp)
		fmt.Printf("🤖 %s\n\n", resp)
	}
}
