package main

import (
	"context"
	"os"
	"strings"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/mcp"
)

type mcpClients struct {
	clients map[string]*client.Client
}

func Start(ctx context.Context, servers string) (*mcpClients, error) {
	clients := make(map[string]*client.Client)

	for server := range strings.SplitSeq(servers, ",") {
		mc, err := client.NewStdioMCPClient(server, os.Environ())
		if err != nil {
			return nil, err
		}

		resp, err := mc.Initialize(ctx, mcp.InitializeRequest{})
		if err != nil {
			return nil, err
		}

		clients[resp.ServerInfo.Name] = mc
	}

	return &mcpClients{clients: clients}, nil
}

func (c *mcpClients) ListServers(ctx context.Context, _ struct{}) ([]string, error) {
	servers := make([]string, 0, len(c.clients))
	for server := range c.clients {
		servers = append(servers, server)
	}

	return servers, nil
}
