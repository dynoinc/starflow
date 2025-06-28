package main

import (
	"context"
	"fmt"
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

func (c *mcpClients) ListTools(ctx context.Context, req struct{}) ([]map[string]any, error) {
	var allTools []map[string]any

	for serverName, client := range c.clients {
		resp, err := client.ListTools(ctx, mcp.ListToolsRequest{})
		if err != nil {
			// Log error but continue with other servers
			continue
		}

		for _, tool := range resp.Tools {
			// Convert MCP tool to OpenAI function format
			toolDef := map[string]any{
				"type": "function",
				"function": map[string]any{
					"name":        fmt.Sprintf("%s_%s", serverName, tool.Name),
					"description": tool.Description,
					"parameters":  tool.InputSchema,
				},
				"mcp_server": serverName,
				"mcp_tool":   tool.Name,
			}
			allTools = append(allTools, toolDef)
		}
	}

	return allTools, nil
}

func (c *mcpClients) CallTool(ctx context.Context, req map[string]any) (map[string]any, error) {
	serverName, ok := req["server"].(string)
	if !ok {
		return nil, fmt.Errorf("server name required")
	}

	toolName, ok := req["tool"].(string)
	if !ok {
		return nil, fmt.Errorf("tool name required")
	}

	arguments, ok := req["arguments"].(map[string]any)
	if !ok {
		arguments = make(map[string]any)
	}

	client, exists := c.clients[serverName]
	if !exists {
		return nil, fmt.Errorf("server %s not found", serverName)
	}

	resp, err := client.CallTool(ctx, mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Name:      toolName,
			Arguments: arguments,
		},
	})

	if err != nil {
		return nil, err
	}

	return map[string]any{
		"content": resp.Content,
		"isError": resp.IsError,
	}, nil
}

func (c *mcpClients) ListResources(ctx context.Context, req struct{}) ([]map[string]any, error) {
	var allResources []map[string]any

	for serverName, client := range c.clients {
		resp, err := client.ListResources(ctx, mcp.ListResourcesRequest{})
		if err != nil {
			continue
		}

		for _, resource := range resp.Resources {
			resourceDef := map[string]any{
				"uri":         resource.URI,
				"name":        resource.Name,
				"description": resource.Description,
				"mimeType":    resource.MIMEType,
				"mcp_server":  serverName,
			}
			allResources = append(allResources, resourceDef)
		}
	}

	return allResources, nil
}

func (c *mcpClients) ReadResource(ctx context.Context, req map[string]any) (map[string]any, error) {
	serverName, ok := req["server"].(string)
	if !ok {
		return nil, fmt.Errorf("server name required")
	}

	uri, ok := req["uri"].(string)
	if !ok {
		return nil, fmt.Errorf("resource URI required")
	}

	client, exists := c.clients[serverName]
	if !exists {
		return nil, fmt.Errorf("server %s not found", serverName)
	}

	resp, err := client.ReadResource(ctx, mcp.ReadResourceRequest{
		Params: mcp.ReadResourceParams{
			URI: uri,
		},
	})

	if err != nil {
		return nil, err
	}

	return map[string]any{
		"contents": resp.Contents,
	}, nil
}
