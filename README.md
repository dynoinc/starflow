# Starflow

A workflow engine for Go that enables deterministic, resumable, and distributed workflow execution using Starlark scripting.

## Features

- **Deterministic Execution**: Workflows are deterministic and can be replayed from any point
- **Resumable Workflows**: Support for yielding and resuming long-running workflows
- **Starlark Scripting**: Write workflows in Starlark (Python-like syntax)
- **Multiple Backends**: Support for in-memory, DynamoDB, and PostgreSQL storage
- **Protobuf Integration**: Native support for Protocol Buffers
- **Retry Policies**: Configurable retry policies with exponential backoff
- **Event History**: Complete audit trail of workflow execution

## Installation

```bash
go get github.com/dynoinc/starflow
```

## Quick Start

```go
package main

import (
    "context"
    "time"
    
    "github.com/dynoinc/starflow"
    "github.com/dynoinc/starflow/suite/proto"
)

func main() {
    // Create an in-memory store
    store := starflow.NewInMemoryStore()
    
    // Create a worker
    worker := starflow.NewWorker[*proto.PingRequest, *proto.PingResponse](store, 10*time.Millisecond)
    
    // Register your functions
    pingFn := func(ctx context.Context, req *proto.PingRequest) (*proto.PingResponse, error) {
        return &proto.PingResponse{Message: "pong: " + req.Message}, nil
    }
    starflow.Register(worker, pingFn, starflow.WithName("pingFn"))
    
    // Create a client
    client := starflow.NewClient[*proto.PingRequest](store)
    
    // Define your workflow script
    script := `
load("proto", "proto")

def main(ctx, input):
    ping_proto = proto.file("suite/proto/ping.proto")
    return pingFn(ctx=ctx, req=ping_proto.PingRequest(message=input.message))
`
    
    // Run the workflow
    runID, err := client.Run(context.Background(), []byte(script), &proto.PingRequest{Message: "hello"})
    if err != nil {
        panic(err)
    }
    
    // Process the workflow
    worker.ProcessOnce(context.Background())
    
    // Get the result
    run, err := client.GetRun(context.Background(), runID)
    if err != nil {
        panic(err)
    }
    
    var output proto.PingResponse
    run.Output.UnmarshalTo(&output)
    println("Result:", output.Message)
}
```

## Development

### Prerequisites

- Go 1.21 or later
- Docker (for backend tests)

### Setup

1. Clone the repository:
```bash
git clone https://github.com/dynoinc/starflow.git
cd starflow
```

2. Install dependencies:
```bash
go mod download
```

### Testing

Run the full test suite:
```bash
just test
```

Or run specific commands:
```bash
just gen    # Generate code and format imports
just lint   # Run linting and security checks
```

### Available Commands

- `just` or `just test` - Run the full test suite
- `just gen` - Generate code and format imports
- `just lint` - Run linting and security checks

## Architecture

Starflow consists of several key components:

- **Worker**: Processes workflow runs and executes functions
- **Store**: Persists workflow state and events
- **Client**: Manages workflow lifecycle
- **Event System**: Records all workflow activities for replay

## Backends

Starflow supports multiple storage backends:

- **In-Memory**: For development and testing
- **DynamoDB**: For production AWS deployments
- **PostgreSQL**: For production deployments with relational databases

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Security

Please see [SECURITY.md](SECURITY.md) for security policy and reporting guidelines. 