# Starflow

A workflow engine for Go that enables deterministic, resumable, and distributed workflow execution using Starlark scripting.

## Features

### Deterministic & Durable Workflows
Write workflows in Starlark (Python-like syntax) that are deterministic and can be replayed from any point with full durability guarantees. 
Every execution step is recorded and can be resumed exactly where it left off.

### Pluggable Backends
Support for in-memory, DynamoDB, and PostgreSQL storage with easy extensibility for custom backends. 
Choose the storage solution that fits your deployment environment and scale requirements.

## Architecture

Starflow primarily revolves around **Store**. **Clients** directly interact with store
to create/query runs. One or more **Worker** in the background pick up workflow runs ready
to run and execute the logic. 

## Installation

```bash
go get github.com/dynoinc/starflow
```

## Quick Start

For a complete working example, please see the [`example_test.go`](example_test.go) file in this repository. It demonstrates how to:

- Create an in-memory store
- Register functions for use in workflows
- Define workflow scripts using Starlark
- Execute workflows and retrieve results

You can run the example with:

```bash
go test -run Example
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Security

Please see [SECURITY.md](SECURITY.md) for security policy and reporting guidelines. 
