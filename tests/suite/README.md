# Starflow Test Suite

This package provides a reusable test suite for testing Starflow store implementations.

## Usage

To use this test suite with your own store implementation:

1. Import the suite package:
```go
import "github.com/dynoinc/starflow/tests/suite"
```

2. Create a test function that provides a store factory:
```go
func TestMyStore_Suite(t *testing.T) {
    suite.RunStoreSuite(t, func(t *testing.T) starflow.Store {
        // Return a new instance of your store implementation
        return NewMyStore()
    })
}
```

## Example

Here's a complete example for a memory store implementation:

```go
package mystore_test

import (
    "testing"
    "github.com/dynoinc/starflow"
    "github.com/dynoinc/starflow/tests/suite"
)

type MyStore struct {
    // Your store implementation
}

func NewMyStore() *MyStore {
    return &MyStore{}
}

// Implement the starflow.Store interface...

func TestMyStore_Suite(t *testing.T) {
    suite.RunStoreSuite(t, func(t *testing.T) starflow.Store {
        return NewMyStore()
    })
}
```

## Test Coverage

The test suite covers all the core functionality required by the `starflow.Store` interface:

- Script persistence and retrieval
- Run creation and management
- Event recording with optimistic concurrency
- Run claiming and leasing
- Yield and signal functionality
- Error handling and edge cases

## Requirements

Your store implementation must implement the complete `starflow.Store` interface:

```go
type Store interface {
    SaveScript(ctx context.Context, content []byte) (string, error)
    GetScript(ctx context.Context, scriptHash string) ([]byte, error)
    CreateRun(ctx context.Context, scriptHash string, input *anypb.Any) (string, error)
    GetRun(ctx context.Context, runID string) (*Run, error)
    ClaimRun(ctx context.Context, runID string, workerID string, leaseUntil time.Time) (bool, error)
    RecordEvent(ctx context.Context, runID string, nextEventID int64, eventMetadata EventMetadata) (int64, error)
    Signal(ctx context.Context, cid string, output *anypb.Any) error
    GetEvents(ctx context.Context, runID string) ([]*Event, error)
    FinishRun(ctx context.Context, runID string, output *anypb.Any) error
    ListRuns(ctx context.Context, statuses ...RunStatus) ([]*Run, error)
}
``` 