package dynamodb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/lithammer/shortuuid/v4"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/dynoinc/starflow"
)

// DynamoDBStore is a DynamoDB implementation of the Store interface.
type DynamoDBStore struct {
	client           *dynamodb.Client
	tableName        string
	scriptsTableName string
	eventsTableName  string
	signalsTableName string
}

// New creates a new DynamoDBStore.
func New(client *dynamodb.Client, tableName, scriptsTableName, eventsTableName, signalsTableName string) *DynamoDBStore {
	return &DynamoDBStore{
		client:           client,
		tableName:        tableName,
		scriptsTableName: scriptsTableName,
		eventsTableName:  eventsTableName,
		signalsTableName: signalsTableName,
	}
}

// formatTimestamp formats a time.Time to ISO 8601 format for DynamoDB storage.
// This ensures consistent timestamp formatting across all operations.
func formatTimestamp(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

// parseTimestamp parses an ISO 8601 formatted timestamp string back to time.Time.
func parseTimestamp(s string) (time.Time, error) {
	return time.Parse("2006-01-02T15:04:05.000Z", s)
}

// SaveScript persists the Starlark script content.
func (s *DynamoDBStore) SaveScript(ctx context.Context, content []byte) (string, error) {
	hash := sha256.Sum256(content)
	hashStr := hex.EncodeToString(hash[:])

	item := map[string]types.AttributeValue{
		"script_hash": &types.AttributeValueMemberS{Value: hashStr},
		"content":     &types.AttributeValueMemberB{Value: content},
		"created_at":  &types.AttributeValueMemberS{Value: formatTimestamp(time.Now())},
	}

	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.scriptsTableName),
		Item:      item,
	})

	if err != nil {
		return "", fmt.Errorf("failed to save script: %w", err)
	}

	return hashStr, nil
}

// GetScript retrieves a script by its sha256 hash.
func (s *DynamoDBStore) GetScript(ctx context.Context, scriptHash string) ([]byte, error) {
	result, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.scriptsTableName),
		Key: map[string]types.AttributeValue{
			"script_hash": &types.AttributeValueMemberS{Value: scriptHash},
		},
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get script: %w", err)
	}

	if result.Item == nil {
		return nil, fmt.Errorf("script with hash %s not found", scriptHash)
	}

	contentAttr, exists := result.Item["content"]
	if !exists {
		return nil, fmt.Errorf("script content not found")
	}

	content, ok := contentAttr.(*types.AttributeValueMemberB)
	if !ok {
		return nil, fmt.Errorf("invalid content type")
	}

	return content.Value, nil
}

// CreateRun creates a new run record for a given script.
func (s *DynamoDBStore) CreateRun(ctx context.Context, scriptHash string, input *anypb.Any) (string, error) {
	runID := shortuuid.New()
	now := time.Now()

	// Create the run item directly with proper types
	runItem := map[string]types.AttributeValue{
		"run_id":        &types.AttributeValueMemberS{Value: runID},
		"script_hash":   &types.AttributeValueMemberS{Value: scriptHash},
		"status":        &types.AttributeValueMemberS{Value: string(starflow.RunStatusPending)},
		"next_event_id": &types.AttributeValueMemberN{Value: "0"},
		"created_at":    &types.AttributeValueMemberS{Value: formatTimestamp(now)},
		"updated_at":    &types.AttributeValueMemberS{Value: formatTimestamp(now)},
	}

	if input != nil {
		runItem["input"] = &types.AttributeValueMemberB{Value: input.Value}
	}

	// Use a transaction to atomically verify script exists and create run
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				ConditionCheck: &types.ConditionCheck{
					TableName: aws.String(s.scriptsTableName),
					Key: map[string]types.AttributeValue{
						"script_hash": &types.AttributeValueMemberS{Value: scriptHash},
					},
					ConditionExpression: aws.String("attribute_exists(#script_hash)"),
					ExpressionAttributeNames: map[string]string{
						"#script_hash": "script_hash",
					},
				},
			},
			{
				Put: &types.Put{
					TableName:           aws.String(s.tableName),
					Item:                runItem,
					ConditionExpression: aws.String("attribute_not_exists(#run_id)"),
					ExpressionAttributeNames: map[string]string{
						"#run_id": "run_id",
					},
				},
			},
		},
	})

	if err != nil {
		var transactionCanceledErr *types.TransactionCanceledException
		if errors.As(err, &transactionCanceledErr) {
			// Check if the cancellation was due to a condition check failure
			for _, reason := range transactionCanceledErr.CancellationReasons {
				if reason.Code != nil {
					switch *reason.Code {
					case "ConditionalCheckFailed":
						if reason.Item != nil {
							// Check if it's the script check that failed
							if _, exists := reason.Item["script_hash"]; exists {
								return "", fmt.Errorf("script with hash %s not found", scriptHash)
							}
							// Check if it's the run check that failed
							if _, exists := reason.Item["run_id"]; exists {
								return "", fmt.Errorf("run with ID %s already exists", runID)
							}
						}
					}
				}
			}
		}
		return "", fmt.Errorf("failed to create run: %w", err)
	}

	return runID, nil
}

// GetRun retrieves the details of a specific run.
func (s *DynamoDBStore) GetRun(ctx context.Context, runID string) (*starflow.Run, error) {
	result, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.tableName),
		Key: map[string]types.AttributeValue{
			"run_id": &types.AttributeValueMemberS{Value: runID},
		},
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get run: %w", err)
	}

	if result.Item == nil {
		return nil, fmt.Errorf("run with ID %s not found", runID)
	}

	return s.itemToRun(result.Item)
}

// ClaimableRuns retrieves runs that are either pending or haven't been updated recently.
func (s *DynamoDBStore) ClaimableRuns(ctx context.Context, staleThreshold time.Duration) ([]*starflow.Run, error) {
	var allRuns []*starflow.Run

	// Query for pending runs using the status-index GSI
	pendingInput := &dynamodb.QueryInput{
		TableName:              aws.String(s.tableName),
		IndexName:              aws.String("status-index"),
		KeyConditionExpression: aws.String("#status = :status"),
		ExpressionAttributeNames: map[string]string{
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":status": &types.AttributeValueMemberS{Value: string(starflow.RunStatusPending)},
		},
	}

	pendingResult, err := s.client.Query(ctx, pendingInput)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending runs: %w", err)
	}

	for _, item := range pendingResult.Items {
		run, err := s.itemToRun(item)
		if err != nil {
			continue
		}
		allRuns = append(allRuns, run)
	}

	// Query for stale runs (updated more than staleThreshold ago)
	staleTime := time.Now().Add(-staleThreshold)
	staleTimeStr := formatTimestamp(staleTime)

	// For DynamoDB, we'll need to scan and filter by updated_at
	// This is less efficient but necessary for the time-based query
	scanInput := &dynamodb.ScanInput{
		TableName:        aws.String(s.tableName),
		FilterExpression: aws.String("#status IN (:running, :yielded) AND #updated_at < :stale_time"),
		ExpressionAttributeNames: map[string]string{
			"#status":     "status",
			"#updated_at": "updated_at",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":running":    &types.AttributeValueMemberS{Value: string(starflow.RunStatusRunning)},
			":yielded":    &types.AttributeValueMemberS{Value: string(starflow.RunStatusYielded)},
			":stale_time": &types.AttributeValueMemberS{Value: staleTimeStr},
		},
	}

	staleResult, err := s.client.Scan(ctx, scanInput)
	if err != nil {
		return nil, fmt.Errorf("failed to scan stale runs: %w", err)
	}

	for _, item := range staleResult.Items {
		run, err := s.itemToRun(item)
		if err != nil {
			continue
		}
		allRuns = append(allRuns, run)
	}

	return allRuns, nil
}

// RecordEvent records an event. It succeeds only if run.NextEventID==expectedNextID.
// On success the store increments NextEventID by one.
func (s *DynamoDBStore) RecordEvent(ctx context.Context, runID string, nextEventID int64, eventMetadata starflow.EventMetadata) (int64, error) {
	// Marshal the event metadata to JSON for storage
	metadataData, err := json.Marshal(eventMetadata)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal event metadata: %w", err)
	}

	// Prepare the event item for the events table
	eventItem := map[string]types.AttributeValue{
		"run_id":     &types.AttributeValueMemberS{Value: runID},
		"event_id":   &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", nextEventID)},
		"event_type": &types.AttributeValueMemberS{Value: string(eventMetadata.EventType())},
		"metadata":   &types.AttributeValueMemberS{Value: string(metadataData)},
		"timestamp":  &types.AttributeValueMemberS{Value: formatTimestamp(time.Now())},
	}

	// Prepare the run update
	updateExpression := "SET #next_event_id = #next_event_id + :inc, #updated_at = :updated_at"
	expressionAttributeNames := map[string]string{
		"#next_event_id": "next_event_id",
		"#updated_at":    "updated_at",
	}
	expressionAttributeValues := map[string]types.AttributeValue{
		":inc":        &types.AttributeValueMemberN{Value: "1"},
		":updated_at": &types.AttributeValueMemberS{Value: formatTimestamp(time.Now())},
	}

	// Add event-specific updates
	switch eventMetadata.EventType() {
	case starflow.EventTypeReturn:
		if returnEvent, ok := eventMetadata.(starflow.ReturnEvent); ok && returnEvent.Error != nil {
			updateExpression += ", #status = :status, #error = :error"
			expressionAttributeNames["#status"] = "status"
			expressionAttributeNames["#error"] = "error"
			expressionAttributeValues[":status"] = &types.AttributeValueMemberS{Value: string(starflow.RunStatusFailed)}
			expressionAttributeValues[":error"] = &types.AttributeValueMemberS{Value: returnEvent.Error.Error()}
		}
	case starflow.EventTypeYield:
		updateExpression += ", #status = :status"
		expressionAttributeNames["#status"] = "status"
		expressionAttributeValues[":status"] = &types.AttributeValueMemberS{Value: string(starflow.RunStatusYielded)}
	case starflow.EventTypeFinish:
		if finishEvent, ok := eventMetadata.(starflow.FinishEvent); ok {
			updateExpression += ", #status = :status, #output = :output"
			expressionAttributeNames["#status"] = "status"
			expressionAttributeNames["#output"] = "output"
			expressionAttributeValues[":status"] = &types.AttributeValueMemberS{Value: string(starflow.RunStatusCompleted)}
			expressionAttributeValues[":output"] = &types.AttributeValueMemberB{Value: finishEvent.Output.Value}
		}
	case starflow.EventTypeClaim:
		updateExpression += ", #status = :status"
		expressionAttributeNames["#status"] = "status"
		expressionAttributeValues[":status"] = &types.AttributeValueMemberS{Value: string(starflow.RunStatusRunning)}
	}

	// Add the condition check
	expressionAttributeValues[":expected_next_event_id"] = &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", nextEventID)}

	// Prepare transaction items
	transactItems := []types.TransactWriteItem{
		{
			Put: &types.Put{
				TableName: aws.String(s.eventsTableName),
				Item:      eventItem,
			},
		},
		{
			Update: &types.Update{
				TableName: aws.String(s.tableName),
				Key: map[string]types.AttributeValue{
					"run_id": &types.AttributeValueMemberS{Value: runID},
				},
				UpdateExpression:          aws.String(updateExpression),
				ExpressionAttributeNames:  expressionAttributeNames,
				ExpressionAttributeValues: expressionAttributeValues,
				ConditionExpression:       aws.String("#next_event_id = :expected_next_event_id"),
			},
		},
	}

	// Add signal insertion for yield events
	if eventMetadata.EventType() == starflow.EventTypeYield {
		if yieldEvent, ok := eventMetadata.(starflow.YieldEvent); ok {
			signalItem := map[string]types.AttributeValue{
				"signal_id":  &types.AttributeValueMemberS{Value: yieldEvent.SignalID},
				"run_id":     &types.AttributeValueMemberS{Value: runID},
				"created_at": &types.AttributeValueMemberS{Value: formatTimestamp(time.Now())},
			}
			transactItems = append(transactItems, types.TransactWriteItem{
				Put: &types.Put{
					TableName: aws.String(s.signalsTableName),
					Item:      signalItem,
				},
			})
		}
	}

	// Execute both operations in a single transaction
	_, err = s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})

	if err != nil {
		var transactionCanceledErr *types.TransactionCanceledException
		if errors.As(err, &transactionCanceledErr) {
			// Check if the cancellation was due to a condition check failure
			for _, reason := range transactionCanceledErr.CancellationReasons {
				if reason.Code != nil && *reason.Code == "ConditionalCheckFailed" {
					return 0, starflow.ErrConcurrentUpdate
				}
			}
		}
		return 0, fmt.Errorf("failed to record event: %w", err)
	}

	return nextEventID + 1, nil
}

// Signal handles signaling a run with a signal ID.
func (s *DynamoDBStore) Signal(ctx context.Context, cid string, output *anypb.Any) error {
	// First find the run associated with this signal ID from the signals table
	input := &dynamodb.GetItemInput{
		TableName: aws.String(s.signalsTableName),
		Key: map[string]types.AttributeValue{
			"signal_id": &types.AttributeValueMemberS{Value: cid},
		},
	}

	result, err := s.client.GetItem(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to find signal: %w", err)
	}

	if result.Item == nil {
		return fmt.Errorf("signal with ID %s not found", cid)
	}

	// Get the run ID from the signal record
	runIDAttr, exists := result.Item["run_id"]
	if !exists {
		return fmt.Errorf("signal record missing run_id")
	}

	runID, ok := runIDAttr.(*types.AttributeValueMemberS)
	if !ok {
		return fmt.Errorf("invalid run_id in signal record")
	}

	// Get the current run to get the next event ID
	run, err := s.GetRun(ctx, runID.Value)
	if err != nil {
		return fmt.Errorf("failed to get run: %w", err)
	}

	// Create resume event metadata
	resumeEvent := starflow.ResumeEvent{SignalID: cid, Output: output}
	resumeEventData, _ := json.Marshal(resumeEvent)

	// Prepare the resume event item for the events table
	eventItem := map[string]types.AttributeValue{
		"run_id":     &types.AttributeValueMemberS{Value: runID.Value},
		"event_id":   &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", run.NextEventID)},
		"event_type": &types.AttributeValueMemberS{Value: string(starflow.EventTypeResume)},
		"metadata":   &types.AttributeValueMemberS{Value: string(resumeEventData)},
		"timestamp":  &types.AttributeValueMemberS{Value: formatTimestamp(time.Now())},
	}

	// Execute atomic transaction: delete signal, insert event, update run
	// Use condition expression to ensure next_event_id hasn't changed
	_, err = s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Delete: &types.Delete{
					TableName: aws.String(s.signalsTableName),
					Key: map[string]types.AttributeValue{
						"signal_id": &types.AttributeValueMemberS{Value: cid},
					},
					ConditionExpression: aws.String("attribute_exists(#signal_id)"),
					ExpressionAttributeNames: map[string]string{
						"#signal_id": "signal_id",
					},
				},
			},
			{
				Put: &types.Put{
					TableName: aws.String(s.eventsTableName),
					Item:      eventItem,
				},
			},
			{
				Update: &types.Update{
					TableName: aws.String(s.tableName),
					Key: map[string]types.AttributeValue{
						"run_id": &types.AttributeValueMemberS{Value: runID.Value},
					},
					UpdateExpression: aws.String("SET #status = :status, #worker_id = :empty, #lease_until = :empty, #updated_at = :updated_at, #next_event_id = :new_next_event_id"),
					ExpressionAttributeNames: map[string]string{
						"#status":        "status",
						"#worker_id":     "worker_id",
						"#lease_until":   "lease_until",
						"#updated_at":    "updated_at",
						"#next_event_id": "next_event_id",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":status":                 &types.AttributeValueMemberS{Value: string(starflow.RunStatusPending)},
						":empty":                  &types.AttributeValueMemberS{Value: ""},
						":updated_at":             &types.AttributeValueMemberS{Value: formatTimestamp(time.Now())},
						":new_next_event_id":      &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", run.NextEventID+1)},
						":expected_next_event_id": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", run.NextEventID)},
					},
					ConditionExpression: aws.String("#next_event_id = :expected_next_event_id"),
				},
			},
		},
	})

	if err != nil {
		var transactionCanceledErr *types.TransactionCanceledException
		if errors.As(err, &transactionCanceledErr) {
			// Check if the cancellation was due to a condition check failure
			for _, reason := range transactionCanceledErr.CancellationReasons {
				if reason.Code != nil && *reason.Code == "ConditionalCheckFailed" {
					return starflow.ErrConcurrentUpdate
				}
			}
		}
		return fmt.Errorf("failed to signal run: %w", err)
	}

	return nil
}

// GetEvents retrieves all events for a specific run, ordered by time.
func (s *DynamoDBStore) GetEvents(ctx context.Context, runID string) ([]*starflow.Event, error) {
	// Query the events table for all events for this run
	input := &dynamodb.QueryInput{
		TableName:              aws.String(s.eventsTableName),
		KeyConditionExpression: aws.String("run_id = :run_id"),
		ScanIndexForward:       aws.Bool(true), // Order by event_id ascending
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":run_id": &types.AttributeValueMemberS{Value: runID},
		},
	}

	result, err := s.client.Query(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to query events: %w", err)
	}

	var events []*starflow.Event
	for _, item := range result.Items {
		eventTypeAttr, hasType := item["event_type"]
		metadataAttr, hasMetadata := item["metadata"]
		timestampAttr, hasTimestamp := item["timestamp"]

		if !hasType || !hasMetadata || !hasTimestamp {
			continue
		}

		eventType, ok := eventTypeAttr.(*types.AttributeValueMemberS)
		metadata, ok2 := metadataAttr.(*types.AttributeValueMemberS)
		timestamp, ok3 := timestampAttr.(*types.AttributeValueMemberS)

		if !ok || !ok2 || !ok3 {
			continue
		}

		// Parse timestamp using our consistent format
		eventTime, err := parseTimestamp(timestamp.Value)
		if err != nil {
			continue
		}

		// Deserialize metadata
		eventMetadata, err := s.deserializeEventMetadata(starflow.EventType(eventType.Value), []byte(metadata.Value))
		if err != nil {
			continue
		}

		event := &starflow.Event{
			Timestamp: eventTime,
			Type:      starflow.EventType(eventType.Value),
			Metadata:  eventMetadata,
		}

		events = append(events, event)
	}

	return events, nil
}

// Helper method to deserialize event metadata based on event type
func (s *DynamoDBStore) deserializeEventMetadata(eventType starflow.EventType, metadataBytes []byte) (starflow.EventMetadata, error) {
	switch eventType {
	case starflow.EventTypeCall:
		var event starflow.CallEvent
		err := json.Unmarshal(metadataBytes, &event)
		return event, err
	case starflow.EventTypeReturn:
		var event starflow.ReturnEvent
		err := json.Unmarshal(metadataBytes, &event)
		return event, err
	case starflow.EventTypeSleep:
		var event starflow.SleepEvent
		err := json.Unmarshal(metadataBytes, &event)
		return event, err
	case starflow.EventTypeTimeNow:
		var event starflow.TimeNowEvent
		err := json.Unmarshal(metadataBytes, &event)
		return event, err
	case starflow.EventTypeRandInt:
		var event starflow.RandIntEvent
		err := json.Unmarshal(metadataBytes, &event)
		return event, err
	case starflow.EventTypeYield:
		var event starflow.YieldEvent
		err := json.Unmarshal(metadataBytes, &event)
		return event, err
	case starflow.EventTypeResume:
		var event starflow.ResumeEvent
		err := json.Unmarshal(metadataBytes, &event)
		return event, err
	case starflow.EventTypeFinish:
		var event starflow.FinishEvent
		err := json.Unmarshal(metadataBytes, &event)
		return event, err
	case starflow.EventTypeClaim:
		var event starflow.ClaimEvent
		err := json.Unmarshal(metadataBytes, &event)
		return event, err
	default:
		return nil, fmt.Errorf("unknown event type: %s", eventType)
	}
}

// Helper method to convert DynamoDB item to Run
func (s *DynamoDBStore) itemToRun(item map[string]types.AttributeValue) (*starflow.Run, error) {
	run := &starflow.Run{}

	// Extract basic fields
	if idAttr, exists := item["run_id"]; exists {
		if id, ok := idAttr.(*types.AttributeValueMemberS); ok {
			run.ID = id.Value
		}
	}

	if scriptHashAttr, exists := item["script_hash"]; exists {
		if scriptHash, ok := scriptHashAttr.(*types.AttributeValueMemberS); ok {
			run.ScriptHash = scriptHash.Value
		}
	}

	if statusAttr, exists := item["status"]; exists {
		if status, ok := statusAttr.(*types.AttributeValueMemberS); ok {
			run.Status = starflow.RunStatus(status.Value)
		}
	}

	if nextEventIDAttr, exists := item["next_event_id"]; exists {
		if nextEventID, ok := nextEventIDAttr.(*types.AttributeValueMemberN); ok {
			if _, err := fmt.Sscanf(nextEventID.Value, "%d", &run.NextEventID); err != nil {
				return nil, fmt.Errorf("invalid next_event_id: %w", err)
			}
		}
	}

	if createdAtAttr, exists := item["created_at"]; exists {
		if createdAt, ok := createdAtAttr.(*types.AttributeValueMemberS); ok {
			if t, err := parseTimestamp(createdAt.Value); err == nil {
				run.CreatedAt = t
			}
		}
	}

	if updatedAtAttr, exists := item["updated_at"]; exists {
		if updatedAt, ok := updatedAtAttr.(*types.AttributeValueMemberS); ok {
			if t, err := parseTimestamp(updatedAt.Value); err == nil {
				run.UpdatedAt = t
			}
		}
	}

	// Extract anypb.Any fields
	if inputAttr, exists := item["input"]; exists {
		if input, ok := inputAttr.(*types.AttributeValueMemberB); ok {
			run.Input = &anypb.Any{Value: input.Value}
		}
	}

	if outputAttr, exists := item["output"]; exists {
		if output, ok := outputAttr.(*types.AttributeValueMemberB); ok {
			run.Output = &anypb.Any{Value: output.Value}
		}
	}

	// Extract error field
	if errorAttr, exists := item["error"]; exists {
		if errorStr, ok := errorAttr.(*types.AttributeValueMemberS); ok {
			run.Error = errors.New(errorStr.Value)
		}
	}

	return run, nil
}
