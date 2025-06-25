package dynamodb

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/dynoinc/starflow"
	"github.com/dynoinc/starflow/suite"
	dynamodbcontainer "github.com/testcontainers/testcontainers-go/modules/dynamodb"
)

func TestDynamoDBStoreSuite(t *testing.T) {
	ctx := context.Background()

	container, err := dynamodbcontainer.Run(ctx, "amazon/dynamodb-local:latest")
	if err != nil {
		t.Fatalf("failed to start dynamodb container: %v", err)
	}
	defer container.Terminate(ctx)

	endpoint, err := container.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("failed to get dynamodb endpoint: %v", err)
	}

	// Add protocol scheme to endpoint
	endpointURL := "http://" + endpoint

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}

	dynamoClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpointURL)
	})

	// Create tables for scripts and runs
	scriptsTable := "starflow_scripts"
	runsTable := "starflow_runs"
	eventsTable := "starflow_events"
	signalsTable := "starflow_signals"

	_, err = dynamoClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: &scriptsTable,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("script_hash"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("script_hash"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("failed to create scripts table: %v", err)
	}

	_, err = dynamoClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: &runsTable,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("run_id"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("run_id"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("failed to create runs table: %v", err)
	}

	_, err = dynamoClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: &eventsTable,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("run_id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("event_id"), AttributeType: types.ScalarAttributeTypeN},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("run_id"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("event_id"), KeyType: types.KeyTypeRange},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("failed to create events table: %v", err)
	}

	_, err = dynamoClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: &signalsTable,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("signal_id"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("signal_id"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("failed to create signals table: %v", err)
	}

	// Wait for tables to be active
	waitForTable := func(table string) {
		for {
			out, err := dynamoClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &table})
			if err == nil && out.Table != nil && out.Table.TableStatus == "ACTIVE" {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	waitForTable(scriptsTable)
	waitForTable(runsTable)
	waitForTable(eventsTable)
	waitForTable(signalsTable)

	suite.RunStoreSuite(t, func(t *testing.T) starflow.Store {
		return New(dynamoClient, runsTable, scriptsTable, eventsTable, signalsTable)
	})
}
