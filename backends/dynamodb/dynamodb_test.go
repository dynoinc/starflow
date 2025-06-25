package dynamodb

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
	dynamodbcontainer "github.com/testcontainers/testcontainers-go/modules/dynamodb"

	"github.com/dynoinc/starflow"
	"github.com/dynoinc/starflow/suite"
)

func TestDynamoDBStoreSuite(t *testing.T) {
	ctx := context.Background()

	container, err := dynamodbcontainer.Run(ctx, "amazon/dynamodb-local:latest")
	require.NoError(t, err)
	defer container.Terminate(ctx)

	endpoint, err := container.Endpoint(ctx, "")
	require.NoError(t, err)

	// Add protocol scheme to endpoint
	endpointURL := "http://" + endpoint

	// Use custom config with dummy credentials for local DynamoDB
	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.StaticCredentialsProvider{Value: aws.Credentials{AccessKeyID: "fake", SecretAccessKey: "fake"}},
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
	require.NoError(t, err)

	_, err = dynamoClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: &runsTable,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("run_id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("status"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("run_id"), KeyType: types.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("status-index"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("status"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("run_id"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

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
	require.NoError(t, err)

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
	require.NoError(t, err)

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
