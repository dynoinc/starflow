package postgres

import (
	"fmt"
	"testing"
	"time"

	"github.com/dynoinc/starflow"
	"github.com/dynoinc/starflow/tests/suite"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestPostgresStore_Suite(t *testing.T) {
	suite.RunStoreSuite(t, newPostgresStore)
}

func newPostgresStore(t *testing.T) starflow.Store {
	ctx := t.Context()

	// Start PostgreSQL container
	postgresContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("starflow_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections"),
		),
	)
	require.NoError(t, err)

	// Get connection details
	host, err := postgresContainer.Host(ctx)
	require.NoError(t, err)
	port, err := postgresContainer.MappedPort(ctx, "5432")
	require.NoError(t, err)

	// Create DSN
	dsn := fmt.Sprintf("host=%s port=%s user=test password=test dbname=starflow_test sslmode=disable", host, port.Port())

	// Wait a bit for the database to be fully ready
	time.Sleep(2 * time.Second)

	// Create store
	store, err := NewStore(dsn)
	require.NoError(t, err)

	// Clean up function
	t.Cleanup(func() {
		if store != nil {
			store.Close()
		}
		if postgresContainer != nil {
			postgresContainer.Terminate(ctx)
		}
	})

	return store
}
