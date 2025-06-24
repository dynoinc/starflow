package postgres

import (
	"testing"

	"github.com/dynoinc/starflow"
	"github.com/dynoinc/starflow/tests/suite"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func newPostgresStore(t *testing.T) starflow.Store {
	ctx := t.Context()

	// Start PostgreSQL container
	postgresContainer, err := postgres.Run(ctx, "postgres:16", postgres.BasicWaitStrategies())
	require.NoError(t, err)
	t.Cleanup(func() {
		postgresContainer.Terminate(ctx)
	})

	// Create DSN
	dsn := postgresContainer.MustConnectionString(ctx, "sslmode=disable")

	// Create store
	store, err := NewStore(dsn)
	require.NoError(t, err)
	return store
}

func TestPostgresStore_Suite(t *testing.T) {
	suite.RunStoreSuite(t, newPostgresStore)
}
