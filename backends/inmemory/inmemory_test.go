package inmemory

import (
	"testing"

	"github.com/dynoinc/starflow"
	"github.com/dynoinc/starflow/suite"
)

func TestMemoryStore_Suite(t *testing.T) {
	suite.RunStoreSuite(t, func(t *testing.T) starflow.Store { return New(t) })
}
