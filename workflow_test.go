package starflow

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestYieldError(t *testing.T) {
	_, err := Yield()

	var y yieldError
	require.True(t, errors.As(err, &y))
	require.Equal(t, "workflow yielded", y.Error())
}
