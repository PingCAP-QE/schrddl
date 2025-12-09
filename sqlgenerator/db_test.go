package sqlgenerator_test

import (
	"testing"

	"github.com/PingCAP-QE/schrddl/sqlgenerator"
	"github.com/stretchr/testify/require"
)

func TestRandGBKStrings(t *testing.T) {
	res := sqlgenerator.RandGBKStringRunes(10)
	require.Greater(t, len(res), 10, res)
}
