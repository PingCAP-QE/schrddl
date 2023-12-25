package reduce

import (
	"github.com/stretchr/testify/require"
	"testing"
)

type checkFunc func(sql string) bool

func TestReduceSQL(t *testing.T) {
	mockChcker := func(sql string) (bool, error) {
		switch sql {
		case "select * from t":
			return false, nil
		case "select * from t where a":
			return true, nil
		default:
			return false, nil
		}
	}

	oriSQL := "select * from t where a = 1"
	reducedSQL := ReduceSQL(mockChcker, oriSQL)
	require.Equal(t, "select * from t where a = 1", reducedSQL)
}
