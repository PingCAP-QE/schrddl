package sqlgenerator

import (
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
	"testing"
)

func TestRandJsons(t *testing.T) {
	t.Run("test rand json", func(t *testing.T) {
		for i := 0; i < 20; i++ {
			json := RandJsons(1)
			t.Log(json)
		}
	})
	t.Run("test rand json with type", func(t *testing.T) {
		for i := 0; i < 20; i++ {
			tp := randArrayTp[rand.Intn(len(randArrayTp))]
			json, err := randomArrayJSON(3, tp)
			require.NoError(t, err)
			t.Log(json)
			escapeJson := "'" + json + "'"
			r := arrayExtract(escapeJson)
			t.Log(r)
		}
	})
	t.Run("test rand json sub", func(t *testing.T) {
		for i := 0; i < 20; i++ {
			str := randomArrayJSONSubValue(randArrayTp[rand.Intn(len(randArrayTp))])
			t.Log(str)
		}
	})
}
