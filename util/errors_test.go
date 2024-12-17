package util

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/assert"
)

func TestDDLIgnoreError(t *testing.T) {
	err := errors.New("shard_row_id_bits 6 will cause next global auto ID 1648238210354062187 overflow random")
	assert.True(t, DDLIgnoreError(err))
	err = errors.New("shard_row_id_bits 6 will cause next global auto ID 164823821035406218d overflow")
	assert.False(t, DDLIgnoreError(err))
	assert.True(t, DDLIgnoreError(errors.New("cause next global auto ID 92738 overflow error")))
	assert.True(t, DDLIgnoreError(errors.New("cause next global auto ID overflow error")))
}
