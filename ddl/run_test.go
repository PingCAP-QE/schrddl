package ddl

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/assert"
)

func TestDDLIgnoreError(t *testing.T) {
	err := errors.New("shard_row_id_bits 6 will cause next global auto ID 1648238210354062187 overflow random")
	assert.True(t, ddlIgnoreError(err))
	err = errors.New("shard_row_id_bits 6 will cause next global auto ID 164823821035406218d overflow")
	assert.False(t, ddlIgnoreError(err))
	assert.True(t, ddlIgnoreError(errors.New("cause next global auto ID 92738 overflow error")))
	assert.True(t, ddlIgnoreError(errors.New("cause next global auto ID overflow error")))
}
