package framework

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// recordedStatements is the log of a database that accepts every statement.
type recordedStatements struct {
	mu         sync.Mutex
	statements []string
}

func (r *recordedStatements) add(query string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statements = append(r.statements, query)
}

func (r *recordedStatements) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.statements...)
}

type recordingConnector struct{ log *recordedStatements }
type recordingConn struct{ log *recordedStatements }
type recordingDriver struct{}

func (connector recordingConnector) Connect(context.Context) (driver.Conn, error) {
	return &recordingConn{connector.log}, nil
}
func (recordingConnector) Driver() driver.Driver         { return recordingDriver{} }
func (recordingDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use the connector") }
func (*recordingConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (*recordingConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected transaction") }
func (*recordingConn) Close() error              { return nil }
func (conn *recordingConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	conn.log.add(query)
	return driver.RowsAffected(0), nil
}

// recordingCase builds a testCase whose statements are recorded instead of sent.
func recordingCase(t *testing.T) (*testCase, *recordedStatements) {
	t.Helper()
	reached := &recordedStatements{}
	pool := sql.OpenDB(recordingConnector{reached})
	t.Cleanup(func() { pool.Close() })
	return &testCase{dbs: []*sql.DB{pool}}, reached
}

func TestGeneratedSetGlobalOnlyReachesTheDatabaseWithTheOptIn(t *testing.T) {
	for _, statement := range []string{
		"set global tidb_ddl_enable_fast_reorg = '1'",
		"set global tidb_enable_dist_task = '0'",
		"set @@global.tidb_row_format_version = 2",
		"set @@global.tidb_enable_clustered_index = 0",
	} {
		t.Setenv("SCHRDDL_ALLOW_GLOBAL_TUNING", "")
		held, reached := recordingCase(t)
		assert.NoError(t, held.execSQL(statement))
		assert.Empty(t, reached.all(), "%q changed the cluster without the opt-in", statement)

		t.Setenv("SCHRDDL_ALLOW_GLOBAL_TUNING", "true")
		applied, reached := recordingCase(t)
		assert.NoError(t, applied.execSQL(statement))
		assert.Equal(t, []string{statement}, reached.all())
	}
}

func TestSessionSettingsAndOtherStatementsAreNotHeldBack(t *testing.T) {
	t.Setenv("SCHRDDL_ALLOW_GLOBAL_TUNING", "")
	for _, statement := range []string{
		"set tidb_max_chunk_size = 32",
		"set @@tidb_enable_chunk_rpc = 1",
		"create table t (id int)",
	} {
		c, reached := recordingCase(t)
		assert.NoError(t, c.execSQL(statement))
		assert.Equal(t, []string{statement}, reached.all())
	}
}
