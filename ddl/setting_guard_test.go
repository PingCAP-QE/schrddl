package ddl

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/PingCAP-QE/schrddl/util"
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

// switchVariables runs the background variable switcher for a while and returns what reached the database.
func switchVariables(t *testing.T) []string {
	t.Helper()
	interval := setVariableInterval
	setVariableInterval = time.Millisecond
	t.Cleanup(func() { setVariableInterval = interval })

	reached := &recordedStatements{}
	pool := sql.OpenDB(recordingConnector{reached})
	defer pool.Close()
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		backgroundSetVariables(ctx, pool)
	}()
	// Two of the generator's nineteen choices are cluster-wide, so hundreds of
	// fast ticks make such a request certain.
	time.Sleep(500 * time.Millisecond)
	cancel()
	<-exited
	return reached.all()
}

func TestBackgroundVariableSwitchesGuardClusterWideSettings(t *testing.T) {
	// Session settings keep running as generated; cluster-wide ones are held
	// back unless the operator opted in, and no failure reaches the caller.
	t.Setenv("SCHRDDL_ALLOW_GLOBAL_TUNING", "")
	held := switchVariables(t)
	assert.Greater(t, len(held), 50, "the switcher did not run")
	for _, statement := range held {
		if _, global := util.GlobalSetting(statement); global {
			t.Fatalf("%q changed the cluster without the opt-in", statement)
		}
	}

	t.Setenv("SCHRDDL_ALLOW_GLOBAL_TUNING", "true")
	applied := switchVariables(t)
	clusterWide := 0
	for _, statement := range applied {
		if _, global := util.GlobalSetting(statement); global {
			clusterWide++
		}
	}
	assert.Greater(t, clusterWide, 0, "the opt-in no longer applies the generator's cluster-wide settings")
}
