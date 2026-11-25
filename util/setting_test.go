package util

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PingCAP-QE/schrddl/sqlgenerator"
)

func TestGlobalSettingRecognizesEveryClusterWideForm(t *testing.T) {
	for statement, setting := range map[string]string{
		"set global tidb_enable_dist_task='1'":            "tidb_enable_dist_task",
		"SET GLOBAL max_execution_time=5000":              "max_execution_time",
		"set @@global.tidb_row_format_version = 2":        "tidb_row_format_version",
		"set @@GLOBAL.tidb_enable_clustered_index = 0":    "tidb_enable_clustered_index",
		"  set\n global tidb_ddl_enable_fast_reorg = '0'": "tidb_ddl_enable_fast_reorg",
		// A mixed assignment changes a cluster-wide setting as well.
		"set @@global.max_execution_time=1, tidb_x=2": "max_execution_time,tidb_x",
		// Anything that cannot be read as one plain assignment fails closed and is named by its text.
		"set global a = 1; set global b = 2":             "set global a = 1; set global b = 2",
		"set this is not sql":                            "set this is not sql",
		"set password for worker = 'synthetic-password'": "set password for worker = 'synthetic-password'",
	} {
		if got, global := GlobalSetting(statement); !global || got != setting {
			t.Errorf("GlobalSetting(%q) = %q, %v; want %q, true", statement, got, global, setting)
		}
	}
	for _, statement := range []string{
		"set tidb_max_chunk_size = 32",
		"set session tidb_executor_concurrency = 5",
		"set @@tidb_max_chunk_size=4096",
		"set @@session.tidb_max_chunk_size=4096",
		"set names utf8mb4",
		// Not a SET statement at all, whatever it starts with.
		"select @@global.max_execution_time",
		"insert into settings values (1)",
		"settings",
		"set",
	} {
		if setting, global := GlobalSetting(statement); global {
			t.Errorf("GlobalSetting(%q) = %q, true; want an ordinary statement", statement, setting)
		}
	}
}

func TestExecSettingKeepsSessionSettingsAndGuardsClusterWideOnes(t *testing.T) {
	t.Setenv("SCHRDDL_ALLOW_GLOBAL_TUNING", "")
	session := "set tidb_max_chunk_size = 32"
	cluster := "set global tidb_enable_dist_task = '1'"

	ran := &fakeExecutor{err: errors.New("unknown variable")}
	if err := ExecSetting(context.Background(), ran, session); err == nil {
		t.Fatal("a failed session setting was not reported")
	} else if len(ran.statements) != 1 || ran.statements[0] != session {
		t.Fatalf("session setting = %v, want it to run as generated", ran.statements)
	}

	skipped := &fakeExecutor{}
	if err := ExecSetting(context.Background(), skipped, cluster); err != nil {
		t.Fatal("a skipped cluster-wide setting must not fail the caller")
	}
	if len(skipped.statements) != 0 {
		t.Fatalf("cluster-wide setting reached the database without the opt-in: %v", skipped.statements)
	}

	t.Setenv("SCHRDDL_ALLOW_GLOBAL_TUNING", "true")
	applied := &fakeExecutor{err: errors.New("command denied")}
	if err := ExecSetting(context.Background(), applied, cluster); err != nil {
		t.Fatal("a cluster-wide setting is best-effort and must not fail the caller")
	}
	if len(applied.statements) != 1 || applied.statements[0] != cluster {
		t.Fatalf("opted-in cluster-wide setting = %v, want it applied", applied.statements)
	}
}

func TestApplyGlobalTuningReportsWhetherTheStatementWasApplied(t *testing.T) {
	t.Setenv("SCHRDDL_ALLOW_GLOBAL_TUNING", "")
	skipped := &fakeExecutor{}
	if ApplyGlobalTuning(skipped, "set global max_execution_time=5000", "max_execution_time") {
		t.Fatal("reported a skipped statement as applied")
	}
	t.Setenv("SCHRDDL_ALLOW_GLOBAL_TUNING", "true")
	failing := &fakeExecutor{err: errors.New("command denied")}
	if ApplyGlobalTuning(failing, "set global max_execution_time=5000", "max_execution_time") {
		t.Fatal("reported a rejected statement as applied")
	}
	withArg := &fakeExecutor{}
	statement, value := "set global tidb_cloud_storage_uri=?", "s3://bucket/prefix"
	if !ApplyGlobalTuning(withArg, statement, "tidb_cloud_storage_uri", value) {
		t.Fatal("reported an applied statement as not applied")
	}
	if len(withArg.statements) != 1 || withArg.statements[0] != statement {
		t.Fatalf("statement = %v, want %q", withArg.statements, statement)
	}
	if ApplyGlobalTuning(nil, statement, "tidb_cloud_storage_uri", value) {
		t.Fatal("reported a statement without a database as applied")
	}
}

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

func TestFreezeTiKVGCIsAClusterWideChangeAndNeedsTheOptIn(t *testing.T) {
	// The freeze applies at most once per process, so each scenario starts it over.
	freeze := func() []string {
		gcFreezeOnce = sync.Once{}
		recorded := &recordedStatements{}
		database := sql.OpenDB(recordingConnector{recorded})
		defer database.Close()
		FreezeTiKVGC(database, time.Hour)
		return recorded.all()
	}

	t.Setenv("SCHRDDL_ALLOW_GLOBAL_TUNING", "")
	if frozen := freeze(); len(frozen) != 0 {
		t.Fatalf("cluster GC was frozen without the opt-in: %v", frozen)
	}
	t.Setenv("SCHRDDL_ALLOW_GLOBAL_TUNING", "true")
	frozen := freeze()
	if len(frozen) != 1 || frozen[0] != "set global tikv_gc_life_time = '1h0m0s'" {
		t.Fatalf("opted-in freeze = %v, want one bounded tikv_gc_life_time change", frozen)
	}
}

func TestGeneratorClusterWideSettingsAreRecognizedByTheGuard(t *testing.T) {
	state := sqlgenerator.NewState()
	defer state.CheckIntegrity()
	seen := map[string]bool{}
	clusterWide, ordinary := 0, 0
	rules := []sqlgenerator.Fn{sqlgenerator.SetVariable, sqlgenerator.SetSystemVars, sqlgenerator.Start}
	for i := 0; i < 3000; i++ {
		statement, err := rules[i%len(rules)].Eval(state)
		if err != nil {
			t.Fatal(err)
		}
		setting, global := GlobalSetting(statement)
		lower := strings.ToLower(statement)
		requested := strings.Contains(lower, "set global") || strings.Contains(lower, "@@global.")
		switch {
		case global && !requested:
			t.Fatalf("%q is not a cluster-wide change but was held back", statement)
		case !global && requested:
			t.Fatalf("cluster-wide %q is not recognized by the guard", statement)
		case global:
			clusterWide++
			seen[setting] = true
		default:
			ordinary++
		}
	}
	if clusterWide == 0 || ordinary == 0 {
		t.Fatalf("cluster-wide %d, ordinary %d: the generator moved and this test needs updating", clusterWide, ordinary)
	}
	for _, setting := range []string{"tidb_ddl_enable_fast_reorg", "tidb_enable_dist_task", "tidb_row_format_version", "tidb_enable_clustered_index"} {
		if !seen[setting] {
			t.Errorf("the generator never offered cluster-wide %s; this test no longer covers it", setting)
		}
	}
}

// TestClusterWideSettingsOnlyGoThroughTheGuard pins what the
// SCHRDDL_ALLOW_GLOBAL_TUNING guardrail promises for the whole repository.
// Outside the generator, whose output is classified before it is executed, and
// the guard itself, a cluster-wide SET may only appear as an argument of
// util.ApplyGlobalTuning.
func TestClusterWideSettingsOnlyGoThroughTheGuard(t *testing.T) {
	const root = ".."
	clusterWide := regexp.MustCompile(`(?i)\bset\s+(@@\s*)?global\b`)
	var unguarded []string
	skip := func(rel string) bool {
		dir, _ := filepath.Split(rel)
		return dir == "sqlgenerator/" || dir == "util/"
	}
	walk := func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path != root && (strings.HasPrefix(entry.Name(), ".") || strings.HasPrefix(entry.Name(), "bug-")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil || skip(filepath.ToSlash(rel)) {
			return err
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for number, line := range strings.Split(string(content), "\n") {
			line = strings.TrimSpace(line)
			if clusterWide.MatchString(line) && !strings.HasPrefix(line, "//") && !strings.Contains(line, "ApplyGlobalTuning(") {
				unguarded = append(unguarded, fmt.Sprintf("%s:%d: %s", rel, number+1, line))
			}
		}
		return nil
	}
	if err := filepath.WalkDir(root, walk); err != nil {
		t.Fatal(err)
	}
	if len(unguarded) > 0 {
		t.Fatalf("cluster-wide SET statements must be issued through util.ApplyGlobalTuning:\n%s",
			strings.Join(unguarded, "\n"))
	}
}
