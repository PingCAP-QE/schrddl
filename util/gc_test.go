package util

import (
	"database/sql"
	"errors"
	"testing"
	"time"
)

type fakeExecutor struct {
	statements []string
	err        error
}

func (f *fakeExecutor) Exec(query string, _ ...interface{}) (sql.Result, error) {
	f.statements = append(f.statements, query)
	return nil, f.err
}

func TestGCFreezeWindowIsBoundedAndDisablable(t *testing.T) {
	t.Setenv("SCHRDDL_GC_FREEZE_WINDOW", "")
	if got := GCFreezeWindow(); got != DefaultGCFreezeWindow {
		t.Fatalf("default window = %s, want %s", got, DefaultGCFreezeWindow)
	}
	t.Setenv("SCHRDDL_GC_FREEZE_WINDOW", "2h")
	if got := GCFreezeWindow(); got != 2*time.Hour {
		t.Fatalf("override window = %s, want 2h", got)
	}
	t.Setenv("SCHRDDL_GC_FREEZE_WINDOW", "0s")
	if got := GCFreezeWindow(); got != 0 {
		t.Fatalf("zero window = %s, want 0 (disabled)", got)
	}
	for _, invalid := range []string{"bogus", "-1h", "10000h"} {
		t.Setenv("SCHRDDL_GC_FREEZE_WINDOW", invalid)
		if got := GCFreezeWindow(); got != DefaultGCFreezeWindow {
			t.Fatalf("window for %q = %s, want fallback to %s", invalid, got, DefaultGCFreezeWindow)
		}
	}
}

func TestGlobalTuningRequiresExplicitOptIn(t *testing.T) {
	t.Setenv("SCHRDDL_ALLOW_GLOBAL_TUNING", "")
	if AllowGlobalTuning() {
		t.Fatal("global tuning enabled by default")
	}
	t.Setenv("SCHRDDL_ALLOW_GLOBAL_TUNING", "1")
	if AllowGlobalTuning() {
		t.Fatal("global tuning enabled by a non-true value")
	}
	t.Setenv("SCHRDDL_ALLOW_GLOBAL_TUNING", "true")
	if !AllowGlobalTuning() {
		t.Fatal("explicit opt-in ignored")
	}
}

func TestApplyGlobalTuningSkipsWithoutOptInAndNeverFails(t *testing.T) {
	fake := &fakeExecutor{}
	t.Setenv("SCHRDDL_ALLOW_GLOBAL_TUNING", "")
	ApplyGlobalTuning(fake, "set global max_execution_time=5000", "max_execution_time")
	if len(fake.statements) != 0 {
		t.Fatalf("unauthorized global statement executed: %v", fake.statements)
	}
	t.Setenv("SCHRDDL_ALLOW_GLOBAL_TUNING", "true")
	ApplyGlobalTuning(fake, "set global max_execution_time=5000", "max_execution_time")
	if len(fake.statements) != 1 || fake.statements[0] != "set global max_execution_time=5000" {
		t.Fatalf("authorized statement not executed exactly once: %v", fake.statements)
	}
	failing := &fakeExecutor{err: errors.New("insufficient privileges")}
	ApplyGlobalTuning(failing, "set global max_execution_time=5000", "max_execution_time")
	if len(failing.statements) != 1 {
		t.Fatal("statement not attempted")
	}
	ApplyGlobalTuning(nil, "set global max_execution_time=5000", "max_execution_time")
}

func TestFreezeTiKVGCIsSafeWithoutDatabaseOrWindow(t *testing.T) {
	FreezeTiKVGC(nil, time.Hour)
	FreezeTiKVGC(nil, 0)
}
