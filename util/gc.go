package util

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ngaut/log"
)

const (
	// DefaultGCFreezeWindow bounds the GC freeze applied after a fatal worker
	// error so the failure scene is preserved for debugging without disabling
	// cluster GC effectively permanently.
	DefaultGCFreezeWindow = 72 * time.Hour
	// MaxGCFreezeWindow is the largest operator-requested freeze window.
	MaxGCFreezeWindow = 720 * time.Hour
)

var gcFreezeOnce sync.Once

// GCFreezeWindow returns the bounded debugging window for freezing cluster GC
// after a fatal worker error. SCHRDDL_GC_FREEZE_WINDOW may override it with a
// Go duration; 0 disables freezing entirely and values above MaxGCFreezeWindow
// fall back to DefaultGCFreezeWindow.
func GCFreezeWindow() time.Duration {
	window := DefaultGCFreezeWindow
	if raw := os.Getenv("SCHRDDL_GC_FREEZE_WINDOW"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		switch {
		case err != nil || parsed < 0 || parsed > MaxGCFreezeWindow:
			log.Warnf("[gc] ignoring invalid SCHRDDL_GC_FREEZE_WINDOW %q; using %s", raw, window)
		default:
			window = parsed
		}
	}
	return window
}

// AllowGlobalTuning reports whether the operator explicitly authorized the
// runner to change cluster-wide TiDB settings. Without it the process never
// issues SET GLOBAL statements, so the workload account does not need
// SUPER or SYSTEM_VARIABLES_ADMIN privileges.
func AllowGlobalTuning() bool {
	return os.Getenv("SCHRDDL_ALLOW_GLOBAL_TUNING") == "true"
}

// Executor is the subset of *sql.DB used for cluster-wide statements.
type Executor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// ApplyGlobalTuning executes a cluster-wide SET GLOBAL statement only when the
// operator opted in with SCHRDDL_ALLOW_GLOBAL_TUNING=true. Skips and failures
// are logged and never fatal, so an unprivileged account can still run tests.
func ApplyGlobalTuning(db Executor, statement, setting string) {
	if db == nil {
		return
	}
	if !AllowGlobalTuning() {
		log.Warnf("[tuning] skipped cluster-wide %s; set SCHRDDL_ALLOW_GLOBAL_TUNING=true with a privileged account to apply it", setting)
		return
	}
	if _, err := db.Exec(statement); err != nil {
		log.Warnf("[tuning] cannot apply cluster-wide %s: %v", setting, err)
	}
}

// FreezeTiKVGC preserves the failure scene before the process exits after a
// fatal worker error. The freeze is best-effort (a missing privilege is only
// logged), bounded by window so GC self-restores, applied once, and the
// previous value plus an exact restore statement are always logged.
func FreezeTiKVGC(db *sql.DB, window time.Duration) {
	if db == nil || window <= 0 {
		return
	}
	gcFreezeOnce.Do(func() {
		previous := "unknown"
		if err := db.QueryRow(
			"select VARIABLE_VALUE from mysql.tidb where VARIABLE_NAME = 'tikv_gc_life_time'").Scan(&previous); err != nil {
			log.Warnf("[gc] cannot read the current tikv_gc_life_time: %v", err)
			previous = "unknown"
		}
		if _, err := db.Exec(fmt.Sprintf("set global tikv_gc_life_time = '%s'", window)); err != nil {
			log.Warnf("[gc] cannot freeze tikv_gc_life_time (the account may lack SYSTEM_VARIABLES_ADMIN): %v", err)
			return
		}
		restore := previous
		if restore == "unknown" {
			// TiDB's documented default GC life time.
			restore = "10m"
		}
		log.Warnf("[gc] fatal worker error: cluster GC frozen for %s, tikv_gc_life_time %q -> %q; "+
			"GC self-restores when the window elapses, restore sooner with: set global tikv_gc_life_time = '%s'",
			window, previous, window, restore)
	})
}
