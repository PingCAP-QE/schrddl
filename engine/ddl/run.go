package ddl

import (
	"github.com/ngaut/log"

	legacyddl "github.com/PingCAP-QE/schrddl/ddl"
	"github.com/PingCAP-QE/schrddl/engine"
)

type Options struct {
	engine.CommonOptions

	GlobalSortURI string
}

func Run(opts Options) {
	if opts.Txn {
		legacyddl.EnableTransactionTest = true
	}
	if opts.RC {
		legacyddl.RCIsolation = true
	}
	if opts.Prepare {
		legacyddl.Prepare = true
	}
	if opts.CheckDDLExtraTimeout > 0 {
		legacyddl.CheckDDLExtraTimeout = opts.CheckDDLExtraTimeout
	}
	if opts.GlobalSortURI != "" {
		legacyddl.GlobalSortUri = opts.GlobalSortURI
	}

	log.Infof("[%s-ddl] start ddl", opts.Mode)
	var testType legacyddl.DDLTestType
	switch opts.Mode {
	case engine.ModeSerial:
		testType = legacyddl.SerialDDLTest
	case engine.ModeParallel:
		testType = legacyddl.ParallelDDLTest
	default:
		log.Fatalf("unknown test mode: %s", opts.Mode)
	}

	engine.PrepareEnv(opts.DBAddr, opts.DBName, false)
	go func() {
		engine.TimeoutExitLoop(opts.TestTime)
	}()
	legacyddl.Run(opts.DBAddr, opts.DBName, opts.Concurrency, opts.TablesToCreate, opts.MySQLCompatible, testType, opts.TestTime)
}
