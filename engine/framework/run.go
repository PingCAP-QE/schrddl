package framework

import (
	"github.com/ngaut/log"

	"github.com/PingCAP-QE/schrddl/engine"
	legacyframework "github.com/PingCAP-QE/schrddl/framework"
)

type Options struct {
	engine.CommonOptions

	PprofAddr      string
	PprofTokenFile string

	EnableApproximateQuerySynthesis bool
	EnableCERT                      bool
	EnableTLP                       bool
	EnableEET                       bool
}

func Run(opts Options) {
	if opts.PprofAddr != "" {
		server, err := startPprof(opts.PprofAddr, opts.PprofTokenFile)
		if err != nil {
			log.Fatalf("cannot start pprof: %v", err)
		}
		defer server.Close()
	}
	if opts.Output != "" {
		legacyframework.GlobalOutPut = opts.Output
	}

	if opts.Txn {
		legacyframework.EnableTransactionTest = true
	}
	if opts.RC {
		legacyframework.RCIsolation = true
	}
	if opts.Prepare {
		legacyframework.Prepare = true
	}
	if opts.CheckDDLExtraTimeout > 0 {
		legacyframework.CheckDDLExtraTimeout = opts.CheckDDLExtraTimeout
	}

	if opts.EnableApproximateQuerySynthesis {
		legacyframework.EnableApproximateQuerySynthesis = true
	}
	if opts.EnableCERT {
		legacyframework.EnableCERT = true
	}
	if opts.EnableTLP {
		legacyframework.EnableTLP = true
	}
	if opts.EnableEET {
		legacyframework.EnableEET = true
	}

	log.Infof("[%s-framework] start framework", opts.Mode)
	var testType legacyframework.DDLTestType
	switch opts.Mode {
	case engine.ModeSerial:
		testType = legacyframework.SerialDDLTest
	case engine.ModeParallel:
		testType = legacyframework.ParallelDDLTest
	default:
		log.Fatalf("unknown test mode: %s", opts.Mode)
	}

	engine.PrepareEnv(opts.DBAddr, opts.DBName, true)
	legacyframework.Run(opts.DBAddr, opts.DBName, opts.Concurrency, opts.TablesToCreate, opts.MySQLCompatible, testType, opts.TestTime)
	if legacyframework.TestFail {
		log.Fatalf("test failed")
	}
}
