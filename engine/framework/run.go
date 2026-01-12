package framework

import (
	"net/http"
	_ "net/http/pprof"

	"github.com/ngaut/log"

	"github.com/PingCAP-QE/schrddl/engine"
	legacyframework "github.com/PingCAP-QE/schrddl/framework"
)

type Options struct {
	engine.CommonOptions

	PprofAddr string

	EnableApproximateQuerySynthesis bool
	EnableCERT                      bool
	EnableTLP                       bool
	EnableEET                       bool
}

func Run(opts Options) {
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
	if opts.PprofAddr != "" {
		go func() {
			if err := http.ListenAndServe(opts.PprofAddr, nil); err != nil {
				log.Warnf("pprof server exited: %v", err)
			}
		}()
	}

	legacyframework.Run(opts.DBAddr, opts.DBName, opts.Concurrency, opts.TablesToCreate, opts.MySQLCompatible, testType, opts.TestTime)
	if legacyframework.TestFail {
		log.Fatalf("test failed")
	}
}
