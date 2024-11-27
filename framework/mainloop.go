package framework

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"

	"github.com/PingCAP-QE/schrddl/sqlgenerator"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
)

// The DDL test case is intended to test the correctness of DDL operations. It
// generates test cases by probability so that it should be run in background for
// enough time to see if there are any issues.
//
// The DDL test case have multiple go routines run in parallel, one for DML operations,
// other for DDL operations. The feature of each operation (for example, covering
// what kind of scenario) is determined and generated at start up time (See
// `generateDMLOps`, `generateDDLOps``), while the order of each operation is
// randomized in each round.
//
// If there are remaining DDL operations while all DML operations are performed, a
// new round of DML operations will be started (with new randomized order) and when
// all DDL operations are done, the remaining DML operations are discarded. vice
// versa.
//
// Since there are some conflicts between some DDL operations and DML operations,
// for example, inserting a row while removing a column may cause errors in
// inserting because of incorrect column numbers, some locks and some conflicting
// detections are introduced. The conflicting detection will ignore errors raised
// in such scenarios. In addition, the data in memory is stored by column instead
// of by row to minimize data conflicts in adding and removing columns.

type CaseConfig struct {
	Concurrency     int
	MySQLCompatible bool
	TablesToCreate  int
	TestTp          DDLTestType
	DBAddr          string
	DBName          string
	TestPrepare     bool
}

var globalBugSeqNum atomic.Int64
var globalRunQueryCnt atomic.Int64
var globalSuccessQueryCnt atomic.Int64

type DDLTestType int

const (
	SerialDDLTest DDLTestType = iota
	ParallelDDLTest
)

type DDLCase struct {
	cfg   *CaseConfig
	cases []*testCase
}

func (c *DDLCase) String() string {
	return "ddl"
}

func (c *DDLCase) statloop() {
	tick := time.NewTicker(10 * time.Second)
	for range tick.C {

		subcaseStat := make([]string, len(c.cases))
		subcaseUseMvindex := make([]string, len(c.cases))
		subcaseUseCERT := make([]string, len(c.cases))
		subcaseUseAggIndexJoin := make([]string, len(c.cases))
		for _, c := range c.cases {
			subcaseStat = append(subcaseStat, fmt.Sprintf("%d", len(c.queryPlanMap)))
			subcaseUseMvindex = append(subcaseUseMvindex, fmt.Sprintf("%d", c.planUseMvIndex))
			subcaseUseCERT = append(subcaseUseCERT, fmt.Sprintf("%d", c.checkCERTCnt))
			subcaseUseAggIndexJoin = append(subcaseUseAggIndexJoin, fmt.Sprintf("%d", c.aggregationAsInnerSideOfIndexJoin))

			//i := 0
			//for k, v := range c.queryPlanMap {
			//	logutil.BgLogger().Warn("sample query plan", zap.String("plan", k), zap.String("query", v))
			//	i++
			//	if i >= 10 {
			//		break
			//	}
			//}
		}

		logutil.BgLogger().Info("stat", zap.Int64("run query:", globalRunQueryCnt.Load()),
			zap.Int64("success:", globalSuccessQueryCnt.Load()),
			zap.Int64("fetch json row val:", sqlgenerator.GlobalFetchJsonRowValCnt.Load()),
			zap.Strings("unique query plan", subcaseStat),
			zap.Strings("use mv index", subcaseUseMvindex),
			zap.Strings("use CERT", subcaseUseCERT),
			zap.Strings("use agg index join", subcaseUseAggIndexJoin),
		)
	}
}

// Execute executes each goroutine (i.e. `testCase`) concurrently.
func (c *DDLCase) Execute(ctx context.Context) error {
	log.Infof("[%s] start to test...", c)
	go func() {
		c.statloop()
	}()
	defer func() {
		log.Infof("[%s] test end...", c)
	}()
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				var err error
				switch c.cases[i].caseType {
				case CaseTypeNormal:
					err = c.cases[i].execute(ctx)
				case CaseTypePlanCache:
					err = c.cases[i].testPlanCache(ctx)
				default:
					log.Fatalf("Unknown case type %d", c.cases[i].caseType)
				}

				if err != nil {
					for _, tc := range c.cases {
						tc.DisableKVGC()
					}
					log.Fatalf("[error] [instance %d] ERROR: %s", i, errors.ErrorStack(err))
				}
				select {
				case <-ctx.Done():
					log.Infof("Time is up, exit schrddl")
					return
				default:
				}
			}
		}(i)
	}
	wg.Wait()
	return nil
}

// Initialize initializes all supported charsets, collates and each concurrent
// goroutine (i.e. `testCase`).
func (c *DDLCase) Initialize(ctx context.Context, dbss [][]*sql.DB, initDB string) error {
	for i := 0; i < c.cfg.Concurrency; i++ {
		c.cases[i].dbname = initDB
		err := c.cases[i].initialize(dbss[i])
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// NewDDLCase returns a DDLCase, which contains specified `testCase`s.
func NewDDLCase(cfg *CaseConfig) *DDLCase {
	cases := make([]*testCase, cfg.Concurrency)
	fileName := "result-" + time.Now().Format("2006-01-02-15-04-05")

	if GlobalOutPut != "" {
		fileName = filepath.Dir(GlobalOutPut) + "/" + fileName
	}
	outputfile, err := os.Create(fileName)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < cfg.Concurrency; i++ {
		cases[i] = &testCase{
			cfg:          cfg,
			caseType:     CaseTypeNormal,
			tables:       make(map[string]*ddlTestTable),
			schemas:      make(map[string]*ddlTestSchema),
			views:        make(map[string]*ddlTestView),
			caseIndex:    i,
			stop:         0,
			tableMap:     make(map[string]*sqlgenerator.Table),
			outputWriter: outputfile,
			queryPlanMap: make(map[string]string),
		}
	}

	if cfg.TestPrepare {
		cases[0].caseType = CaseTypePlanCache
	}

	return &DDLCase{
		cfg:   cfg,
		cases: cases,
	}
}

const (
	ddlTestValueNull string = "NULL"
)

type checker interface {
	check(sql string, isReduce bool) (bool, error)
}
