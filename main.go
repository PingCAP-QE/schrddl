// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"

	"github.com/PingCAP-QE/schrddl/ddl"
	"github.com/PingCAP-QE/schrddl/framework"
)

var (
	dbAddr               = flag.String("addr", "127.0.0.1:4000", "database address")
	dbName               = flag.String("db", "test", "database name")
	engine               = flag.String("engine", "ddl", "test engine: ddl, framework")
	mode                 = flag.String("mode", "parallel", "test mode: serial, parallel")
	concurrency          = flag.Int("concurrency", 20, "concurrency")
	tablesToCreate       = flag.Int("tables", 1, "the number of the tables to create")
	mysqlCompatible      = flag.Bool("mysql-compatible", false, "disable TiDB-only features")
	testTime             = flag.Duration("time", 2*time.Hour, "test time")
	output               = flag.String("output", "", "output file")
	txn                  = flag.Bool("txn", false, "enable txn dml")
	rc                   = flag.Bool("rc-txn", false, "read-committed isolation")
	prepare              = flag.Bool("prepare", false, "use prepare statement")
	checkDDLExtraTimeout = flag.Duration("check-ddl-extra-timeout", 0, "check ddl extra timeout")
	globalSortUri        = flag.String("global-sort-uri", "", "global sort uri")

	pprofAddr = flag.String("pprof-addr", "", "enable pprof on address (framework engine), e.g. 127.0.0.1:6060")

	aqs  = flag.Bool("aqs", false, "enable Approximate Query Synthesis (framework engine)")
	cert = flag.Bool("cert", false, "enable CERT (framework engine)")
	tlp  = flag.Bool("tlp", false, "enable TLP (framework engine)")
	eet  = flag.Bool("eet", false, "enable EET (framework engine)")
)

func prepareEnv(enableIndexJoinOnAggregation bool) {
	dbURL := fmt.Sprintf("root:@tcp(%s)/%s", *dbAddr, *dbName)
	tiDb, err := sql.Open("mysql", dbURL)
	if err != nil {
		log.Fatalf("Can't open database, err: %s", err.Error())
	}
	tidbC, err := tiDb.Conn(context.Background())
	if err != nil {
		log.Fatalf("Can't connect to database, err: %s", err.Error())
	}
	if _, err = tidbC.ExecContext(context.Background(), fmt.Sprintf("set global time_zone='%s'", ddl.Local.String())); err != nil {
		if _, err = tidbC.ExecContext(context.Background(), fmt.Sprintf("set global time_zone='%s'", time.Local.String())); err != nil {
			if _, err = tidbC.ExecContext(context.Background(), "set global time_zone='+8:00'"); err != nil {
				log.Fatalf("Can't set time_zone for tidb, please check tidb env")
			}
		}
	}
	if enableIndexJoinOnAggregation {
		_, _ = tidbC.ExecContext(context.Background(), "set GLOBAL tidb_enable_inl_join_inner_multi_pattern='ON'")
	}
	_ = tidbC.Close()

	mysql.SetLogger(log.Logger())
}

func timeoutExitLoop(timeout time.Duration) {
	time.Sleep(timeout + 20*time.Second)
	os.Exit(0)
}

func main() {
	flag.Parse()

	if *output != "" {
		log.SetOutputByName(*output)
		framework.GlobalOutPut = *output
	}

	switch *engine {
	case "ddl":
		if *txn {
			ddl.EnableTransactionTest = true
		}
		if *rc {
			ddl.RCIsolation = true
		}
		if *prepare {
			ddl.Prepare = true
		}
		if *checkDDLExtraTimeout > 0 {
			ddl.CheckDDLExtraTimeout = *checkDDLExtraTimeout
		}
		if *globalSortUri != "" {
			ddl.GlobalSortUri = *globalSortUri
		}

		log.Infof("[%s-ddl] start ddl", *mode)
		var testType ddl.DDLTestType
		switch *mode {
		case "serial":
			testType = ddl.SerialDDLTest
		case "parallel":
			testType = ddl.ParallelDDLTest
		default:
			log.Fatalf("unknown test mode: %s", *mode)
		}

		prepareEnv(false)
		go func() {
			timeoutExitLoop(*testTime)
		}()
		ddl.Run(*dbAddr, *dbName, *concurrency, *tablesToCreate, *mysqlCompatible, testType, *testTime)

	case "framework":
		if *txn {
			framework.EnableTransactionTest = true
		}
		if *rc {
			framework.RCIsolation = true
		}
		if *prepare {
			framework.Prepare = true
		}
		if *checkDDLExtraTimeout > 0 {
			framework.CheckDDLExtraTimeout = *checkDDLExtraTimeout
		}
		if *aqs {
			framework.EnableApproximateQuerySynthesis = true
		}
		if *cert {
			framework.EnableCERT = true
		}
		if *tlp {
			framework.EnableTLP = true
		}
		if *eet {
			framework.EnableEET = true
		}

		log.Infof("[%s-framework] start framework", *mode)
		var testType framework.DDLTestType
		switch *mode {
		case "serial":
			testType = framework.SerialDDLTest
		case "parallel":
			testType = framework.ParallelDDLTest
		default:
			log.Fatalf("unknown test mode: %s", *mode)
		}

		prepareEnv(true)
		if *pprofAddr != "" {
			go func() {
				if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
					log.Warnf("pprof server exited: %v", err)
				}
			}()
		}

		framework.Run(*dbAddr, *dbName, *concurrency, *tablesToCreate, *mysqlCompatible, testType, *testTime)
		if framework.TestFail {
			log.Fatalf("test failed")
		}

	default:
		log.Fatalf("unknown engine: %s", *engine)
	}
}
