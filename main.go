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
	"time"

	. "github.com/PingCAP-QE/schrddl/framework"
	"github.com/PingCAP-QE/schrddl/util"
	"github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
)

var (
	dbAddr               = flag.String("addr", "127.0.0.1:4000", "database address")
	dbName               = flag.String("db", "test", "database name")
	mode                 = flag.String("mode", "serial", "test mode: serial, parallel")
	concurrency          = flag.Int("concurrency", 20, "concurrency")
	tablesToCreate       = flag.Int("tables", 1, "the number of the tables to create")
	mysqlCompatible      = flag.Bool("mysql-compatible", false, "disable TiDB-only features")
	testTime             = flag.Duration("time", 2*time.Hour, "test time")
	output               = flag.String("output", "", "output file")
	txn                  = flag.Bool("txn", false, "enable txn dml")
	rc                   = flag.Bool("rc-txn", false, "read-committed isolation")
	prepare              = flag.Bool("prepare", false, "use prepare statement")
	checkDDLExtraTimeout = flag.Duration("check-ddl-extra-timeout", 0, "check ddl extra timeout")
	aqs                  = flag.Bool("aqs", false, "enable Approximate Query Synthesis")
	cert                 = flag.Bool("cert", false, "enable CERT")
	tlp                  = flag.Bool("tlp", false, "enable TLP")
)

func prepareEnv() {
	dbURL := fmt.Sprintf("root:@tcp(%s)/%s", *dbAddr, *dbName)
	tiDb, err := sql.Open("mysql", dbURL)
	if err != nil {
		log.Fatalf("Can't open database, err: %s", err.Error())
	}
	defer tiDb.Close()

	tidbC, err := tiDb.Conn(context.Background())
	if err != nil {
		log.Fatalf("Can't connect to database, err: %s", err.Error())
	}
	defer tidbC.Close()

	if _, err = tidbC.ExecContext(context.Background(), fmt.Sprintf("set global time_zone='%s'", Local.String())); err != nil {
		if _, err = tidbC.ExecContext(context.Background(), fmt.Sprintf("set global time_zone='%s'", time.Local.String())); err != nil {
			if _, err = tidbC.ExecContext(context.Background(), "set global time_zone='+8:00'"); err != nil {
				log.Fatalf("Can't set time_zone for tidb, please check tidb env")
			}
		}
	}

	initSQLs := []string{
		"create database if not exists testcache",
		"set GLOBAL tidb_enable_inl_join_inner_multi_pattern='ON'",
		"set GLOBAL tidb_enable_instance_plan_cache=1",
		"set global tidb_enable_global_index=true",
	}
	if util.RCIsolation {
		initSQLs = append(initSQLs, "set global transaction_isolation='read-committed'")
	}

	for _, sql := range initSQLs {
		_, err = tidbC.ExecContext(context.Background(), sql)
		if err != nil {
			log.Fatalf("[DDL] %s failed", sql)
		}
	}
	mysql.SetLogger(log.Logger())
}

func main() {
	flag.Parse()
	if *output != "" {
		log.SetOutputByName(*output)
		GlobalOutPut = *output
	}
	if *txn {
		EnableTransactionTest = true
	}
	if *rc {
		util.RCIsolation = true
	}
	if *prepare {
		Prepare = true
	}
	if *checkDDLExtraTimeout > 0 {
		CheckDDLExtraTimeout = *checkDDLExtraTimeout
	}
	if *aqs {
		EnableApproximateQuerySynthesis = true
	}
	if *cert {
		EnableCERT = true
	}
	if *tlp {
		EnableTLP = true
	}
	log.Infof("[%s-ddl] start ddl", *mode)
	var testType DDLTestType
	switch *mode {
	case "serial":
		testType = SerialDDLTest
	case "parallel":
		testType = ParallelDDLTest
	default:
		log.Fatalf("unknown test mode: %s", *mode)
	}
	prepareEnv()
	go func() {
		http.ListenAndServe("127.0.0.1:6060", nil)
	}()

	cfg := CaseConfig{
		Concurrency:     *concurrency,
		TablesToCreate:  *tablesToCreate,
		MySQLCompatible: *mysqlCompatible,
		DBName:          *dbName,
		DBAddr:          *dbAddr,
		TestTp:          testType,
		TestPrepare:     *prepare,
	}

	Run(cfg, *testTime)
	if TestFail {
		log.Fatalf("test failed")
	}
}
