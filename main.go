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
	"os"
	"time"

	. "github.com/PingCAP-QE/schrddl/ddl"
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
	globalSortUri        = flag.String("global-sort-uri", "", "global sort uri")
)

func prepareEnv() {
	dbURL := fmt.Sprintf("root:@tcp(%s)/%s", *dbAddr, *dbName)
	tiDb, err := sql.Open("mysql", dbURL)
	if err != nil {
		log.Fatalf("Can't open database, err: %s", err.Error())
	}
	tidbC, err := tiDb.Conn(context.Background())
	if err != nil {
		log.Fatalf("Can't connect to database, err: %s", err.Error())
	}
	if _, err = tidbC.ExecContext(context.Background(), fmt.Sprintf("set global time_zone='%s'", Local.String())); err != nil {
		if _, err = tidbC.ExecContext(context.Background(), fmt.Sprintf("set global time_zone='%s'", time.Local.String())); err != nil {
			if _, err = tidbC.ExecContext(context.Background(), fmt.Sprintf("set global time_zone='+8:00'")); err != nil {
				log.Fatalf("Can't set time_zone for tidb, please check tidb env")
			}
		}
	}
	tidbC.Close()

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
	}
	if *txn {
		EnableTransactionTest = true
	}
	if *rc {
		RCIsolation = true
	}
	if *prepare {
		Prepare = true
	}
	if *checkDDLExtraTimeout > 0 {
		CheckDDLExtraTimeout = *checkDDLExtraTimeout
	}
	if *globalSortUri != "" {
		GlobalSortUri = *globalSortUri
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
		timeoutExitLoop(*testTime)
	}()
	Run(*dbAddr, *dbName, *concurrency, *tablesToCreate, *mysqlCompatible, testType, *testTime)
}
