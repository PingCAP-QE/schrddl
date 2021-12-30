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
	"flag"
	"time"

	. "github.com/PingCAP-QE/schrddl/ddl"
	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
)

var (
	dbAddr          = flag.String("addr", "127.0.0.1:4000", "database address")
	dbName          = flag.String("db", "test", "database name")
	mode            = flag.String("mode", "serial", "test mode: serial, parallel")
	concurrency     = flag.Int("concurrency", 20, "concurrency")
	tablesToCreate  = flag.Int("tables", 1, "the number of the tables to create")
	mysqlCompatible = flag.Bool("mysql-compatible", false, "disable TiDB-only features")
	testTime        = flag.Duration("time", 2*time.Hour, "test time")
)

func main() {
	flag.Parse()
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
	Run(*dbAddr, *dbName, *concurrency, *tablesToCreate, *mysqlCompatible, testType, *testTime)
}
