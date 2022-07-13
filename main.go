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
	"strings"
	"time"

	. "github.com/PingCAP-QE/schrddl/ddl"
	"github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
)

var (
	dbs             = &dbAddr{dbs: []string{}}
	dbName          = flag.String("db", "test", "database name")
	mode            = flag.String("mode", "serial", "test mode: serial, parallel")
	concurrency     = flag.Int("concurrency", 20, "concurrency")
	tablesToCreate  = flag.Int("tables", 1, "the number of the tables to create")
	mysqlCompatible = flag.Bool("mysql-compatible", false, "disable TiDB-only features")
	chaos           = flag.Bool("chaos", true, "whether to test in the chaos mode")
	testTime        = flag.Duration("time", 6*time.Hour, "test time")
	output          = flag.String("output", "", "output file")
)

func prepareEnv(dbAddr []string) {
	if len(dbAddr) < 1 {
		log.Fatalf("no db address")
	}
	dbURL := fmt.Sprintf("root:@tcp(%s)/%s", dbAddr[0], *dbName)
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

type dbAddr struct {
	dbs []string
}

func (d *dbAddr) String() string {
	return strings.Join(d.dbs, ",")
}

func (d *dbAddr) Set(s string) error {
	d.dbs = append(d.dbs, s)
	return nil
}

func main() {
	flag.Var(dbs, "addr", "database address")
	flag.Parse()
	if *output != "" {
		log.SetOutputByName(*output)
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
	prepareEnv(dbs.dbs)
	Run(dbs.dbs, *dbName, *concurrency, *tablesToCreate, *mysqlCompatible, testType, *testTime, *chaos)
}
