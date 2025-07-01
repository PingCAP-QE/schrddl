// Copyright 2025 PingCAP, Inc.
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
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
)

var (
	dbAddr        = flag.String("addr", "127.0.0.1:4000", "database address")
	dbName        = flag.String("db", "test", "database name")
	testTime      = flag.Duration("time", 22*time.Hour, "test time")
	output        = flag.String("output", "", "output file")
	globalSortUri = flag.String("global-sort-uri", "", "global sort uri")
	password      = flag.String("password", "", "database password")
	platform      = flag.String("platform", "ks3", "platform, ks3 or s3")
)

var GlobalSortUri string

func prepareEnv() {
	dbURL := fmt.Sprintf("root%s:@tcp(%s)/%s", *password, *dbAddr, *dbName)
	tiDb, err := sql.Open("mysql", dbURL)
	if err != nil {
		log.Fatalf("Can't open database, err: %s", err.Error())
	}
	tidbC, err := tiDb.Conn(context.Background())
	if err != nil {
		log.Fatalf("Can't connect to database, err: %s", err.Error())
	}
	if _, err = tidbC.ExecContext(context.Background(), fmt.Sprintf("set global time_zone='%s'", time.Local.String())); err != nil {
		if _, err = tidbC.ExecContext(context.Background(), fmt.Sprintf("set global time_zone='+8:00'")); err != nil {
			log.Fatalf("Can't set time_zone for tidb, please check tidb env")
		}
	}
	tidbC.Close()

	mysql.SetLogger(log.Logger())
}

func timeoutExitLoop(timeout time.Duration) {
	time.Sleep(timeout + 20*time.Second)
	os.Exit(0)
}

var S3ImportIntoDataSource = []string{"s3://sql-data-service/csv/wide_table_1t/*/*"}
var KS3ImportIntoDataSource = []string{"s3://qe-testing/kernel-testing/sql_data_service/csv/wide_table_1t/*/*.csv"}
var schemaList = []string{
	`CREATE TABLE sbtest1 (
    id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
    k int NOT NULL DEFAULT '0',
    c char(120) NOT NULL DEFAULT '',
    pad char(60) NOT NULL DEFAULT '',
    int_0 int NOT NULL DEFAULT '0', -- stdDev=10000.0, mean=0.0,
    int_1 int NOT NULL DEFAULT '0', -- stdDev=1000000.0, mean=0.0,
    int_2 int NOT NULL DEFAULT '0', -- stdDev=700000000.0, mean=0.0,
    bigint_0 bigint DEFAULT NULL UNIQUE KEY, -- TOTAL ORDERED
    bigint_1 bigint DEFAULT NULL UNIQUE KEY, -- PARTIAL ORDERED
    bigint_2 bigint DEFAULT NULL UNIQUE KEY, -- TOTAL RANDOM
    varchar_0 varchar(768) DEFAULT NULL UNIQUE KEY,
    text_0 text DEFAULT NULL, -- varchar(61440)
    json_0 json DEFAULT NULL
);`,
	`CREATE TABLE sbtest1 (
    id bigint NOT NULL AUTO_INCREMENT,
    k int NOT NULL DEFAULT '0',
    c char(120) NOT NULL DEFAULT '',
    pad char(60) NOT NULL DEFAULT '',
    int_0 int NOT NULL DEFAULT '0', -- stdDev=10000.0, mean=0.0,
    int_1 int NOT NULL DEFAULT '0', -- stdDev=1000000.0, mean=0.0,
    int_2 int NOT NULL DEFAULT '0', -- stdDev=700000000.0, mean=0.0,
    bigint_0 bigint DEFAULT NULL UNIQUE KEY, -- TOTAL ORDERED
    bigint_1 bigint DEFAULT NULL UNIQUE KEY, -- PARTIAL ORDERED
    bigint_2 bigint DEFAULT NULL UNIQUE KEY, -- TOTAL RANDOM
    varchar_0 varchar(768) DEFAULT NULL UNIQUE KEY,
    text_0 text DEFAULT NULL, -- varchar(61440)
    json_0 json DEFAULT NULL,
    PRIMARY KEY(c, id)
);`,
	`CREATE TABLE sbtest1 (
    id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
    k int NOT NULL DEFAULT '0',
    c char(120) NOT NULL DEFAULT '',
    pad char(60) NOT NULL DEFAULT '',
    int_0 int NOT NULL DEFAULT '0', -- stdDev=10000.0, mean=0.0,
    int_1 int NOT NULL DEFAULT '0', -- stdDev=1000000.0, mean=0.0,
    int_2 int NOT NULL DEFAULT '0', -- stdDev=700000000.0, mean=0.0,
    bigint_0 bigint DEFAULT NULL UNIQUE KEY GLOBAL, -- TOTAL ORDERED
    bigint_1 bigint DEFAULT NULL UNIQUE KEY GLOBAL, -- PARTIAL ORDERED
    bigint_2 bigint DEFAULT NULL UNIQUE KEY GLOBAL, -- TOTAL RANDOM
    varchar_0 varchar(768) DEFAULT NULL UNIQUE KEY GLOBAL,
    text_0 text DEFAULT NULL, -- varchar(61440)
    json_0 json DEFAULT NULL,
    unique key (id, c)
) partition by hash(id) partitions 256;`,
	`CREATE TABLE sbtest1 (
    id bigint NOT NULL AUTO_INCREMENT,
    k int NOT NULL DEFAULT '0',
    c char(120) NOT NULL DEFAULT '',
    pad char(60) NOT NULL DEFAULT '',
    int_0 int NOT NULL DEFAULT '0', -- stdDev=10000.0, mean=0.0,
    int_1 int NOT NULL DEFAULT '0', -- stdDev=1000000.0, mean=0.0,
    int_2 int NOT NULL DEFAULT '0', -- stdDev=700000000.0, mean=0.0,
    bigint_0 bigint DEFAULT NULL UNIQUE KEY GLOBAL, -- TOTAL ORDERED
    bigint_1 bigint DEFAULT NULL UNIQUE KEY GLOBAL, -- PARTIAL ORDERED
    bigint_2 bigint DEFAULT NULL UNIQUE KEY GLOBAL, -- TOTAL RANDOM
    varchar_0 varchar(768) DEFAULT NULL UNIQUE KEY GLOBAL,
    text_0 text DEFAULT NULL, -- varchar(61440)
    json_0 json DEFAULT NULL,
    PRIMARY KEY(c, id),
    unique key (c, id, k)
) partition by hash(id) partitions 256;`,
	`CREATE TABLE sbtest1 (
    id bigint NOT NULL AUTO_INCREMENT,
    k int NOT NULL DEFAULT '0',
    c char(120) NOT NULL DEFAULT '',
    pad char(60) NOT NULL DEFAULT '',
    int_0 int NOT NULL DEFAULT '0', -- stdDev=10000.0, mean=0.0,
    int_1 int NOT NULL DEFAULT '0', -- stdDev=1000000.0, mean=0.0,
    int_2 int NOT NULL DEFAULT '0', -- stdDev=700000000.0, mean=0.0,
    bigint_0 bigint DEFAULT NULL UNIQUE KEY, -- TOTAL ORDERED
    bigint_1 bigint DEFAULT NULL UNIQUE KEY, -- PARTIAL ORDERED
    bigint_2 bigint DEFAULT NULL UNIQUE KEY, -- TOTAL RANDOM
    varchar_0 varchar(768) DEFAULT NULL UNIQUE KEY,
    text_0 text DEFAULT NULL, -- varchar(61440)
    json_0 json DEFAULT NULL,
    PRIMARY KEY(id, c)
);`,
	`CREATE TABLE sbtest1 (
    id bigint NOT NULL AUTO_INCREMENT,
    k int NOT NULL DEFAULT '0',
    c char(120) NOT NULL DEFAULT '',
    pad char(60) NOT NULL DEFAULT '',
    int_0 int NOT NULL DEFAULT '0', -- stdDev=10000.0, mean=0.0,
    int_1 int NOT NULL DEFAULT '0', -- stdDev=1000000.0, mean=0.0,
    int_2 int NOT NULL DEFAULT '0', -- stdDev=700000000.0, mean=0.0,
    bigint_0 bigint DEFAULT NULL UNIQUE KEY GLOBAL, -- TOTAL ORDERED
    bigint_1 bigint DEFAULT NULL UNIQUE KEY GLOBAL, -- PARTIAL ORDERED
    bigint_2 bigint DEFAULT NULL UNIQUE KEY GLOBAL, -- TOTAL RANDOM
    varchar_0 varchar(768) DEFAULT NULL UNIQUE KEY GLOBAL,
    text_0 text DEFAULT NULL, -- varchar(61440)
    json_0 json DEFAULT NULL,
    PRIMARY KEY(id, c),
    unique key (id, c, k)
) partition by hash(id) partitions 256;`,
}

var IndexList = [][]string{
	{"alter table sbtest1 add index idx_k(k)", "alter table sbtest1 add index idx_id(id)", "alter table sbtest1 add index idx_c(c)"},
	{"alter table sbtest1 add index idx_k(k)", "alter table sbtest1 add index idx_id(id)", "alter table sbtest1 add index idx_c(c)"},
	{"alter table sbtest1 add index idx_k(id, k)", "alter table sbtest1 add index idx_id(id)", "alter table sbtest1 add index idx_c(id, c)"},
	{"alter table sbtest1 add index idx_k(c, id, k)", "alter table sbtest1 add index idx_id(c, id)"},
	{"alter table sbtest1 add index idx_k(k)", "alter table sbtest1 add index idx_id(id)", "alter table sbtest1 add index idx_c(c)"},
	{"alter table sbtest1 add index idx_k(id, c, k)", "alter table sbtest1 add index idx_id(id, c)"},
}

func main() {
	flag.Parse()
	if *output != "" {
		log.SetOutputByName(*output)
	}
	if *globalSortUri != "" {
		GlobalSortUri = *globalSortUri
	}
	log.Infof("start test")
	prepareEnv()
	go func() {
		timeoutExitLoop(*testTime)
	}()

	dbURL := fmt.Sprintf("root%s:@tcp(%s)/%s", *password, *dbAddr, *dbName)
	tiDb, err := sql.Open("mysql", dbURL)
	if err != nil {
		log.Fatalf("Can't open database, err: %s", err.Error())
	}
	tidbC, err := tiDb.Conn(context.Background())
	if _, err = tidbC.ExecContext(context.Background(), "use test"); err != nil {
		log.Fatalf("Can't use database, err: %s", err.Error())
	}

	if err != nil {
		log.Fatalf("Can't connect to database, err: %s", err.Error())
	}
	for {
		// 1. Pick a random schema
		i := rand.Intn(len(schemaList))
		tbl := schemaList[i]
		log.Infof("create table with schema: %s", tbl)
		_, err = tidbC.ExecContext(context.Background(), tbl)
		if err != nil {
			log.Fatalf("Can't create table, err: %s", err.Error())
		}

		// 2. Set variable
		if rand.Intn(2) == 0 {
			_, err = tidbC.ExecContext(context.Background(), "set global tidb_max_dist_task_nodes = 1")
		} else {
			_, err = tidbC.ExecContext(context.Background(), "set global tidb_max_dist_task_nodes = 3")
		}
		if err != nil {
			log.Fatalf("Can't set variable, err: %s", err.Error())
		}

		// 3. Pick a random data source to import data
		dataSource := S3ImportIntoDataSource[rand.Intn(len(S3ImportIntoDataSource))]
		if *platform == "ks3" {
			dataSource = KS3ImportIntoDataSource[rand.Intn(len(KS3ImportIntoDataSource))]
			aksk := (*globalSortUri)[strings.Index(*globalSortUri, "?"):]
			dataSource += aksk
		}
		importSQL := fmt.Sprintf("IMPORT INTO sbtest1 FROM '%s' WITH FIELDS_DEFINED_NULL_BY='NULL', SPLIT_FILE, THREAD=8,LINES_TERMINATED_BY='\n'", dataSource)
		_, err = tidbC.ExecContext(context.Background(), importSQL)
		if err != nil {
			log.Fatalf("Can't import table, err: %s", err.Error())
		}

		// 4. Pick an index to add
		addIndexSQL := IndexList[i][rand.Intn(len(IndexList[i]))]
		log.Infof("add index with sql: %s", addIndexSQL)
		_, err = tidbC.ExecContext(context.Background(), addIndexSQL)
		if err != nil {
			log.Fatalf("Can't add index, err: %s", err.Error())
		}

		// 5. Drop the table
		_, err = tidbC.ExecContext(context.Background(), "DROP TABLE sbtest1")
		if err != nil {
			log.Fatalf("Can't drop table, err: %s", err.Error())
		}
	}
}
