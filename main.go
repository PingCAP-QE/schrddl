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
	"strings"
	"time"

	"github.com/ngaut/log"

	enginepkg "github.com/PingCAP-QE/schrddl/engine"
	engineddl "github.com/PingCAP-QE/schrddl/engine/ddl"
	engineframework "github.com/PingCAP-QE/schrddl/engine/framework"
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

func main() {
	flag.Parse()

	if *output != "" {
		log.SetOutputByName(*output)
	}

	commonOpts := enginepkg.CommonOptions{
		DBAddr: *dbAddr,
		DBName: *dbName,

		Mode: enginepkg.Mode(*mode),

		Concurrency:     *concurrency,
		TablesToCreate:  *tablesToCreate,
		MySQLCompatible: *mysqlCompatible,
		TestTime:        *testTime,

		Output: *output,

		Txn:                  *txn,
		RC:                   *rc,
		Prepare:              *prepare,
		CheckDDLExtraTimeout: *checkDDLExtraTimeout,
	}

	switch *engine {
	case "ddl":
		ignored := make([]string, 0)
		if *pprofAddr != "" {
			ignored = append(ignored, "-pprof-addr")
		}
		if *aqs {
			ignored = append(ignored, "-aqs")
		}
		if *cert {
			ignored = append(ignored, "-cert")
		}
		if *tlp {
			ignored = append(ignored, "-tlp")
		}
		if *eet {
			ignored = append(ignored, "-eet")
		}
		if len(ignored) > 0 {
			log.Warnf("ignored flags for -engine ddl: %s", strings.Join(ignored, ", "))
		}

		engineddl.Run(engineddl.Options{
			CommonOptions: commonOpts,
			GlobalSortURI: *globalSortUri,
		})

	case "framework":
		ignored := make([]string, 0)
		if *globalSortUri != "" {
			ignored = append(ignored, "-global-sort-uri")
		}
		if len(ignored) > 0 {
			log.Warnf("ignored flags for -engine framework: %s", strings.Join(ignored, ", "))
		}

		engineframework.Run(engineframework.Options{
			CommonOptions: commonOpts,
			PprofAddr:     *pprofAddr,

			EnableApproximateQuerySynthesis: *aqs,
			EnableCERT:                      *cert,
			EnableTLP:                       *tlp,
			EnableEET:                       *eet,
		})

	default:
		log.Fatalf("unknown engine: %s", *engine)
	}
}
