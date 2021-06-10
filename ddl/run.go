package ddl

import (
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/ngaut/log"
	"golang.org/x/net/context"
)

var defaultPushMetricsInterval = 15 * time.Second
var enableTransactionTestFlag = "0"
var enableTransactionTest = false

func init() {
	if enableTransactionTestFlag == "1" {
		enableTransactionTest = true
	}
}

func OpenDB(dsn string, maxIdleConns int) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxIdleConns(maxIdleConns)
	log.Info("DB opens successfully")
	return db, nil
}

func Run(dbAddr string, dbName string, concurrency int, tablesToCreate int, mysqlCompatible bool, testTp DDLTestType) {
	ctx, cancel := context.WithCancel(context.Background())
	dbss := make([][]*sql.DB, 0, concurrency)
	dbDSN := fmt.Sprintf("root:@tcp(%s)/%s", dbAddr, dbName)
	for i := 0; i < concurrency; i++ {
		dbs := make([]*sql.DB, 0, 2)
		// Parallel send DDL request need more connection to send DDL request concurrently
		db0, err := OpenDB(dbDSN, 20)
		if err != nil {
			log.Fatalf("[ddl] create db client error %v", err)
		}
		db1, err := OpenDB(dbDSN, 1)
		if err != nil {
			log.Fatalf("[ddl] create db client error %v", err)
		}
		dbs = append(dbs, db0)
		dbs = append(dbs, db1)
		dbss = append(dbss, dbs)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		sig := <-sc
		log.Infof("[ddl] Got signal [%d] to exist.", sig)
		cancel()
		os.Exit(0)
	}()

	cfg := DDLCaseConfig{
		Concurrency:     concurrency,
		TablesToCreate:  tablesToCreate,
		MySQLCompatible: mysqlCompatible,
		TestTp:          testTp,
	}
	ddl := NewDDLCase(&cfg)
	exeDDLFunc := SerialExecuteOperations
	if cfg.TestTp == ParallelDDLTest {
		exeDDLFunc = ParallelExecuteOperations
	}
	execDMLFunc := SerialExecuteDML
	if enableTransactionTest {
		execDMLFunc = TransactionExecuteOperations
	}
	if err := ddl.Initialize(ctx, dbss, dbName); err != nil {
		log.Fatalf("[ddl] initialze error %v", err)
	}
	if err := ddl.Execute(ctx, dbss, exeDDLFunc, execDMLFunc); err != nil {
		log.Fatalf("[ddl] execute error %v", err)
	}
}

func dmlIgnoreError(err error) bool {
	if err == nil {
		return true
	}
	errStr := err.Error()
	if strings.Contains(errStr, "Information schema is changed") {
		return true
	}
	if strings.Contains(errStr, "try again later") {
		return true
	}
	// Sometimes, there might be duplicated entry error caused by concurrent.
	// So we ignore here.
	if strings.Contains(errStr, "Duplicate entry") {
		return true
	}
	// Sometimes, a insert to a table might generate an error caused by exceeding maximum auto increment id,
	// we ignore this error here.
	if strings.Contains(errStr, "Failed to read auto-increment value from storage engine") {
		return true
	}
	if strings.Contains(errStr, "invalid connection") {
		return true
	}
	if strings.Contains(errStr, "doesn't exist") || strings.Contains(errStr, "not found") ||
		strings.Contains(errStr, "column is deleted") || strings.Contains(errStr, "Can't find column") ||
		strings.Contains(errStr, "converting driver.Value type") || strings.Contains(errStr, "column specified twice") ||
		strings.Contains(errStr, "Out of range value for column") || strings.Contains(errStr, "Unknown column") ||
		strings.Contains(errStr, "column has index reference") || strings.Contains(errStr, "Data too long for column") ||
		strings.Contains(errStr, "Data Truncated") || strings.Contains(errStr, "no rows in result set") ||
		strings.Contains(errStr, "Truncated incorrect") || strings.Contains(errStr, "Data truncated for column") ||
		// eg: For Incorrect tinyint value, Incorrect data value...
		strings.Contains(errStr, "Incorrect") ||
		// eg: For constant 20030522161944 overflows tinyint
		strings.Contains(errStr, "overflows") ||
		strings.Contains(errStr, "Bad Number") ||
		strings.Contains(errStr, "invalid year") ||
		strings.Contains(errStr, "value is out of range in") ||
		strings.Contains(errStr, "Data Too Long"){
		return true
	}
	return false
}

func ddlIgnoreError(err error) bool {
	if err == nil {
		return true
	}
	errStr := err.Error()
	log.Warnf("check DDL err:%s", errStr)
	if strings.Contains(errStr, "Information schema is changed") {
		return true
	}
	// Sometimes, set shard row id bits to a large value might cause global auto ID overflow error.
	// We ignore this error here.
	if match, _ := regexp.MatchString(`cause next global auto ID( \d+ | )overflow`, errStr); match {
		return true
	}
	if strings.Contains(errStr, "invalid connection") {
		return true
	}
	if strings.Contains(errStr, "Unsupported shard_row_id_bits for table with primary key as row id") {
		return true
	}
	// Ignore Column Type Change error.
	if strings.Contains(errStr, "Unsupported modify column") ||
		strings.Contains(errStr, "Truncated incorrect") ||
		strings.Contains(errStr, "overflows") ||
		strings.Contains(errStr, "Invalid year value") ||
		strings.Contains(errStr, "Incorrect time value") ||
		strings.Contains(errStr, "Incorrect timestamp value") ||
		strings.Contains(errStr, "All parts of a PRIMARY KEY must be NOT NULL") ||
		strings.Contains(errStr, "value is out of range") ||
		strings.Contains(errStr, "Unsupported modify charset from") ||
		strings.Contains(errStr, "Unsupported modifying collation of column") ||
		// eg: For v"BLOB/TEXT column '319de167-6d2e-4778-966c-60b95103a02c' used in key specification without a key length"
		strings.Contains(errStr, "used in key specification without a key length") {
		fmt.Println(errStr)
		return true
	}
	if strings.Contains(errStr, "table doesn't exist") ||
		strings.Contains(errStr, "doesn't have a default value") ||
		strings.Contains(errStr, "is not exists") || strings.Contains(errStr, "column does not exist") ||
		strings.Contains(errStr, "doesn't exist") || strings.Contains(errStr, "Unknown table") ||
		strings.Contains(errStr, "admin show ddl jobs len != len(tasks)") ||
		strings.Contains(errStr, "doesn't exist") || strings.Contains(errStr, "not found") ||
		strings.Contains(errStr, "column is deleted") || strings.Contains(errStr, "Can't find column") ||
		strings.Contains(errStr, "converting driver.Value type") || strings.Contains(errStr, "column specified twice") ||
		strings.Contains(errStr, "Out of range value for column") || strings.Contains(errStr, "Unknown column") ||
		strings.Contains(errStr, "column has index reference") || strings.Contains(errStr, "Data too long for column") ||
		strings.Contains(errStr, "Data Truncated") || strings.Contains(errStr, "no rows in result set") {
		return true
	}
	return false
}
