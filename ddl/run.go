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

var EnableTransactionTest = false
var RCIsolation = false
var Prepare = false
var CheckDDLExtraTimeout = 0 * time.Second
var GlobalSortUri = ""

func OpenDB(dsn string, maxIdleConns int) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxIdleConns(maxIdleConns)
	log.Info("DB opens successfully")
	return db, nil
}

func Run(dbAddr string, dbName string, concurrency int, tablesToCreate int, mysqlCompatible bool, testTp DDLTestType, testTime time.Duration) {
	wrapCtx := context.WithCancel
	if testTime > 0 {
		wrapCtx = func(ctx context.Context) (context.Context, context.CancelFunc) {
			return context.WithTimeout(ctx, testTime)
		}
	}
	ctx, cancel := wrapCtx(context.Background())
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
	if EnableTransactionTest {
		execDMLFunc = TransactionExecuteOperations
	}
	var err error
	if GlobalSortUri != "" {
		dbss[0][0].Exec("set global tidb_enable_dist_task=1")
		dbss[0][0].Exec(fmt.Sprintf("set global tidb_cloud_storage_uri='%s'", GlobalSortUri))
	}
	globalDDLSeqNum, err = getStartDDLSeqNum(dbss[0][0])
	if err != nil {
		log.Fatalf("[ddl] get start ddl seq num error %v", err)
	}
	if RCIsolation {
		dbss[0][0].Exec("set global transaction_isolation='read-committed'")
	}
	if err := ddl.Initialize(ctx, dbss, dbName); err != nil {
		log.Fatalf("[ddl] initialze error %v", err)
	}
	if err := ddl.Execute(ctx, dbss, exeDDLFunc, execDMLFunc); err != nil {
		log.Fatalf("[ddl] execute error %v", err)
	}
}

var checkList = []string{
	"can't have a default value",
	"strconv.Atoi",
}

func dmlIgnoreError(err error) bool {
	if err == nil {
		return true
	}
	errStr := err.Error()
	if strings.Contains(errStr, "assert") {
		return false
	} else {
		return true
	}
	for _, check := range checkList {
		if strings.Contains(errStr, check) {
			return true
		}
	}
	if strings.Contains(errStr, "slice bounds out of range") {
		return true
	}
	if strings.Contains(errStr, "bad connection") {
		return true
	}
	if strings.Contains(errStr, "Information schema is changed") && !RCIsolation {
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
	if strings.Contains(errStr, "doesn't exist") ||
		strings.Contains(errStr, "column is deleted") || strings.Contains(errStr, "Can't find column") ||
		strings.Contains(errStr, "converting driver.Value type") || strings.Contains(errStr, "column specified twice") ||
		strings.Contains(errStr, "Out of range value for column") || strings.Contains(errStr, "Unknown column") ||
		strings.Contains(errStr, "column has index reference") || strings.Contains(errStr, "Data too long for column") ||
		strings.Contains(errStr, "Data truncated") || strings.Contains(errStr, "no rows in result set") ||
		strings.Contains(errStr, "Truncated incorrect") || strings.Contains(errStr, "Data truncated for column") ||
		// eg: For Incorrect tinyint value, Incorrect data value...
		strings.Contains(errStr, "Incorrect") ||
		// eg: For constant 20030522161944 overflows tinyint
		strings.Contains(errStr, "overflows") ||
		strings.Contains(errStr, "Bad Number") ||
		strings.Contains(errStr, "invalid year") ||
		strings.Contains(errStr, "value is out of range in") ||
		strings.Contains(errStr, "Data Too Long") ||
		strings.Contains(errStr, "doesn't have a default value") ||
		strings.Contains(errStr, "specified twice") ||
		strings.Contains(errStr, "cannot convert datum from") ||
		strings.Contains(errStr, "sql_mode=only_full_group_by") ||
		strings.Contains(errStr, "cannot be null") ||
		strings.Contains(errStr, "Column count doesn't match value count") ||
		strings.Contains(errStr, "Percentage value") ||
		strings.Contains(errStr, "Index column") ||
		strings.Contains(errStr, "Illegal mix of collations") ||
		strings.Contains(errStr, "Cannot convert string") ||
		strings.Contains(errStr, "interface conversion") ||
		strings.Contains(errStr, "connection is already closed") ||
		strings.Contains(errStr, "should contain a UNION") ||
		strings.Contains(errStr, "have different column counts") ||
		strings.Contains(errStr, "followed by one or more recursive ones") ||
		strings.Contains(errStr, "Not unique table/alias") ||
		strings.Contains(errStr, "have a different number of columns") ||
		strings.Contains(errStr, "Split table region lower value count") ||
		strings.Contains(errStr, "Out Of Memory") ||
		strings.Contains(errStr, "invalid syntax") ||
		strings.Contains(errStr, "newer than query schema version") ||
		strings.Contains(errStr, "PD server timeout") ||
		strings.Contains(errStr, "Information schema is out of date") ||
		strings.Contains(errStr, "Your query has been cancelled due to exceeding the allowed memory limit for a single SQL query") {
		return true
	}
	if strings.Contains(errStr, "Unsupported multi schema change") {
		return true
	}
	if !RCIsolation && strings.Contains(errStr, "public column") {
		return true
	}
	return false
}

var ddlIgnoreList = []string{
	"Specified key was too long",
	"Incorrect date value",
}

func ddlIgnoreError(err error) bool {
	if err == nil {
		return true
	}
	errStr := err.Error()
	log.Warnf("check DDL err:%s", errStr)
	for _, l := range ddlIgnoreList {
		if strings.Contains(errStr, l) {
			return true
		}
	}
	if strings.Contains(errStr, "Information schema is changed") {
		return true
	}
	if strings.Contains(errStr, "can't have a default value") {
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
		strings.Contains(errStr, "Cancelled DDL job") ||
		strings.Contains(errStr, "Truncated incorrect") ||
		strings.Contains(errStr, "overflows") ||
		strings.Contains(errStr, "Invalid year value") ||
		strings.Contains(errStr, "Incorrect time value") ||
		strings.Contains(errStr, "Incorrect datetime value") ||
		strings.Contains(errStr, "Incorrect timestamp value") ||
		strings.Contains(errStr, "All parts of a PRIMARY KEY must be NOT NULL") ||
		strings.Contains(errStr, "value is out of range") ||
		strings.Contains(errStr, "Unsupported modify charset from") ||
		strings.Contains(errStr, "Unsupported modifying collation of column") ||
		strings.Contains(errStr, "Data truncated") ||
		strings.Contains(errStr, "Bad Number") ||
		strings.Contains(errStr, "cannot convert") ||
		strings.Contains(errStr, "Data Too Long") ||
		// eg: For v"BLOB/TEXT column '319de167-6d2e-4778-966c-60b95103a02c' used in key specification without a key length"
		strings.Contains(errStr, "used in key specification without a key length") ||
		strings.Contains(errStr, "Specified key was too long; max key length is ") ||
		strings.Contains(errStr, "should be less than the total tiflash server count") ||
		strings.Contains(errStr, "Unsupported ALTER TiFlash settings") {
		fmt.Println(errStr)
		return true
	}
	if strings.Contains(errStr, "table doesn't exist") ||
		strings.Contains(errStr, "doesn't have a default value") ||
		strings.Contains(errStr, "with composite index covered or Primary Key covered now") ||
		strings.Contains(errStr, "does not exist, this column may have been updated by other DDL") ||
		strings.Contains(errStr, "is not exists") || strings.Contains(errStr, "column does not exist") ||
		strings.Contains(errStr, "doesn't exist") || strings.Contains(errStr, "Unknown table") ||
		strings.Contains(errStr, "admin show ddl jobs len != len(tasks)") ||
		strings.Contains(errStr, "check that column/key exists") ||
		strings.Contains(errStr, "Invalid default value") ||
		strings.Contains(errStr, "Duplicate column name") ||
		strings.Contains(errStr, "can't drop only column") ||
		strings.Contains(errStr, "doesn't exist") || strings.Contains(errStr, "not found") ||
		strings.Contains(errStr, "column is deleted") || strings.Contains(errStr, "Can't find column") ||
		strings.Contains(errStr, "converting driver.Value type") || strings.Contains(errStr, "column specified twice") ||
		strings.Contains(errStr, "Out of range value for column") || strings.Contains(errStr, "Unknown column") ||
		strings.Contains(errStr, "column has index reference") || strings.Contains(errStr, "Data too long for column") ||
		strings.Contains(errStr, "Data truncated") || strings.Contains(errStr, "no rows in result set") ||
		strings.Contains(errStr, "with tidb_enable_change_multi_schema is disable") ||
		strings.Contains(errStr, "not allowed type for this type of partitioning") ||
		strings.Contains(errStr, "A PRIMARY KEY must include all columns in the table's partitioning function") ||
		strings.Contains(errStr, "A UNIQUE INDEX must include all columns in the table's partitioning function") ||
		strings.Contains(errStr, "cannot convert datum") ||
		strings.Contains(errStr, "Duplicate entry") ||
		strings.Contains(errStr, "has a partitioning function dependency and cannot be dropped or renamed") ||
		strings.Contains(errStr, "A CLUSTERED INDEX must include all columns in the table's partitioning function") ||
		strings.Contains(errStr, "PD server timeout") ||
		strings.Contains(errStr, "Information schema is out of date") {
		return true
	}
	return false
}
