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
	} else {
		version := ""
		err := dbss[0][0].QueryRow("select tidb_version()").Scan(&version)
		if err != nil {
			log.Fatalf("[ddl] get tidb version error %v", err)
		}
		if !strings.Contains(version, "Generation") {
			dbss[0][0].Exec("set global tidb_cloud_storage_uri=''")
		}
	}
	dbss[0][0].Exec("set global max_execution_time=5000")

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
	//"assertion failed",
	"cannot cast from bigint to vector",
	"does not fit",
	"Invalid vector text",
	"cannot cast",
	"Data Truncated",
	"context canceled",
	"epoch_not_match",
	"Invalid utf8mb4 character",
	"requested pd is not leader of cluster",

	// bug
	"unsupport column type for encode 225",
	"decode date time",
	"Unexpected ExprType String and EvalType Enum",

	"batchScanRegion from PD failed",
	"PessimisticLockNotFound",
}

var dmlCheckLists = []string{
	"slice bounds out of range",
	"bad connection",
	"try again later",
	"invalid connection",
	"Unsupported multi schema change",

	// Sometimes, there might be duplicated entry error caused by concurrent dml.
	// So we ignore here.
	"Duplicate entry",

	// Sometimes, a insert to a table might generate an error caused by exceeding maximum auto increment id,
	// we ignore this error here.
	"Failed to read auto-increment value from storage engine",

	"doesn't exist",
	"column is deleted",
	"Can't find column",
	"converting driver.Value type",
	"column specified twice",
	"Out of range value for column",
	"Unknown column",
	"column has index reference",
	"Data too long for column",
	"Data truncated",
	"no rows in result set",
	"Truncated incorrect",
	"Data truncated for column",
	"Incorrect", // eg: For Incorrect tinyint value, Incorrect data value...
	"overflows", // eg: For constant 20030522161944 overflows tinyint
	"Bad Number",
	"invalid year",
	"value is out of range in",
	"Data Too Long",
	"doesn't have a default value",
	"specified twice",
	"cannot convert datum from",
	"sql_mode=only_full_group_by",
	"cannot be null",
	"Column count doesn't match value count",
	"Percentage value",
	"Index column",
	"Illegal mix of collations",
	"Cannot convert string",
	"interface conversion",
	"connection is already closed",
	"should contain a UNION",
	"have different column counts",
	"followed by one or more recursive ones",
	"Not unique table/alias",
	"have a different number of columns",
	"Split table region lower value count",
	"Out Of Memory",
	"invalid syntax",
	"newer than query schema version",
	"PD server timeout",
	"Information schema is out of date",
	"Your query has been cancelled due to exceeding the allowed memory limit for a single SQL query",

	// JSON related errors
	"cannot be pushed down",
}

func dmlIgnoreError(err error) bool {
	if err == nil {
		return true
	}

	errStr := err.Error()
	for _, check := range checkList {
		if strings.Contains(errStr, check) {
			return true
		}
	}

	for _, check := range dmlCheckLists {
		if strings.Contains(errStr, check) {
			return true
		}
	}

	if !RCIsolation {
		if strings.Contains(errStr, "Information schema is changed") ||
			strings.Contains(errStr, "public column") {
			return true
		}
	}

	return false
}

var ddlIgnoreList = []string{
	"Specified key was too long",
	"Too many keys specified",
	"Incorrect date value",
	"can't have a literal default",
	"Unsupported add vector index",
	"does not fit",
	"can only be defined on fixed-dimension vector columns",
	"with Vector Key covered",
	"already exist on column",
	"Unsupported",
	"context canceled",
	"cancelled by user",
	"error msg: injected random error, caller: github.com/pingcap/tidb/pkg/disttask/framework/storage.(*TaskManager).WithNewSession",
	"found index conflict records",
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
		strings.Contains(errStr, "since the unique index is not including all partitioning columns, and GLOBAL is not given as IndexOption") ||
		strings.Contains(errStr, "A UNIQUE INDEX must include all columns in the table's partitioning function") ||
		strings.Contains(errStr, "Unsupported Global Index") ||
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
