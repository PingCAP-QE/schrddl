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

// DML errors that should be ignored during testing
var dmlIgnorePatterns = []string{
	// Default value related
	"can't have a default value",
	"doesn't have a default value",

	// Data type and casting related
	"strconv.Atoi",
	"cannot cast from bigint to vector",
	"cannot cast",
	"Invalid vector text",
	"does not fit",
	"Data Truncated",
	"Data truncated",
	"Data truncated for column",
	"Data too long for column",
	"Data Too Long",
	"Truncated incorrect",
	"cannot convert datum from",
	"Cannot convert string",
	"converting driver.Value type",
	"interface conversion",

	// Value range and validation related
	"Out of range value for column",
	"value is out of range in",
	"overflows",
	"Bad Number",
	"invalid year",
	"Incorrect",
	"cannot be null",

	// Character encoding related
	"Invalid utf8mb4 character",

	// Connection and network related
	"context canceled",
	"bad connection",
	"invalid connection",
	"connection is already closed",
	"epoch_not_match",
	"requested pd is not leader of cluster",
	"batchScanRegion from PD failed",
	"PD server timeout",
	"try again later",

	// Column and table related
	"doesn't exist",
	"column is deleted",
	"Can't find column",
	"Unknown column",
	"column specified twice",
	"specified twice",
	"column has index reference",
	"no rows in result set",

	// Auto increment related
	"Failed to read auto-increment value from storage engine",

	// Concurrency related
	"Duplicate entry",
	"PessimisticLockNotFound",

	// SQL syntax and structure related
	"Column count doesn't match value count",
	"sql_mode=only_full_group_by",
	"should contain a UNION",
	"have different column counts",
	"have a different number of columns",
	"followed by one or more recursive ones",
	"Not unique table/alias",

	// Value formatting related
	"Percentage value",
	"Index column",
	"Illegal mix of collations",

	// System and runtime related
	"Out Of Memory",
	"invalid syntax",
	"slice bounds out of range",
	"Split table region lower value count",
	"newer than query schema version",
	"Information schema is out of date",
	"Your query has been cancelled due to exceeding the allowed memory limit for a single SQL query",
	"Unsupported multi schema change",

	// Known bugs
	"unsupport column type for encode 225",
	"decode date time",
	"Unexpected ExprType String and EvalType Enum",

	// JSON related errors
	"cannot be pushed down",
	"document root",
	"Illegal Json text",
}

func dmlIgnoreError(err error) bool {
	if err == nil {
		return true
	}

	errStr := err.Error()

	for _, pattern := range dmlIgnorePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	if !RCIsolation {
		if strings.Contains(errStr, "Information schema is changed") || strings.Contains(errStr, "public column") {
			return true
		}
	}

	return false
}

// DDL errors that should be ignored during testing
var ddlIgnorePatterns = []string{
	// MV index related
	"has an expression index dependency",

	// Vector index related
	"Unsupported add vector index",
	"can only be defined on fixed-dimension vector columns",
	"with Vector Key covered",

	// Key and index related
	"Specified key was too long",
	"Too many keys specified",
	"used in key specification without a key length",
	"A UNIQUE INDEX must include all columns in the table's partitioning function",
	"A CLUSTERED INDEX must include all columns in the table's partitioning function",
	"Unsupported Global Index",
	"found index conflict records",
	"column has index reference",
	"with composite index covered or Primary Key covered now",

	// Data type and value related
	"Incorrect date value",
	"Incorrect time value",
	"Incorrect datetime value",
	"Incorrect timestamp value",
	"Invalid year value",
	"Invalid default value",
	"can't have a literal default",
	"can't have a default value",
	"doesn't have a default value",
	"overflows",
	"value is out of range",
	"Out of range value for column",
	"Data truncated",
	"Data too long for column",
	"Data Too Long",
	"Bad Number",
	"Truncated incorrect",

	// Column and table related
	"already exist on column",
	"table doesn't exist",
	"doesn't exist",
	"does not exist",
	"is not exists",
	"column does not exist",
	"column is deleted",
	"Can't find column",
	"Unknown table",
	"Unknown column",
	"Duplicate column name",
	"Duplicate entry",
	"can't drop only column",
	"not found",
	"column specified twice",

	// Schema and DDL related
	"Information schema is changed",
	"Information schema is out of date",
	"Cancelled DDL job",
	"admin show ddl jobs len != len(tasks)",
	"with tidb_enable_change_multi_schema is disable",
	"does not exist, this column may have been updated by other DDL",

	// Unsupported operations
	"Unsupported",
	"Unsupported modify column",
	"Unsupported modify charset from",
	"Unsupported modifying collation of column",
	"Unsupported shard_row_id_bits for table with primary key as row id",
	"Unsupported ALTER TiFlash settings",

	// Partitioning related
	"not allowed type for this type of partitioning",
	"since the unique index is not including all partitioning columns, and GLOBAL is not given as IndexOption",
	"has a partitioning function dependency and cannot be dropped or renamed",

	// Primary key related
	"All parts of a PRIMARY KEY must be NOT NULL",

	// Connection and system related
	"context canceled",
	"cancelled by user",
	"invalid connection",
	"PD server timeout",
	"should be less than the total tiflash server count",

	// Conversion related
	"cannot convert",
	"cannot convert datum",
	"converting driver.Value type",

	// Testing related
	"error msg: injected random error, caller: github.com/pingcap/tidb/pkg/disttask/framework/storage.(*TaskManager).WithNewSession",
	"check that column/key exists",
	"no rows in result set",
	"does not fit",
}

func ddlIgnoreError(err error) bool {
	if err == nil {
		return true
	}

	errStr := err.Error()
	log.Warnf("check DDL err:%s", errStr)

	// Check against predefined patterns
	for _, pattern := range ddlIgnorePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	// Special regex check for auto ID overflow
	if match, _ := regexp.MatchString(`cause next global auto ID( \d+ | )overflow`, errStr); match {
		return true
	}

	return false
}
