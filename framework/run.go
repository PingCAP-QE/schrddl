package framework

import (
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ngaut/log"
	"golang.org/x/net/context"
)

var GlobalOutPut = ""
var EnableTransactionTest = false
var Prepare = false
var CheckDDLExtraTimeout = 0 * time.Second
var EnableApproximateQuerySynthesis = false
var EnableCERT = false
var EnableTLP = false
var TestFail = false

func OpenDB(dsn string, maxIdleConns int) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxIdleConns(maxIdleConns)
	log.Info("DB opens successfully")
	return db, nil
}

func createDBs(dbDSN string) []*sql.DB {
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
	return dbs
}

func createDBsForPrepare(dbAddr, dbName string) []*sql.DB {
	dbs := make([]*sql.DB, 0, 2)
	db0, err := OpenDB(fmt.Sprintf("root:@tcp(%s)/%s", dbAddr, dbName), 20)
	if err != nil {
		log.Fatalf("[ddl] create db client error %v", err)
	}
	dbcache, err := OpenDB(fmt.Sprintf("root:@tcp(%s)/%s", dbAddr, "testcache"), 20)
	if err != nil {
		log.Fatalf("[ddl] create db client error %v", err)
	}
	dbcache.SetMaxOpenConns(32)
	dbcache.SetMaxIdleConns(32)
	dbcache.SetConnMaxLifetime(time.Hour)
	dbs = append(dbs, db0)
	dbs = append(dbs, dbcache)
	return dbs
}

func Run(cfg CaseConfig, testTime time.Duration) {
	wrapCtx := context.WithCancel
	if testTime > 0 {
		wrapCtx = func(ctx context.Context) (context.Context, context.CancelFunc) {
			return context.WithTimeout(ctx, testTime)
		}
	}
	ctx, cancel := wrapCtx(context.Background())
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

	dbss := make([][]*sql.DB, 0, cfg.Concurrency)
	dbDSN := fmt.Sprintf("root:@tcp(%s)/%s", cfg.DBAddr, cfg.DBName)
	for i := 0; i < cfg.Concurrency; i++ {
		// Currently, we only use one testCase for plan cache test.
		if cfg.TestPrepare && i == 0 {
			dbss = append(dbss, createDBsForPrepare(cfg.DBAddr, cfg.DBName))
		} else {
			dbss = append(dbss, createDBs(dbDSN))
		}
	}

	ddl := NewDDLCase(&cfg)
	if err := ddl.Initialize(ctx, dbss, cfg.DBName); err != nil {
		log.Fatalf("[ddl] initialze error %v", err)
	}
	if err := ddl.Execute(ctx); err != nil {
		log.Fatalf("[ddl] execute error %v", err)
	}
}
