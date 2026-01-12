package engine

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"

	"github.com/PingCAP-QE/schrddl/ddl"
)

func PrepareEnv(dbAddr string, dbName string, enableIndexJoinOnAggregation bool) {
	dbURL := fmt.Sprintf("root:@tcp(%s)/%s", dbAddr, dbName)
	tiDB, err := sql.Open("mysql", dbURL)
	if err != nil {
		log.Fatalf("Can't open database, err: %s", err.Error())
	}
	defer func() {
		_ = tiDB.Close()
	}()

	tiDBConn, err := tiDB.Conn(context.Background())
	if err != nil {
		log.Fatalf("Can't connect to database, err: %s", err.Error())
	}
	defer func() {
		_ = tiDBConn.Close()
	}()

	if _, err = tiDBConn.ExecContext(context.Background(), fmt.Sprintf("set global time_zone='%s'", ddl.Local.String())); err != nil {
		if _, err = tiDBConn.ExecContext(context.Background(), fmt.Sprintf("set global time_zone='%s'", time.Local.String())); err != nil {
			if _, err = tiDBConn.ExecContext(context.Background(), "set global time_zone='+8:00'"); err != nil {
				log.Fatalf("Can't set time_zone for tidb, please check tidb env")
			}
		}
	}

	if enableIndexJoinOnAggregation {
		_, _ = tiDBConn.ExecContext(context.Background(), "set GLOBAL tidb_enable_inl_join_inner_multi_pattern='ON'")
	}

	mysql.SetLogger(log.Logger())
}

func TimeoutExitLoop(timeout time.Duration) {
	time.Sleep(timeout + 20*time.Second)
	os.Exit(0)
}
