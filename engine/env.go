package engine

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"

	"github.com/PingCAP-QE/schrddl/dbconfig"
	"github.com/PingCAP-QE/schrddl/ddl"
	"github.com/PingCAP-QE/schrddl/util"
)

func PrepareEnv(dbAddr string, dbName string, enableIndexJoinOnAggregation bool) {
	tiDB, err := dbconfig.Open(dbAddr, dbName)
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

	// Cluster-wide settings are privileged changes: each is applied only
	// through the guard SCHRDDL_ALLOW_GLOBAL_TUNING controls.
	setClusterTimeZone(tiDBConn)
	if enableIndexJoinOnAggregation {
		util.ApplyGlobalTuning(tiDBConn, "set global tidb_enable_inl_join_inner_multi_pattern='ON'",
			"tidb_enable_inl_join_inner_multi_pattern")
	}

	mysql.SetLogger(log.Logger())
}

// setClusterTimeZone tries the runner's own time zone names before falling
// back to a fixed offset. Without the opt-in, or when none can be applied,
// timestamp-related statements use the cluster default.
func setClusterTimeZone(db util.Executor) {
	for _, zone := range clusterTimeZones() {
		if util.ApplyGlobalTuning(db, fmt.Sprintf("set global time_zone='%s'", zone), "time_zone") {
			return
		}
	}
	log.Warnf("Can't set time_zone for tidb, please check tidb env")
}

// clusterTimeZones lists the time zones to try, the runner's own first.
func clusterTimeZones() []string {
	zones := []string{ddl.Local.String()}
	if local := time.Local.String(); local != zones[0] {
		zones = append(zones, local)
	}
	return append(zones, "+8:00")
}

func TimeoutExitLoop(timeout time.Duration) {
	time.Sleep(timeout + 20*time.Second)
	os.Exit(0)
}
