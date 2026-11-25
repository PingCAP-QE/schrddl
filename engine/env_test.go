package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/PingCAP-QE/schrddl/ddl"
	"github.com/stretchr/testify/assert"
)

// zoneExecutor accepts only the statements it is told to, like a cluster that
// knows some time zone names and rejects the others.
type zoneExecutor struct {
	statements []string
	accepted   map[string]bool
}

func (e *zoneExecutor) ExecContext(_ context.Context, query string, _ ...interface{}) (sql.Result, error) {
	e.statements = append(e.statements, query)
	if !e.accepted[query] {
		return nil, errors.New("Unknown or incorrect time zone")
	}
	return nil, nil
}

func clusterTimeZoneStatement(zone string) string {
	return fmt.Sprintf("set global time_zone='%s'", zone)
}

// clusterTimeZoneStatements are the changes tried for the zones, in order.
func clusterTimeZoneStatements(zones ...string) []string {
	statements := make([]string, 0, len(zones))
	for _, zone := range zones {
		statements = append(statements, clusterTimeZoneStatement(zone))
	}
	return statements
}

func TestClusterTimeZonesStartWithTheRunnerAndEndWithAFixedOffset(t *testing.T) {
	zones := clusterTimeZones()
	unique := map[string]bool{}
	for _, zone := range zones {
		unique[zone] = true
	}
	assert.Equal(t, ddl.Local.String(), zones[0], "the runner's own time zone must be tried first")
	assert.Equal(t, "+8:00", zones[len(zones)-1], "a fixed offset must be the last resort")
	assert.Len(t, unique, len(zones), "no time zone may be tried twice")
}

func TestClusterTimeZoneIsOnlyChangedWithTheOptIn(t *testing.T) {
	zones := clusterTimeZones()

	t.Setenv("SCHRDDL_ALLOW_GLOBAL_TUNING", "")
	held := &zoneExecutor{}
	setClusterTimeZone(held)
	assert.Empty(t, held.statements, "the cluster time zone was changed without the opt-in")

	t.Setenv("SCHRDDL_ALLOW_GLOBAL_TUNING", "true")
	// Every earlier name is rejected: the search stops at the last resort.
	fallback := &zoneExecutor{accepted: map[string]bool{clusterTimeZoneStatement(zones[len(zones)-1]): true}}
	setClusterTimeZone(fallback)
	assert.Equal(t, clusterTimeZoneStatements(zones...), fallback.statements)

	// The first name the cluster knows ends the search.
	known := &zoneExecutor{accepted: map[string]bool{clusterTimeZoneStatement(zones[0]): true}}
	setClusterTimeZone(known)
	assert.Equal(t, clusterTimeZoneStatements(zones[0]), known.statements)

	// A cluster that rejects every candidate is reported, never fatal.
	none := &zoneExecutor{}
	setClusterTimeZone(none)
	assert.Equal(t, clusterTimeZoneStatements(zones...), none.statements)
}
