package framework

import (
	"fmt"
	"testing"
)

func TestSimplifyQueryPlan(t *testing.T) {
	plan := []string{
		" Projection                          ",
		" └─Projection                        ",
		"   └─HashJoin                        ",
		"     ├─PartitionUnion(Build)         ",
		"     │ ├─Projection                  ",
		"     │ │ └─TableReader               ",
		"     │ │   └─Selection               ",
		"     │ │     └─TableFullScan         ",
		"     │ ├─Projection                  ",
		"     │ │ └─TableReader               ",
		"     │ │   └─Selection               ",
		"     │ │     └─TableFullScan         ",
		"     │ ├─Projection                  ",
		"     │ │ └─TableReader               ",
		"     │ │   └─Selection               ",
		"     │ │     └─TableFullScan         ",
		"     │ ├─Projection                  ",
		"     │ │ └─TableReader               ",
		"     │ │   └─Selection               ",
		"     │ │     └─TableFullScan         ",
		"     │ ├─Projection                  ",
		"     │ │ └─TableReader               ",
		"     │ │   └─Selection               ",
		"     │ │     └─TableFullScan         ",
		"     │ └─Projection                  ",
		"     │   └─TableReader               ",
		"     │     └─Selection               ",
		"     │       └─TableFullScan         ",
		"     └─PartitionUnion(Probe)         ",
		"       ├─TableReader                 ",
		"       │ └─TableFullScan             ",
		"       ├─TableReader                 ",
		"       │ └─TableFullScan             ",
		"       └─TableReader                 ",
		"         └─TableFullScan             "}
	res := simplifyQueryPlan(plan)
	fmt.Println(res)
}
