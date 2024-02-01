package framework

import (
	"strings"
	"unicode/utf8"
)

func simplifyQueryPlan(s []string) *planDetail {
	// state
	enterPartitionUnion := false
	canPrintInPartitionUnion := false
	idxRuneForPartitionUnion := 0

	rs := &planDetail{}

	simpleRow := func(s string) string {
		if strings.Contains(s, "PartitionUnion") {
			enterPartitionUnion = true
			canPrintInPartitionUnion = false
			idxRuneForPartitionUnion = utf8.RuneCountInString(s[:strings.Index(s, "PartitionUnion")])
		}
		if enterPartitionUnion && !canPrintInPartitionUnion {
			a := string([]rune(s)[idxRuneForPartitionUnion:])
			if strings.HasPrefix(a, "└─") {
				canPrintInPartitionUnion = true
			}
		}
		if enterPartitionUnion && !canPrintInPartitionUnion && !strings.Contains(s, "PartitionUnion") {
			return ""
		}

		simplePlan := strings.ReplaceAll(s, "├─", "")
		simplePlan = strings.ReplaceAll(simplePlan, "└─", "")
		simplePlan = strings.ReplaceAll(simplePlan, "│", "")
		simplePlan = strings.ReplaceAll(simplePlan, "'", "")
		simplePlan = strings.TrimSpace(simplePlan)
		simplePlan += ";"
		return simplePlan
	}
	totalPlan := ""
	for _, r := range s {
		totalPlan += simpleRow(r)
	}
	rs.plan = totalPlan
	return rs
}

type planDetail struct {
	plan       string
	useMvIndex bool
}

func (c *testCase) getQueryPlan(query string) (*planDetail, error) {
	planQuery := "explain format=brief " + query
	rs, err := c.execQuery(planQuery)
	if err != nil {
		return nil, err
	}
	useMvIndex := false
	ss := make([]string, 0, len(rs))
	for _, r := range rs {
		ss = append(ss, r[0])
		if strings.Contains(r[3], "array") {
			useMvIndex = true
		}
	}
	pd := simplifyQueryPlan(ss)
	pd.useMvIndex = useMvIndex
	return pd, err
}
