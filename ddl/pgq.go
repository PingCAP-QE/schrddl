package ddl

import (
	"strings"
	"unicode/utf8"
)

func simplifyQueryPlan(s []string) string {
	// state
	enterPartitionUnion := false
	canPrintInPartitionUnion := false
	idxRuneForPartitionUnion := 0

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
	return totalPlan
}

func (c *testCase) getQueryPlan(query string) (string, error) {
	planQuery := "explain format=brief " + query
	rs, err := c.execQuery(planQuery)
	if err != nil {
		return "", err
	}
	ss := make([]string, 0, len(rs))
	for _, r := range rs {
		ss = append(ss, r[0])
	}
	return simplifyQueryPlan(ss), nil
}
