package dump

import (
	"github.com/PingCAP-QE/schrddl/util"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

type tableNameExtractor struct {
	names    map[string]struct{}
	cteNames map[string]struct{}
	err      error
}

func ExtraFromSQL(sql string) ([]string, error) {
	ast, err := util.SQL2Ast(sql)
	if err != nil {
		return nil, err
	}
	tne := &tableNameExtractor{
		names:    make(map[string]struct{}),
		cteNames: make(map[string]struct{}),
	}
	ast.Accept(tne)
	return tne.getTablesAndViews(), nil
}

func (tne *tableNameExtractor) getTablesAndViews() []string {
	r := make(map[string]struct{})
	for tablePair := range tne.names {
		// remove cte in table names
		_, ok := tne.cteNames[tablePair]
		if !ok {
			r[tablePair] = struct{}{}
		}
	}
	rs := make([]string, 0, len(r))
	for k := range r {
		rs = append(rs, k)
	}
	return rs
}

func (tne *tableNameExtractor) Enter(in ast.Node) (ast.Node, bool) {
	if _, ok := in.(*ast.TableName); ok {
		return in, true
	}
	return in, false
}

func (tne *tableNameExtractor) Leave(in ast.Node) (ast.Node, bool) {
	if tne.err != nil {
		return in, true
	}
	if t, ok := in.(*ast.TableName); ok {
		tne.names[t.Name.String()] = struct{}{}
	} else if s, ok := in.(*ast.SelectStmt); ok {
		if s.With != nil && len(s.With.CTEs) > 0 {
			for _, cte := range s.With.CTEs {
				tne.cteNames[cte.Name.L] = struct{}{}
			}
		}
	}
	return in, true
}
