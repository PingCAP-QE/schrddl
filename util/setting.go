package util

import (
	"context"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// GlobalSetting reports whether a statement changes cluster-wide settings, and
// names what it changes. Only statements that look like SET are parsed. A SET
// that is not a plain session-scoped assignment counts as cluster-wide as
// well and is named by its text, so the guard fails closed on anything it
// cannot read.
func GlobalSetting(statement string) (string, bool) {
	head := strings.TrimSpace(statement)
	if len(head) < 4 || !strings.EqualFold(head[:3], "set") || isNameByte(head[3]) {
		return "", false
	}
	nodes, _, err := parser.New().Parse(statement, "", "")
	if err != nil || len(nodes) != 1 {
		return head, true
	}
	set, ok := nodes[0].(*ast.SetStmt)
	if !ok {
		return head, true
	}
	names := make([]string, 0, len(set.Variables))
	global := false
	for _, variable := range set.Variables {
		names = append(names, variable.Name)
		global = global || variable.IsGlobal
	}
	return strings.Join(names, ","), global
}

func isNameByte(b byte) bool {
	return b == '_' || b == '$' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b >= 0x80
}

// ExecSetting runs a generated SET statement. A session-scoped setting runs as
// generated. A cluster-wide one is a privileged change: it only reaches the
// database through ApplyGlobalTuning, and its outcome is only logged.
func ExecSetting(ctx context.Context, db Executor, statement string) error {
	setting, global := GlobalSetting(statement)
	if global {
		ApplyGlobalTuning(db, statement, setting)
		return nil
	}
	_, err := db.ExecContext(ctx, statement)
	return err
}
