package util

import (
	"github.com/ngaut/log"
	"go.uber.org/zap"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
)

func SQL2Ast(sql string) (ast.StmtNode, error) {
	// TODO: use parser pool.
	tidbParser := parser.New()
	stmts, _, err := tidbParser.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}
	return stmts[0], nil
}

func Ast2SQL(n ast.StmtNode) (string, error) {
	var sb strings.Builder
	err := n.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutDefaultCharset|format.RestoreStringWithoutCharset, &sb))
	if err != nil {
		return "", err
	}
	return sb.String(), nil
}

func CloneAst(n ast.StmtNode) ast.StmtNode {
	sql, err := Ast2SQL(n)
	if err != nil {
		log.Fatal("Ast2SQL failed", zap.Error(err))
	}
	n, err = SQL2Ast(sql)
	if err != nil {
		log.Fatal("SQL2Ast failed", zap.Error(err))
	}
	return n
}
