package tlp

import (
	"github.com/PingCAP-QE/schrddl/util"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
)

type TlpRewriter struct {
	pred           []ast.Node
	invalid        bool
	ctes           []string
	insideCte      bool
	insideSubQuery bool
	isAgg          bool

	// config
	negativePred bool
	noPred       bool
}

func (tr *TlpRewriter) Reset() {
	tr.invalid = false
	tr.ctes = make([]string, 0)
	tr.insideCte = false
	tr.insideSubQuery = false
	tr.isAgg = false
	tr.negativePred = true
	tr.noPred = false
}

func (tr *TlpRewriter) SetGenerateIsNull() {
	tr.negativePred = false
	tr.noPred = false
}

func (tr *TlpRewriter) SetGenerateAll() {
	tr.noPred = true
}

func (tr *TlpRewriter) Valid() bool {
	return !tr.invalid
}

func (tr *TlpRewriter) IsAgg() bool {
	return tr.isAgg
}

func (tr *TlpRewriter) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch v := n.(type) {
	case *ast.SubqueryExpr:
		tr.insideSubQuery = true
	case *ast.TableSource:
		switch v.Source.(type) {
		case *ast.SelectStmt, *ast.SetOprStmt:
			tr.insideSubQuery = true
		}
	case *ast.WithClause:
		for _, cte := range v.CTEs {
			tr.ctes = append(tr.ctes, cte.Name.String())
		}
		tr.insideCte = true
	}
	return n, false
}

func (tr *TlpRewriter) Leave(n ast.Node) (retNode ast.Node, ok bool) {
	switch v := n.(type) {
	case *ast.SubqueryExpr:
		tr.insideSubQuery = false
	case *ast.TableSource:
		switch v.Source.(type) {
		case *ast.SelectStmt, *ast.SetOprStmt:
			tr.insideSubQuery = false
		}
	case *ast.WithClause:
		tr.insideCte = false
	case *ast.SelectStmt:
		if tr.insideCte {
			// Do not rewrite CTE
			return n, true
		}
		if tr.insideSubQuery {
			// Do not rewrite subquery
			return n, true
		}
		hasAgg := util.DetectAgg(v)
		tr.isAgg = hasAgg
		if !hasAgg {
			whereNode := v.Where
			if whereNode == nil {
				tr.invalid = true
				return n, true
			}
			if tr.noPred {
				v.Where = nil
			} else if tr.negativePred {
				pExpr := &ast.ParenthesesExpr{Expr: whereNode}
				np := &ast.UnaryOperationExpr{Op: opcode.Not, V: pExpr}
				v.Where = np
			} else {
				pExpr := &ast.ParenthesesExpr{Expr: whereNode}
				isnullP := &ast.IsNullExpr{Expr: pExpr}
				v.Where = isnullP
			}
		} else {
			if v.Having == nil {
				tr.invalid = true
				return n, true
			}
			havingExpr := v.Having.Expr
			if tr.noPred {
				v.Having = nil
			} else if tr.negativePred {
				pExpr := &ast.ParenthesesExpr{Expr: havingExpr}
				np := &ast.UnaryOperationExpr{Op: opcode.Not, V: pExpr}
				v.Having.Expr = np
			} else {
				pExpr := &ast.ParenthesesExpr{Expr: havingExpr}
				isnullP := &ast.IsNullExpr{Expr: pExpr}
				v.Having.Expr = isnullP
			}
		}
	}
	return n, true
}
