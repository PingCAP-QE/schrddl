package tlp

import (
	"github.com/PingCAP-QE/schrddl/util"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

type TlpRewriter struct {
	pred      []ast.Node
	invalid   bool
	ctes      []string
	insideCte bool
	isAgg     bool

	// config
	
}

func (tr *TlpRewriter) Reset() {
	tr.invalid = false
	tr.ctes = make([]string, 0)
	tr.insideCte = false
	tr.isAgg = false
}

func (tr *TlpRewriter) Valid() bool {
	return !tr.invalid
}

func (tr *TlpRewriter) IsAgg() bool {
	return tr.isAgg
}

func (tr *TlpRewriter) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch v := n.(type) {
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
	case *ast.WithClause:
		tr.insideCte = false
	case *ast.SelectStmt:
		if tr.insideCte {
			// Do not rewrite CTE
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
			pExpr := &ast.ParenthesesExpr{Expr: whereNode}
			newExpr := ast.IsTruthExpr{Expr: pExpr, Not: false, True: 1}
			sumExpr := ast.AggregateFuncExpr{F: ast.AggFuncSum, Args: []ast.ExprNode{&newExpr}}
			v.Fields = &ast.FieldList{
				Fields: []*ast.SelectField{
					{
						Expr: &sumExpr,
					},
				},
			}
			v.Where = nil
			v.OrderBy = nil
		} else {
			if v.Having == nil {
				tr.invalid = true
				return n, true
			}
			havingExpr := v.Having.Expr
			pExpr := &ast.ParenthesesExpr{Expr: havingExpr}
			newExpr := ast.IsTruthExpr{Expr: pExpr, Not: false, True: 1}
			sumExpr := ast.AggregateFuncExpr{F: ast.AggFuncSum, Args: []ast.ExprNode{&newExpr}}
			sf := make([]*ast.SelectField, 0)
			sf = append(sf, &ast.SelectField{
				Expr: &sumExpr,
			})
			sf = append(sf, v.Fields.Fields...)
			v.Fields.Fields = sf
			v.Having = nil
			v.OrderBy = nil
		}
	}
	return n, true
}
