package norec

import (
	"github.com/PingCAP-QE/schrddl/util"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

type NoRecRewriter struct {
	pred      []ast.Node
	invalid   bool
	ctes      []string
	insideCte bool
	isAgg     bool
}

func (nr *NoRecRewriter) Reset() {
	nr.invalid = false
	nr.ctes = make([]string, 0)
	nr.insideCte = false
	nr.isAgg = false
}

func (nr *NoRecRewriter) Valid() bool {
	return !nr.invalid
}

func (nr *NoRecRewriter) IsAgg() bool {
	return nr.isAgg
}

func (nr *NoRecRewriter) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch v := n.(type) {
	case *ast.WithClause:
		for _, cte := range v.CTEs {
			nr.ctes = append(nr.ctes, cte.Name.String())
		}
		nr.insideCte = true
	}
	return n, false
}

func (nr *NoRecRewriter) Leave(n ast.Node) (retNode ast.Node, ok bool) {
	switch v := n.(type) {
	case *ast.WithClause:
		nr.insideCte = false
	case *ast.SelectStmt:
		if nr.insideCte {
			// Do not rewrite CTE
			return n, true
		}
		hasAgg := util.DetectAgg(v)
		nr.isAgg = hasAgg
		if !hasAgg {
			whereNode := v.Where
			if whereNode == nil {
				nr.invalid = true
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
				nr.invalid = true
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
