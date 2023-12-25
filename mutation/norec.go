package mutation

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

type NoRecRewriter struct {
	pred []ast.Node
}

func (nr NoRecRewriter) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	return n, false
}

func detectAgg(sel *ast.SelectStmt) bool {
	if sel.GroupBy != nil {
		return true
	}
	for _, f := range sel.Fields.Fields {
		if f.WildCard != nil {
			continue
		}
		if ast.HasAggFlag(f.Expr) {
			return true
		}
	}
	if sel.Having != nil {
		if ast.HasAggFlag(sel.Having.Expr) {
			return true
		}
	}
	if sel.OrderBy != nil {
		for _, item := range sel.OrderBy.Items {
			if ast.HasAggFlag(item.Expr) {
				return true
			}
		}
	}
	return false
}

func (nr NoRecRewriter) Leave(n ast.Node) (retNode ast.Node, ok bool) {
	switch v := n.(type) {
	case *ast.SelectStmt:
		hasAgg := detectAgg(v)
		if !hasAgg {
			whereNode := v.Where
			if whereNode == nil {
				return nil, true
			}
			pExpr := &ast.ParenthesesExpr{Expr: whereNode}
			newExpr := ast.IsTruthExpr{Expr: pExpr, Not: false, True: 1}
			v.Fields = &ast.FieldList{
				Fields: []*ast.SelectField{
					{
						Expr: &newExpr,
					},
				},
			}
			v.Where = nil
			v.OrderBy = nil
		} else {
			if v.Having == nil {
				return nil, true
			}
			havingExpr := v.Having.Expr
			pExpr := &ast.ParenthesesExpr{Expr: havingExpr}
			newExpr := ast.IsTruthExpr{Expr: pExpr, Not: false, True: 1}
			sf := make([]*ast.SelectField, 0)
			sf = append(sf, &ast.SelectField{
				Expr: &newExpr,
			})
			sf = append(sf, v.Fields.Fields...)
			v.Fields.Fields = sf
			v.Having = nil
			v.OrderBy = nil
		}
	}
	return n, true
}
