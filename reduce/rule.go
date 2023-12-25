package reduce

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"math/rand"
)

type rule interface {
	apply(node *ast.StmtNode) bool
}

var selectStmtRule = []rule{
	&removeWhereFromSelectRule{},
	&simpleWhereFromSelectRule{},
	&removeForUpdateRule{},
	&removeOrderByRule{},
	&removeHintRule{},
	&removeSelectField{},
}

type removeWhereFromSelectRule struct {
}

func (r *removeWhereFromSelectRule) apply(node *ast.StmtNode) bool {
	sel := (*node).(*ast.SelectStmt)
	if sel.Where == nil {
		return false
	}
	sel.Where = nil
	return false
}

type simpleWhereFromSelectRule struct {
}

func (r *simpleWhereFromSelectRule) apply(node *ast.StmtNode) bool {
	sel := (*node).(*ast.SelectStmt)
	if sel.Where == nil {
		return false
	}
	return simpleExprRule(&sel.Where)
}

func simpleExprRule(node *ast.ExprNode) bool {
	switch v := (*node).(type) {
	case *ast.BinaryOperationExpr:
		l := rand.Intn(2) == 0
		if l {
			exprNode := v.L.(ast.ExprNode)
			// Recursively apply the rule.
			if _, ok := exprNode.(*ast.BinaryOperationExpr); ok && rand.Intn(2) == 0 {
				return simpleExprRule(&exprNode)
			}
			node = &exprNode
			return true
		} else {
			exprNode := v.R.(ast.ExprNode)
			// Recursively apply the rule.
			if _, ok := exprNode.(*ast.BinaryOperationExpr); ok && rand.Intn(2) == 0 {
				return simpleExprRule(&exprNode)
			}
			node = &exprNode
			return true
		}
	default:
		return false
	}
}

type removeForUpdateRule struct {
}

func (r *removeForUpdateRule) apply(node *ast.StmtNode) bool {
	sel := (*node).(*ast.SelectStmt)
	if sel.LockInfo == nil {
		return false
	}
	sel.LockInfo = nil
	return false
}

type removeHintRule struct {
}

func (r *removeHintRule) apply(node *ast.StmtNode) bool {
	sel := (*node).(*ast.SelectStmt)
	if sel.TableHints == nil {
		return false
	}

	randIdx := rand.Intn(len(sel.TableHints))
	sel.TableHints = append(sel.TableHints[:randIdx], sel.TableHints[randIdx+1:]...)

	if len(sel.TableHints) == 0 {
		return false
	}
	return true
}

type removeOrderByRule struct {
}

func (r *removeOrderByRule) apply(node *ast.StmtNode) bool {
	sel := (*node).(*ast.SelectStmt)
	if sel.OrderBy == nil {
		return false
	}
	sel.OrderBy = nil
	return false
}

type removeSelectField struct {
}

func (r *removeSelectField) apply(node *ast.StmtNode) bool {
	sel := (*node).(*ast.SelectStmt)
	if len(sel.Fields.Fields) > 1 {
		sel.Fields.Fields = sel.Fields.Fields[:len(sel.Fields.Fields)-1]
		return true
	}
	return false
}
