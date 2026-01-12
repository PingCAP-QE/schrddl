package eet

import (
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/PingCAP-QE/schrddl/sqlgenerator"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
)

// Build an expr from input node that are always true/false
func buildIdentityExpr(n ast.Node, isTrue bool) ast.ExprNode {
	innerOp := opcode.LogicAnd
	if isTrue {
		innerOp = opcode.LogicOr
	}

	expr := &ast.BinaryOperationExpr{
		Op: innerOp,
		L: &ast.BinaryOperationExpr{
			Op: innerOp,
			L:  deepCopy(n),
			R: &ast.UnaryOperationExpr{
				Op: opcode.Not,
				V:  deepCopy(n),
			},
		},
		R: &ast.IsNullExpr{Expr: deepCopy(n), Not: !isTrue},
	}

	return &ast.ParenthesesExpr{
		Expr: expr,
	}
}

// Copied from pkg/planner/indexadvisor/utils.go
type nodeVisitor struct {
	enter func(n ast.Node) (skip bool)
	leave func(n ast.Node) (ok bool)
}

func (v *nodeVisitor) Enter(n ast.Node) (out ast.Node, skipChildren bool) {
	return n, v.enter(n)
}

func (v *nodeVisitor) Leave(n ast.Node) (out ast.Node, ok bool) {
	return n, true
}

// buildIdentityExprFromColumn extract an column from node and build an indendity expr based on this column.
func buildIdentityExprFromColumn(n ast.Node, isTrue bool) ast.ExprNode {
	var column ast.Node
	collectColumn := func(n ast.Node) {
		if x, ok := n.(*ast.ColumnNameExpr); ok {
			column = x
		}
	}

	enterFunc := func(n ast.Node) (skip bool) {
		switch x := n.(type) {
		case *ast.BetweenExpr:
			collectColumn(x.Expr)
		case *ast.PatternInExpr:
			collectColumn(x.Expr)
		case *ast.BinaryOperationExpr:
			collectColumn(x.L)
			collectColumn(x.R)
		default:
		}
		return false
	}
	n.Accept(&nodeVisitor{enterFunc, nil})

	// If no column found, return true / false directly
	if column == nil {
		return ast.NewValueExpr(isTrue, "", "")
	}
	return buildIdentityExpr(column, isTrue)
}

func isBooleanExpr(n ast.Node) bool {
	switch x := n.(type) {
	case *ast.BetweenExpr, *ast.IsNullExpr, *ast.IsTruthExpr, *ast.ExistsSubqueryExpr, *ast.PatternInExpr, *ast.PatternLikeOrIlikeExpr:
		return true
	case *ast.BinaryOperationExpr:
		// Avoid recursive transform, for example:
		// a and b and c --> transform(a, transform(b, c))
		countColumnAndValue := 0
		switch x.L.(type) {
		case *ast.ColumnNameExpr, *driver.ValueExpr:
			countColumnAndValue++
		}
		switch x.L.(type) {
		case *ast.ColumnNameExpr, *driver.ValueExpr:
			countColumnAndValue++
		}
		if countColumnAndValue != 2 {
			return false
		}

		switch x.Op {
		case opcode.GE, opcode.GT, opcode.LE, opcode.LT, opcode.EQ, opcode.NE, opcode.In:
			return true
		default:
			return false
		}
	}
	return false
}

func isNormalExpr(n ast.Node) bool {
	switch x := n.(type) {
	case *ast.BetweenExpr, *ast.IsNullExpr, *ast.ExistsSubqueryExpr, *ast.PatternInExpr, *ast.PatternLikeOrIlikeExpr:
		return false
	case *ast.BinaryOperationExpr:
		// Avoid recursive transform, for example:
		// a and b and c --> transform(a, transform(b, c))
		countColumnAndValue := 0
		switch x.L.(type) {
		case *ast.ColumnNameExpr, *driver.ValueExpr:
			countColumnAndValue++
		}
		switch x.L.(type) {
		case *ast.ColumnNameExpr, *driver.ValueExpr:
			countColumnAndValue++
		}
		if countColumnAndValue != 2 {
			return false
		}

		switch x.Op {
		case opcode.GE, opcode.GT, opcode.LE, opcode.LT, opcode.EQ, opcode.NE, opcode.In:
			return false
		default:
			return true
		}
	case *ast.AggregateFuncExpr:
		return true
	default:
		return false
	}
}

func determinedBooleanExpr(n ast.Node) ast.Node {
	if rand.Intn(3) != 0 {
		return n
	}

	isTrue := rand.Intn(2) == 0
	op := opcode.LogicOr
	if isTrue {
		op = opcode.LogicAnd
	}

	return &ast.ParenthesesExpr{
		Expr: &ast.BinaryOperationExpr{
			Op: op,
			L:  buildIdentityExpr(n, isTrue),
			R:  n.(ast.ExprNode),
		},
	}
}

func getRandomExpr(tp types.EvalType) ast.ValueExpr {
	v := &driver.ValueExpr{}
	v.Type.SetFlen(types.UnspecifiedLength)
	v.Type.SetDecimal(types.UnspecifiedLength)
	v.Type.AddFlag(mysql.NotNullFlag)
	types.SetBinChsClnFlag(&v.Type)

	switch tp {
	case types.ETInt:
		v.Datum = types.NewIntDatum(1)
		v.Type.SetType(types.KindInt64)
	case types.ETReal:
		v.Datum = types.NewFloat64Datum(1.0)
		v.Type.SetType(types.KindFloat64)
	case types.ETDecimal:
		v.Datum = types.NewDecimalDatum(types.NewDecFromInt(10))
		v.Type.SetType(types.KindMysqlDecimal)
	case types.ETDatetime:
		v.Datum.SetMysqlTime(types.NewTime(types.FromGoTime(time.Now()), mysql.TypeDatetime, -1))
		v.Type.SetType(types.KindMysqlTime)
	case types.ETTimestamp:
		v.Datum.SetMysqlTime(types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, -1))
		v.Type.SetType(types.KindMysqlTime)
	case types.ETDuration:
		v.Datum = types.NewDurationDatum(types.Duration{Duration: time.Duration(10000)})
		v.Type.SetType(types.KindMysqlDuration)
	case types.ETString:
		v.Datum.SetString("test", "utf8_bin")
		v.Type.SetType(types.KindString)
	case types.ETJson:
		v.Datum = types.NewDatum(types.CreateBinaryJSON(nil))
		v.Type.SetType(types.KindMysqlJSON)
	default:
		panic(fmt.Sprintf("unknown eval type %d", tp))
	}

	return v
}

// deepCopy is a helper function to create a deep copy of a node using reflection.
func deepCopy(n ast.Node) ast.ExprNode {
	if n == nil {
		return nil
	}

	// Obtain the concrete value
	val := reflect.ValueOf(n)

	// Create a new instance of the same type
	newInstance := reflect.New(val.Elem().Type()).Elem()

	// Copy each field
	for i := 0; i < val.Elem().NumField(); i++ {
		field := val.Elem().Field(i)
		newField := newInstance.Field(i)

		if field.Kind() == reflect.Ptr {
			if !field.IsNil() {
				newField.Set(reflect.New(field.Elem().Type()))
				newField.Elem().Set(field.Elem())
			}
		} else {
			if field.CanSet() {
				newField.Set(field)
			}
		}
	}

	p := newInstance.Addr().Interface().(ast.Node)

	return p.(ast.ExprNode)
}

type EETRewriter struct {
	invalid        bool
	insideCaseWhen bool
	isAgg          bool
	schemas        []*model.TableInfo
}

func (eet *EETRewriter) SetSchemas(schemas []*model.TableInfo) {
	eet.schemas = schemas
}

func (eet *EETRewriter) Reset() {
	eet.invalid = false
	eet.insideCaseWhen = false
	eet.isAgg = false
}

func (eet *EETRewriter) Valid() bool {
	return !eet.invalid
}

func (eet *EETRewriter) redundantCaseWhenExpr(n ast.ExprNode) ast.Node {
	if rand.Intn(8) != 0 {
		return n
	}

	// Omit error from evalating return expression's type
	buildOptions, err := sqlgenerator.GetBuildOptions("test", eet.schemas)
	if err != nil {
		return n
	}
	tp, err := sqlgenerator.EvalSingleExpr(n, buildOptions)
	if err != nil || tp == types.ETJson {
		// Currently TiDB doesn't support restore JSON type
		return n
	}

	genTp := rand.Intn(4)
	switch genTp {
	case 1:
		return &ast.CaseExpr{
			WhenClauses: []*ast.WhenClause{
				{
					Expr:   buildIdentityExprFromColumn(n, false),
					Result: getRandomExpr(tp),
				},
			},
			ElseClause: deepCopy(n),
		}
	case 2:
		return &ast.CaseExpr{
			WhenClauses: []*ast.WhenClause{
				{
					Expr:   buildIdentityExprFromColumn(n, true),
					Result: deepCopy(n),
				},
			},
			ElseClause: getRandomExpr(tp),
		}
	case 3:
		return &ast.CaseExpr{
			WhenClauses: []*ast.WhenClause{
				{
					Expr:   ast.NewValueExpr(false, "", ""),
					Result: deepCopy(n),
				},
			},
			ElseClause: n,
		}
	default:
		return &ast.CaseExpr{
			WhenClauses: []*ast.WhenClause{
				{
					Expr:   ast.NewValueExpr(true, "", ""),
					Result: n,
				},
			},
			ElseClause: deepCopy(n),
		}
	}
}

func (eet *EETRewriter) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch n.(type) {
	case *ast.CaseExpr:
		eet.insideCaseWhen = true
	}
	return n, false
}

func (eet *EETRewriter) Leave(n ast.Node) (retNode ast.Node, ok bool) {
	defer func() {
		if _, ok := n.(*ast.CaseExpr); ok {
			eet.insideCaseWhen = false
		}
	}()

	if eet.insideCaseWhen {
		return n, true
	}

	if isBooleanExpr(n) {
		return determinedBooleanExpr(n), true
	} else if isNormalExpr(n) {
		return eet.redundantCaseWhenExpr(n.(ast.ExprNode)), true
	}

	return n, true
}
