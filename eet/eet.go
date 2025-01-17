package eet

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"math/rand"
	"reflect"
)

// deepCopy is a helper function to create a deep copy of a node using reflection.
func deepCopy(n ast.Node) ast.Node {
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

	return p
}

// copyRecursive recursively copies the values from the original to the copy.
func copyRecursive(original, copy reflect.Value) {
	switch original.Kind() {
	case reflect.Ptr:
		if original.IsNil() {
			return
		}
		copy.Set(reflect.New(original.Elem().Type()).Elem())
		if copy.CanSet() {
			copyRecursive(original.Elem(), copy)
		}
	case reflect.Struct:
		for i := 0; i < original.NumField(); i++ {
			copyRecursive(original.Field(i), copy.Field(i))
		}
	case reflect.Slice:
		if original.IsNil() {
			return
		}
		copy.Set(reflect.MakeSlice(original.Type(), original.Len(), original.Cap()))
		for i := 0; i < original.Len(); i++ {
			copyRecursive(original.Index(i), copy.Index(i))
		}
	default:
		copy.Set(original)
	}
}

type EETRewriter struct {
	pred           []ast.Node
	invalid        bool
	ctes           []string
	insideCte      bool
	insideSubQuery bool
	isAgg          bool
}

func (eet *EETRewriter) Reset() {
	eet.invalid = false
	eet.ctes = make([]string, 0)
	eet.insideCte = false
	eet.insideSubQuery = false
	eet.isAgg = false
}

func (eet *EETRewriter) Valid() bool {
	return !eet.invalid
}

func (eet *EETRewriter) IsAgg() bool {
	return eet.isAgg
}

func (eet *EETRewriter) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch v := n.(type) {
	case *ast.WithClause:
		for _, cte := range v.CTEs {
			eet.ctes = append(eet.ctes, cte.Name.String())
		}
		eet.insideCte = true
	case *ast.TableSource:
		switch v.Source.(type) {
		case *ast.SelectStmt, *ast.SetOprStmt:
			eet.insideSubQuery = true
		}
	}
	return n, false
}

func (eet *EETRewriter) Leave(n ast.Node) (retNode ast.Node, ok bool) {
	switch v := n.(type) {
	case *ast.WithClause:
		eet.insideCte = false
	case *ast.BetweenExpr:
		if rand.Intn(3) != 0 {
			return
		}

		p := v
		p1 := &ast.BinaryOperationExpr{Op: opcode.LogicAnd, L: &ast.BinaryOperationExpr{Op: opcode.LogicAnd, L: deepCopy(p).(ast.ExprNode), R: &ast.UnaryOperationExpr{Op: opcode.Not, V: deepCopy(p).(ast.ExprNode)}}, R: &ast.IsNullExpr{Expr: deepCopy(p).(ast.ExprNode), Not: true}}
		p2 := &ast.BinaryOperationExpr{Op: opcode.LogicOr, L: deepCopy(p).(ast.ExprNode), R: p1}
		return &ast.ParenthesesExpr{Expr: p2}, true
	}
	return n, true
}
