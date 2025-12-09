package sqlgenerator

import (
	"fmt"
	"strings"
)

func (t *Table) String() string {
	var sb strings.Builder
	sb.WriteString("create table ")
	sb.WriteString(t.Name)
	sb.WriteString(" (")
	for i, c := range t.Columns {
		if i != 0 {
			sb.WriteString(",")
		}
		sb.WriteString(c.String())
	}
	if len(t.Indexes) != 0 {
		sb.WriteString(", ")
		for i, idx := range t.Indexes {
			if i != 0 {
				sb.WriteString(",")
			}
			sb.WriteString(idx.String())
		}
	}
	sb.WriteString(")")
	return sb.String()
}

// TypeString return the string represent the type of this column.
func (c *Column) TypeString() string {
	typeArg := ""
	switch c.Tp {
	case ColumnTypeBit, ColumnTypeChar, ColumnTypeBinary, ColumnTypeVarchar,
		ColumnTypeText, ColumnTypeBlob, ColumnTypeVarBinary:
		typeArg = fmt.Sprintf("(%d)", c.Arg1)
	case ColumnTypeDecimal:
		typeArg = fmt.Sprintf("(%d, %d)", c.Arg1, c.Arg2)
	case ColumnTypeSet, ColumnTypeEnum:
		s := make([]string, 0, len(c.Args))
		for _, a := range c.Args {
			s = append(s, fmt.Sprintf("'%s'", a))
		}
		typeArg = fmt.Sprintf("(%s)", strings.Join(s, ","))
	}

	return fmt.Sprintf("%s%s", c.Tp.String(), typeArg)
}

func (c *Column) String() string {
	return fmt.Sprintf("%s %s", c.Name, c.TypeString())
}

func (i *Index) String() string {
	var sb strings.Builder
	sb.WriteString(i.Tp.String())
	sb.WriteString(" ")
	sb.WriteString(i.Name)
	for i, c := range i.Columns {
		if i != 0 {
			sb.WriteString(",")
		}
		sb.WriteString(c.Name)
	}
	return sb.String()
}
