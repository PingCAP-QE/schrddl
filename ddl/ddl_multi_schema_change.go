package ddl

const MaxSubjobsInMultiSchemaChange int = 5

var supportMultiSchemaChangeDDLKind = []DDLKind{
	ddlAddColumn,
	ddlDropColumn,
	ddlAddIndex,
	ddlDropIndex,
	ddlRenameIndex,
	ddlModifyColumn,
	ddlModifyColumn2,
}

type ddlMultiSchemaChangeJobArg struct {
	subJobs []*ddlJobTask
}

type multiSchemaChangeCtx struct {
	tblInfo     *ddlTestTable
	sql         string
	addDropCols map[string]struct{}
	modifyCols  map[string]struct{}
	relatedCols map[string]struct{}
	modifyIdx   map[string]struct{}
	arg         *ddlMultiSchemaChangeJobArg
}

func checkAddDropColumn(ctx interface{}, col *ddlTestColumn) bool {
	if ctx == nil {
		return true
	}
	context := ctx.(*multiSchemaChangeCtx)
	if _, ok := context.addDropCols[col.name]; ok {
		return false
	}
	if _, ok := context.modifyCols[col.name]; ok {
		return false
	}
	if _, ok := context.relatedCols[col.name]; ok {
		return false
	}
	context.addDropCols[col.name] = struct{}{}
	return true
}

func checkModifyColumn(ctx interface{}, col *ddlTestColumn) bool {
	if ctx == nil {
		return true
	}
	context := ctx.(*multiSchemaChangeCtx)
	if _, ok := context.addDropCols[col.name]; ok {
		return false
	}
	if _, ok := context.modifyCols[col.name]; ok {
		return false
	}
	context.modifyCols[col.name] = struct{}{}
	return true
}

func checkRelatedColumn(ctx interface{}, col *ddlTestColumn) bool {
	if ctx == nil {
		return true
	}
	context := ctx.(*multiSchemaChangeCtx)
	if _, ok := context.addDropCols[col.name]; ok {
		return false
	}
	context.relatedCols[col.name] = struct{}{}
	return true
}

func checkModifyIdx(ctx interface{}, idx *ddlTestIndex) bool {
	if ctx == nil {
		return true
	}
	context := ctx.(*multiSchemaChangeCtx)
	if _, ok := context.modifyIdx[idx.name]; ok {
		return false
	}
	context.modifyIdx[idx.name] = struct{}{}
	return true
}
