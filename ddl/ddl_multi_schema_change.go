package ddl

import (
	"fmt"
	"math/rand"
	"strings"
)

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

func checkAddDropColumn(ctx any, col *ddlTestColumn) bool {
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

func checkModifyColumn(ctx any, col *ddlTestColumn) bool {
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

func checkRelatedColumn(ctx any, col *ddlTestColumn) bool {
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

func checkModifyIdx(ctx any, idx *ddlTestIndex) bool {
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

func (c *testCase) multiSchemaChangeJob(task *ddlJobTask) error {
	jobArg := (*ddlMultiSchemaChangeJobArg)(task.arg)
	tblInfo := task.tblInfo

	if c.isTableDeleted(tblInfo) {
		return fmt.Errorf("table %s is not exists", tblInfo.name)
	}

	for _, subJob := range jobArg.subJobs {
		if err := c.updateTableInfo(subJob); err != nil {
			return err
		}
	}
	return nil
}

func (c *testCase) generateMultiSchemaChange(repeat int) error {
	for i := 0; i < repeat; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.prepareMultiSchemaChange, nil, ddlMultiSchemaChange})
	}
	return nil
}

func (c *testCase) prepareSubJobs(ctx *multiSchemaChangeCtx, ddlKind DDLKind) error {
	tmpCh := make(chan *ddlJobTask, 1)
	defer close(tmpCh)
	table := ctx.tblInfo
	prefixSQL := strings.ToLower(fmt.Sprintf("ALTER TABLE `%s` ", table.name))
	switch ddlKind {
	case ddlAddColumn:
		if err := c.prepareAddColumn(ctx, tmpCh); err != nil {
			return err
		}
	case ddlDropColumn:
		if err := c.prepareDropColumn(ctx, tmpCh); err != nil {
			return err
		}
	case ddlAddIndex:
		if err := c.prepareAddIndex(ctx, tmpCh); err != nil {
			return err
		}
	case ddlDropIndex:
		if err := c.prepareDropIndex(ctx, tmpCh); err != nil {
			return err
		}
	case ddlRenameIndex:
		if err := c.prepareRenameIndex(ctx, tmpCh); err != nil {
			return err
		}
	case ddlModifyColumn:
		if err := c.prepareModifyColumn(ctx, tmpCh); err != nil {
			return err
		}
	case ddlModifyColumn2:
		if err := c.prepareModifyColumn2(ctx, tmpCh); err != nil {
			return err
		}
	}
	var subJob *ddlJobTask
	if len(tmpCh) == 1 {
		subJob = <-tmpCh
	} else {
		return nil
	}
	pos := strings.Index(strings.ToLower(subJob.sql), prefixSQL)
	if pos != -1 {
		if len(ctx.arg.subJobs) != 0 {
			ctx.sql += ", "
		}
		ctx.sql += subJob.sql[pos+len(prefixSQL):]
	} else {
		return fmt.Errorf("invalid sub job sql: %s", subJob.sql)
	}
	subJob.isSubJob = true
	ctx.arg.subJobs = append(ctx.arg.subJobs, subJob)
	return nil
}

func (c *testCase) prepareMultiSchemaChange(_ any, taskCh chan *ddlJobTask) error {
	table := c.pickupRandomTable()
	if table == nil {
		return nil
	}
	numAlterStmts := rand.Intn(MaxSubjobsInMultiSchemaChange) + 1

	ctx := &multiSchemaChangeCtx{
		tblInfo:     table,
		sql:         fmt.Sprintf("ALTER TABLE `%s` ", table.name),
		addDropCols: make(map[string]struct{}),
		modifyCols:  make(map[string]struct{}),
		relatedCols: make(map[string]struct{}),
		modifyIdx:   make(map[string]struct{}),
		arg: &ddlMultiSchemaChangeJobArg{
			subJobs: make([]*ddlJobTask, 0, numAlterStmts),
		},
	}
	for i := 0; i < numAlterStmts; i++ {
		ddlKind := supportMultiSchemaChangeDDLKind[rand.Intn(len(supportMultiSchemaChangeDDLKind))]
		if err := c.prepareSubJobs(ctx, ddlKind); err != nil {
			return err
		}
	}

	if len(ctx.arg.subJobs) == 0 {
		return nil
	}

	task := &ddlJobTask{
		k:       ddlMultiSchemaChange,
		sql:     ctx.sql,
		tblInfo: table,
		arg:     ddlJobArg(ctx.arg),
	}
	taskCh <- task
	return nil
}
