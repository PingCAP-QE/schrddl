package ddl

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"

	"github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"
	"github.com/juju/errors"
	"github.com/ngaut/log"
)

func (c *testCase) generateDMLOps() error {
	if err := c.generateInsert(); err != nil {
		return errors.Trace(err)
	}
	if err := c.generateUpdate(); err != nil {
		return errors.Trace(err)
	}
	if err := c.generateDelete(); err != nil {
		return errors.Trace(err)
	}
	if err := c.generateSelect(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

type ddlTestInsertColumnStrategy int
type ddlTestInsertMissingValueStrategy int

const (
	ddlTestInsertColumnStrategyBegin ddlTestInsertColumnStrategy = iota
	ddlTestInsertColumnStrategyZeroNonPk
	ddlTestInsertColumnStrategyAllNonPk
	ddlTestInsertColumnStrategyRandomNonPk
	ddlTestInsertColumnStrategyEnd
)

const (
	ddlTestInsertMissingValueStrategyBegin ddlTestInsertMissingValueStrategy = iota
	ddlTestInsertMissingValueStrategyAllNull
	ddlTestInsertMissingValueStrategyAllDefault
	ddlTestInsertMissingValueStrategyRandom
	ddlTestInsertMissingValueStrategyEnd
)

type ddlTestInsertConfig struct {
	useSetStatement      bool                              // whether to use SET or VALUE statement
	columnStrategy       ddlTestInsertColumnStrategy       // how non-Primary-Key columns are picked
	missingValueStrategy ddlTestInsertMissingValueStrategy // how columns are filled when they are not picked in VALUE statement
}

func checkConflict(task *dmlJobTask) error {
	table := task.tblInfo
	table.lock.Lock()
	defer table.lock.Unlock()
	if table.isDeleted() {
		return ddlTestErrorConflict{}
	}
	if task.assigns != nil {
		for _, cd := range task.assigns {
			if cd.column.isDeleted() || cd.column.isRenamed() {
				return ddlTestErrorConflict{}
			}
		}
	}
	if task.whereColumns != nil {
		for _, cd := range task.whereColumns {
			if cd.column.isDeleted() || cd.column.isRenamed() {
				return ddlTestErrorConflict{}
			}
		}
	}
	return nil
}

func (c *testCase) sendDMLRequest(ctx context.Context, conn *sql.Conn, task *dmlJobTask) error {
	var err error
	var stmt *sql.Stmt
	if Prepare {
		stmt, err = conn.PrepareContext(ctx, task.sql)
		if err == nil {
			_, err = stmt.ExecContext(ctx)
			_ = stmt.Close()
		}
	} else {
		_, err = conn.ExecContext(ctx, task.sql)
	}
	task.err = err
	log.Infof("[dml] [instance %d] %s, err: %v", c.caseIndex, task.sql, err)
	if err != nil {
		err2 := checkConflict(task)
		if err2 != nil {
			return nil
		}
		return errors.Annotatef(err, "Error when executing SQL: %s\n%s", task.sql, task.tblInfo.debugPrintToString())
	}
	return nil
}

func (c *testCase) execDMLInLocal(task *dmlJobTask) error {
	return nil
}

// execSerialDMLSQL gets a job from taskCh, and then executes the job.
func (c *testCase) execSerialDMLSQL(taskCh chan *dmlJobTask) error {
	if len(taskCh) == 0 {
		return nil
	}
	ctx := context.Background()
	dbIdx := rand.Intn(len(c.dbs))
	db := c.dbs[dbIdx]
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil
	}
	defer conn.Close()
	task := <-taskCh
	err = c.sendDMLRequest(ctx, conn, task)
	if err != nil {
		if dmlIgnoreError(err) {
			return nil
		}
		return errors.Trace(err)
	}
	if task.err != nil {
		return nil
	}
	err = c.execDMLInLocal(task)
	if err != nil {
		return fmt.Errorf("Error when executing SQL: %s\n local Err: %#v\n%s\n", task.sql, err, task.tblInfo.debugPrintToString())
	}
	return nil
}

// execDMLInTransactionSQL gets a job from taskCh, and then executes the job.
func (c *testCase) execDMLInTransactionSQL(taskCh chan *dmlJobTask) error {
	tasksLen := len(taskCh)

	ctx := context.Background()
	conn, err := c.dbs[1].Conn(ctx)
	if err != nil {
		return nil
	}
	defer conn.Close()

	_, err = conn.ExecContext(ctx, "begin")
	log.Infof("[dml] [instance %d] begin error: %v", c.caseIndex, err)
	if err != nil {
		return errors.Annotatef(err, "Error when executing SQL: %s", "begin")
	}

	tasks := make([]*dmlJobTask, 0, tasksLen)
	for i := 0; i < tasksLen; i++ {
		task := <-taskCh
		err = c.sendDMLRequest(ctx, conn, task)
		tasks = append(tasks, task)
	}

	_, err = conn.ExecContext(ctx, "commit")
	log.Infof("[dml] [instance %d] commit error: %v", c.caseIndex, err)
	if err != nil {
		if dmlIgnoreError(err) {
			return nil
		}
		for i := 0; i < tasksLen; i++ {
			task := tasks[i]
			// no conflict when send request but conflict when commit
			if task.err == nil && checkConflict(task) != nil {
				return nil
			}
		}
		return errors.Annotatef(err, "Error when executing SQL: %s", "commit")
	}

	for i := 0; i < tasksLen; i++ {
		task := tasks[i]
		if task.err != nil {
			continue
		}
		err = c.execDMLInLocal(task)
		if err != nil {
			return fmt.Errorf("Error when executing SQL: %s\n local Err: %#v\n%s\n", task.sql, err, task.tblInfo.debugPrintToString())
		}
	}
	log.Infof("[dml] [instance %d] finish transaction dml", c.caseIndex)
	return nil
}

const dmlSizeEachRound = 10

func (c *testCase) generateInsert() error {
	for i := 0; i < dmlSizeEachRound; i++ {
		for columnStrategy := ddlTestInsertColumnStrategyBegin + 1; columnStrategy < ddlTestInsertColumnStrategyEnd; columnStrategy++ {
			// Note: `useSetStatement` is commented out since `... VALUES ...` SQL will generates column conflicts with add / drop column.
			// We always use `... SET ...` syntax currently.

			// for useSetStatement := 0; useSetStatement < 2; useSetStatement++ {
			config := ddlTestInsertConfig{
				useSetStatement: true, // !(useSetStatement == 0),
				columnStrategy:  columnStrategy,
			}
			//	if config.useSetStatement {
			c.dmlOps = append(c.dmlOps, dmlTestOpExecutor{c.prepareInsert, config})
			// 	} else {
			// 		for missingValueStrategy := ddlTestInsertMissingValueStrategyBegin + 1; missingValueStrategy < ddlTestInsertMissingValueStrategyEnd; missingValueStrategy++ {
			// 			config.missingValueStrategy = missingValueStrategy
			// 			c.dmlOps = append(c.dmlOps, ddlTestOpExecutor{c.executeInsert, config})
			// 		}
			// 	}
			// }
		}
	}
	return nil
}

func (c *testCase) prepareInsert(cfg interface{}, taskCh chan *dmlJobTask) error {
	c.tablesLock.Lock()
	defer c.tablesLock.Unlock()
	table := c.pickupRandomTable()
	if table == nil {
		return nil
	}
	table.lock.Lock()
	defer table.lock.Unlock()

	if rand.Intn(2) == 0 {
		state := sqlgen.NewState()
		state.Tables = append(state.Tables, table.mapTableToRandTestTable())
		sql, err := sqlgen.CommonInsertOrReplace.Eval(state)
		if err != nil {
			return err
		}
		task := &dmlJobTask{
			k:       dmlInsert,
			sql:     sql,
			tblInfo: table,
		}
		taskCh <- task
		return nil
	}

	columns := table.filterColumns(table.predicateNotGenerated)
	nonPkColumns := table.filterColumns(table.predicateNonPrimaryKeyAndNotGen)

	config := cfg.(ddlTestInsertConfig)

	// build assignments
	assigns := make([]*ddlTestColumnDescriptor, 0)
	for _, column := range columns {
		pick := false
		if column.isPrimaryKey {
			// PrimaryKey Column is always assigned values
			pick = true
		} else {
			// NonPrimaryKey Column is assigned by strategy
			switch config.columnStrategy {
			case ddlTestInsertColumnStrategyAllNonPk:
				pick = true
			case ddlTestInsertColumnStrategyZeroNonPk:
				pick = false
			case ddlTestInsertColumnStrategyRandomNonPk:
				if rand.Float64() <= float64(1)/float64(len(nonPkColumns)) {
					pick = true
				}
			}
		}
		if pick {
			// check unique value when inserting into a column of primary key
			if column.isPrimaryKey {
				if newValue, ok := column.randValueUnique(column.rows); ok {
					assigns = append(assigns, &ddlTestColumnDescriptor{column, newValue})
				} else {
					return nil
				}
			} else {
				assigns = append(assigns, &ddlTestColumnDescriptor{column, column.randValue()})
			}
		}
	}

	// build SQL
	sql := ""
	if config.useSetStatement {
		if len(assigns) == 0 {
			return nil
		}
		sql = fmt.Sprintf("INSERT INTO `%s` SET ", table.name)
		perm := rand.Perm(len(assigns))
		for i, idx := range perm {
			assign := assigns[idx]
			if i > 0 {
				sql += ", "
			}
			sql += fmt.Sprintf("`%s` = %v", assign.column.name, assign.getValueString())
		}
	} else {
		sql = fmt.Sprintf("INSERT INTO `%s` VALUE (", table.name)
		for colIdx, column := range columns {
			if colIdx > 0 {
				sql += ", "
			}
			cd := column.getMatchedColumnDescriptor(assigns)
			if cd != nil {
				sql += fmt.Sprintf("%v", cd.getValueString())
			} else {
				var missingValueSQL string
				switch config.missingValueStrategy {
				case ddlTestInsertMissingValueStrategyAllDefault:
					missingValueSQL = "DEFAULT"
				case ddlTestInsertMissingValueStrategyAllNull:
					missingValueSQL = "NULL"
				case ddlTestInsertMissingValueStrategyRandom:
					if rand.Float64() <= 0.5 {
						missingValueSQL = "DEFAULT"
					} else {
						missingValueSQL = "NULL"
					}
				}
				sql += missingValueSQL
				var missingValue interface{}
				if missingValueSQL == "DEFAULT" {
					missingValue = column.defaultValue
				} else if missingValueSQL == "NULL" {
					missingValue = ddlTestValueNull
				} else {
					panic("invalid missing value")
				}
				// add column to ref list
				assigns = append(assigns, &ddlTestColumnDescriptor{column, missingValue})
			}
		}
		sql += ")"
	}

	task := &dmlJobTask{
		k:       dmlInsert,
		sql:     sql,
		tblInfo: table,
		assigns: assigns,
	}
	taskCh <- task
	return nil
}

type ddlTestWhereStrategy int

const (
	ddlTestWhereStrategyBegin ddlTestWhereStrategy = iota
	ddlTestWhereStrategyNone
	ddlTestWhereStrategyRandomInPk
	ddlTestWhereStrategyRandomInNonPk
	ddlTestWhereStrategyRandomMixed
	ddlTestWhereStrategyEnd
)

type ddlTestUpdateTargetStrategy int

const (
	ddlTestUpdateTargetStrategyBegin ddlTestUpdateTargetStrategy = iota
	ddlTestUpdateTargetStrategyAllColumns
	ddlTestUpdateTargetStrategyRandom
	ddlTestUpdateTargetStrategyEnd
)

type ddlTestUpdateConfig struct {
	whereStrategy  ddlTestWhereStrategy        // how "where" statement is generated
	targetStrategy ddlTestUpdateTargetStrategy // which column to update
}

func (c *testCase) generateUpdate() error {
	for i := 0; i < dmlSizeEachRound; i++ {
		for whereStrategy := ddlTestWhereStrategyBegin + 1; whereStrategy < ddlTestWhereStrategyEnd; whereStrategy++ {
			for targetStrategy := ddlTestUpdateTargetStrategyBegin + 1; targetStrategy < ddlTestUpdateTargetStrategyEnd; targetStrategy++ {
				config := ddlTestUpdateConfig{
					whereStrategy:  whereStrategy,
					targetStrategy: targetStrategy,
				}
				c.dmlOps = append(c.dmlOps, dmlTestOpExecutor{c.prepareUpdate, config})
			}
		}
	}
	return nil
}

func (c *testCase) buildWhereColumns(whereStrategy ddlTestWhereStrategy, pkColumns, nonPkColumns []*ddlTestColumn, numberOfRows int) []*ddlTestColumnDescriptor {
	// build where conditions
	whereColumns := make([]*ddlTestColumnDescriptor, 0)
	if whereStrategy == ddlTestWhereStrategyRandomInPk || whereStrategy == ddlTestWhereStrategyRandomMixed {
		if len(pkColumns) > 0 {
			picks := rand.Intn(len(pkColumns))
			perm := rand.Perm(picks)
			for _, idx := range perm {
				// value will be filled later
				whereColumns = append(whereColumns, &ddlTestColumnDescriptor{pkColumns[idx], -1})
			}
		}
	}
	if whereStrategy == ddlTestWhereStrategyRandomInNonPk || whereStrategy == ddlTestWhereStrategyRandomMixed {
		if len(nonPkColumns) > 0 {
			picks := rand.Intn(len(nonPkColumns))
			perm := rand.Perm(picks)
			for _, idx := range perm {
				// value will be filled later
				whereColumns = append(whereColumns, &ddlTestColumnDescriptor{nonPkColumns[idx], -1})
			}
		}
	}

	// fill values of where statements
	if len(whereColumns) > 0 {
		rowToUpdate := rand.Intn(numberOfRows)
		for _, cd := range whereColumns {
			cd.value = getRowFromArrayList(cd.column.rows, rowToUpdate)
		}
	}

	return whereColumns
}

func (c *testCase) prepareUpdate(cfg interface{}, taskCh chan *dmlJobTask) error {
	c.tablesLock.Lock()
	defer c.tablesLock.Unlock()
	table := c.pickupRandomTable()
	if table == nil {
		return nil
	}
	table.lock.Lock()
	defer table.lock.Unlock()

	if rand.Intn(2) == 0 {
		state := sqlgen.NewState()
		state.Tables = append(state.Tables, table.mapTableToRandTestTable())
		sql, err := sqlgen.CommonUpdate.Eval(state)
		if err != nil {
			return err
		}
		task := &dmlJobTask{
			k:       dmlUpdate,
			sql:     sql,
			tblInfo: table,
		}
		taskCh <- task
		return nil
	}

	pkColumns := table.filterColumns(table.predicatePrimaryKey)
	nonPkColumnsAndCanBeWhere := table.filterColumns(table.predicateNonPrimaryKeyAndCanBeWhere)
	nonPkColumnsAndNotGen := table.filterColumns(table.predicateNonPrimaryKeyAndNotGen)

	if table.numberOfRows == 0 {
		return nil
	}

	config := cfg.(ddlTestUpdateConfig)

	// build where conditions
	whereColumns := c.buildWhereColumns(config.whereStrategy, pkColumns, nonPkColumnsAndCanBeWhere, table.numberOfRows)

	// build assignments
	assigns := make([]*ddlTestColumnDescriptor, 0)
	picks := 0
	switch config.targetStrategy {
	case ddlTestUpdateTargetStrategyRandom:
		if len(nonPkColumnsAndNotGen) > 0 {
			picks = rand.Intn(len(nonPkColumnsAndNotGen))
		}
	case ddlTestUpdateTargetStrategyAllColumns:
		picks = len(nonPkColumnsAndNotGen)
	}
	if picks == 0 {
		return nil
	}
	perm := rand.Perm(picks)
	for _, idx := range perm {
		assigns = append(assigns, &ddlTestColumnDescriptor{nonPkColumnsAndNotGen[idx], nonPkColumnsAndNotGen[idx].randValue()})
	}

	// build SQL
	sql := fmt.Sprintf("UPDATE `%s` SET ", table.name)
	for i, cd := range assigns {
		if i > 0 {
			sql += ", "
		}
		sql += fmt.Sprintf("`%s` = %v", cd.column.name, cd.getValueString())
	}
	if len(whereColumns) > 0 {
		sql += " WHERE "
		for i, cd := range whereColumns {
			if i > 0 {
				sql += " AND "
			}
			sql += cd.buildConditionSQL()
		}
	}

	task := &dmlJobTask{
		k:            dmlUpdate,
		tblInfo:      table,
		sql:          sql,
		assigns:      assigns,
		whereColumns: whereColumns,
	}

	taskCh <- task
	return nil
}

type ddlTestDeleteConfig struct {
	whereStrategy ddlTestWhereStrategy // how "where" statement is generated
}

func (c *testCase) generateDelete() error {
	for i := 0; i < dmlSizeEachRound; i++ {
		for whereStrategy := ddlTestWhereStrategyBegin + 1; whereStrategy < ddlTestWhereStrategyEnd; whereStrategy++ {
			config := ddlTestDeleteConfig{
				whereStrategy: whereStrategy,
			}
			c.dmlOps = append(c.dmlOps, dmlTestOpExecutor{c.prepareDelete, config})
		}
	}
	return nil
}

func (c *testCase) prepareDelete(cfg interface{}, taskCh chan *dmlJobTask) error {
	c.tablesLock.Lock()
	defer c.tablesLock.Unlock()
	table := c.pickupRandomTable()
	if table == nil {
		return nil
	}
	table.lock.Lock()
	defer table.lock.Unlock()

	state := sqlgen.NewState()
	state.Tables = append(state.Tables, table.mapTableToRandTestTable())
	sql, err := sqlgen.CommonDelete.Eval(state)
	if err != nil {
		return err
	}
	task := &dmlJobTask{
		k:       dmlDelete,
		sql:     sql,
		tblInfo: table,
	}
	taskCh <- task
	return nil
}

func (c *testCase) generateSelect() error {
	for i := 0; i < dmlSizeEachRound; i++ {
		c.dmlOps = append(c.dmlOps, dmlTestOpExecutor{c.prepareSelect, nil})
	}
	return nil
}

func (c *testCase) prepareSelect(cfg interface{}, taskCh chan *dmlJobTask) error {
	c.tablesLock.Lock()
	defer c.tablesLock.Unlock()
	table := c.pickupRandomTable()
	if table == nil {
		return nil
	}

	state := sqlgen.NewState()
	state.Tables = append(state.Tables, table.mapTableToRandTestTable())
	//state.ReplaceRule(sqlgen.Query, sqlgen.CommonSelect)

	query, err := sqlgen.SingleSelect.Eval(state)
	if err != nil {
		return err
	}
	log.Infof("query: %s", query)

	task := &dmlJobTask{
		k:       dmlSelect,
		tblInfo: table,
		sql:     query,
	}
	taskCh <- task
	return nil
}
