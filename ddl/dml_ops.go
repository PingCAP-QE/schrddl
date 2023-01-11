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
		return errors.Annotatef(err, "Error when executing SQL: %s\n%s", task.sql)
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
		return fmt.Errorf("Error when executing SQL: %s\n local Err: %#v\n\n", task.sql, err)
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
			if task.err == nil {
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
			return fmt.Errorf("Error when executing SQL: %s\n local Err: %#v\n\n", task.sql, err)
		}
	}
	log.Infof("[dml] [instance %d] finish transaction dml", c.caseIndex)
	return nil
}

const dmlSizeEachRound = 100

func (c *testCase) generateInsert() error {
	for i := 0; i < dmlSizeEachRound; i++ {
		c.dmlOps = append(c.dmlOps, dmlTestOpExecutor{c.prepareInsert, nil})
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

func (c *testCase) generateUpdate() error {
	for i := 0; i < dmlSizeEachRound; i++ {
		c.dmlOps = append(c.dmlOps, dmlTestOpExecutor{c.prepareUpdate, nil})
	}
	return nil
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

func (c *testCase) generateDelete() error {
	for i := 0; i < dmlSizeEachRound; i++ {
		c.dmlOps = append(c.dmlOps, dmlTestOpExecutor{c.prepareDelete, nil})
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
	//table := c.pickupRandomTable()
	//if table == nil {
	//	return nil
	//}

	if len(c.tables) == 0 {
		return nil
	}

	state := sqlgen.NewState()
	state.SetWeight(sqlgen.WindowFunctionOverW, 0)
	for _, tbl := range c.tables {
		state.Tables = append(state.Tables, tbl.mapTableToRandTestTable())
	}

	query, err := sqlgen.Query.Eval(state)
	if err != nil {
		return err
	}
	log.Infof("query: %s", query)

	task := &dmlJobTask{
		k:       dmlSelect,
		tblInfo: nil,
		sql:     query,
	}
	taskCh <- task
	return nil
}
