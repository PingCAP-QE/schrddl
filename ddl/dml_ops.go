package ddl

import (
	"context"
	"database/sql"
	"math"
	"math/rand"
	"regexp"
	"sort"
	"strconv"

	"github.com/PingCAP-QE/schrddl/sqlgenerator"
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
	//if err := c.generateSelect(); err != nil {
	//	return errors.Trace(err)
	//}
	return nil
}

func sendQueryRequest(ctx context.Context, conn *sql.Conn, task *dmlJobTask) ([][]string, error) {
	return readData(ctx, conn, task.sql)
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
	//log.Infof("[dml] [instance %d] %s, err: %v", c.caseIndex, task.sql, err)
	if err != nil {
		return errors.Annotatef(err, "Error when executing SQL: %s\n%s", task.sql)
	}
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
	// disable compare for now since it's not stable.
	if task.k == dmlSelect && false {
		if rand.Intn(5) == 0 {
			r, _ := regexp.Compile("/\\*.*?\\*/")
			var rows [][]string
			// Check plan
			_, err := conn.ExecContext(ctx, "begin")
			if err != nil {
				return err
			}
			defer func() {
				conn.ExecContext(ctx, "commit")
			}()
			rows, err = sendQueryRequest(ctx, conn, task)
			log.Infof("[dml] [instance %d] %s, err: %v", c.caseIndex, task.sql, err)
			if err != nil {
				if dmlIgnoreError(err) {
					return nil
				}
				return errors.Trace(err)
			}

			// Check no hint plan
			origianlSql := task.sql
			noHintSql := r.ReplaceAllString(task.sql, "")
			task.sql = noHintSql
			newRow, err := sendQueryRequest(ctx, conn, task)
			log.Infof("[dml] [instance %d] %s, err: %v", c.caseIndex, task.sql, err)
			if err != nil {
				return errors.Trace(err)
			}
			err = compareTwoRows(rows, newRow)
			var ts [][]string
			if err == nil {
				return nil
			}

			// Make sure the query is stable itself.
			task.sql = origianlSql
			orignalRowToCheck, err2 := sendQueryRequest(ctx, conn, task)
			if err2 != nil {
				return nil
			}
			err2 = compareTwoRows(rows, orignalRowToCheck)
			if err2 != nil {
				// The query is not stable.
				return nil
			}

			task.sql = "select @@tidb_current_ts"
			ts, _ = sendQueryRequest(ctx, conn, task)
			return errors.Errorf("[dml] [instance %d] found inconsistent data for two plans, sql-with-hint %s, sql-without-hint %s, %s, err: %v", c.caseIndex, origianlSql, noHintSql, ts[0][0], err)
		}
	}
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
	return nil
}

func compareTwoRows(rows1 [][]string, rows2 [][]string) error {
	if len(rows1) != len(rows2) {
		return errors.Errorf("rows1 and rows2 have different length, rows1: %v, rows2: %v", rows1, rows2)
	}
	sort.Slice(rows1, func(i, j int) bool {
		colLen := len(rows1[i])
		for ii := 0; ii < colLen; ii++ {
			if rows1[i][ii] != rows1[j][ii] {
				return rows1[i][ii] < rows1[j][ii]
			}
		}
		return true
	})
	sort.Slice(rows2, func(i, j int) bool {
		colLen := len(rows2[i])
		for ii := 0; ii < colLen; ii++ {
			if rows2[i][ii] != rows2[j][ii] {
				return rows2[i][ii] < rows2[j][ii]
			}
		}
		return true
	})
	for i := 0; i < len(rows1); i++ {
		for j := 0; j < len(rows1[i]); j++ {
			if rows1[i][j] != rows2[i][j] {
				if f1, err := strconv.ParseFloat(rows1[i][j], 64); err == nil {
					if f2, err := strconv.ParseFloat(rows2[i][j], 64); err == nil {
						if math.Abs(f1-f2) < 0.001 {
							continue
						}
						log.Errorf("f1: %v, f2: %v, abs: %v", f1, f2, math.Abs(f1-f2))
					}
					log.Errorf("err: %v", err)
				} else {
					log.Errorf("err: %v", err)
				}
				return errors.Errorf("rows1 and rows2 have different value, rows1: %v, rows2: %v. Full data: rows1 %v, rows2 %v", rows1[i][j], rows2[i][j], rows1, rows2)
			}
		}
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

	state := sqlgenerator.NewState()
	state.Tables = append(state.Tables, table.mapTableToRandTestTable())
	sql, err := sqlgenerator.CommonInsertOrReplace.Eval(state)
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

	state := sqlgenerator.NewState()
	state.SetWeight(sqlgenerator.Limit, 1000)
	state.SetWeight(sqlgenerator.ScalarSubQuery, 0)
	for _, table := range c.tableMap {
		state.Tables = append(state.Tables, table)
	}

	if len(state.Tables) == 0 {
		return nil
	}

	sql, err := sqlgenerator.CommonUpdate.Eval(state)
	if err != nil {
		return err
	}
	task := &dmlJobTask{
		k:       dmlUpdate,
		sql:     sql,
		tblInfo: nil,
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

	state := sqlgenerator.NewState()
	state.SetWeight(sqlgenerator.Limit, 1000)
	state.SetWeight(sqlgenerator.ScalarSubQuery, 0)
	for _, table := range c.tableMap {
		state.Tables = append(state.Tables, table)
	}

	if len(state.Tables) == 0 {
		return nil
	}

	sql, err := sqlgenerator.CommonDelete.Eval(state)
	if err != nil {
		return err
	}
	task := &dmlJobTask{
		k:       dmlDelete,
		sql:     sql,
		tblInfo: nil,
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

	if len(c.tables) == 0 {
		return nil
	}

	state := sqlgenerator.NewState()
	state.SetWeight(sqlgenerator.WindowFunctionOverW, 0)
	for _, table := range c.tableMap {
		state.Tables = append(state.Tables, table)
	}

	query, err := sqlgenerator.Query.Eval(state)
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
