package ddl

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/PingCAP-QE/schrddl/sqlgenerator"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/parser/model"
)

// The DDL test case is intended to test the correctness of DDL operations. It
// generates test cases by probability so that it should be run in background for
// enough time to see if there are any issues.
//
// The DDL test case have multiple go routines run in parallel, one for DML operations,
// other for DDL operations. The feature of each operation (for example, covering
// what kind of scenario) is determined and generated at start up time (See
// `generateDMLOps`, `generateDDLOps``), while the order of each operation is
// randomized in each round.
//
// If there are remaining DDL operations while all DML operations are performed, a
// new round of DML operations will be started (with new randomized order) and when
// all DDL operations are done, the remaining DML operations are discarded. vice
// versa.
//
// Since there are some conflicts between some DDL operations and DML operations,
// for example, inserting a row while removing a column may cause errors in
// inserting because of incorrect column numbers, some locks and some conflicting
// detections are introduced. The conflicting detection will ignore errors raised
// in such scenarios. In addition, the data in memory is stored by column instead
// of by row to minimize data conflicts in adding and removing columns.

type DDLCaseConfig struct {
	Concurrency     int         `toml:"concurrency"`
	MySQLCompatible bool        `toml:"mysql_compactible"`
	TablesToCreate  int         `toml:"tables_to_create"`
	TestTp          DDLTestType `toml:"test_type"`
}

type DDLTestType int

const (
	SerialDDLTest DDLTestType = iota
	ParallelDDLTest
)

type ExecuteDDLFunc func(*testCase, []ddlTestOpExecutor, func() error) error
type ExecuteDMLFunc func(*testCase, []dmlTestOpExecutor, func() error) error

type DDLCase struct {
	cfg   *DDLCaseConfig
	cases []*testCase
}

func (c *DDLCase) String() string {
	return "ddl"
}

func backgroundUpdateSeq(ctx context.Context, db *sql.DB) {
	tk := time.NewTicker(100 * time.Millisecond)
	var jobmeta []byte
	lastInternalJobId := int64(0)

	rows, err := db.QueryContext(context.Background(), "select job_meta from information_schema.ddl_jobs join mysql.tidb_ddl_history where ddl_jobs.job_id=tidb_ddl_history.job_id and JOB_TYPE = 'update tiflash replica status' order by ddl_jobs.job_id desc limit 1;")
	if err != nil {
		return
	}
	if rows.Next() {
		err = rows.Scan(&jobmeta)
		if err != nil {
			return
		}
		var job model.Job
		err = json.Unmarshal(jobmeta, &job)
		if err != nil {
			log.Infof("unmarshal error %s", err.Error())
		}
		lastInternalJobId = job.ID
	}

	err = rows.Close()
	if err != nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			log.Infof("Time is up, exit schrddl")
			return
		case <-tk.C:
		}
		rows, err := db.QueryContext(context.Background(), fmt.Sprintf("select job_meta from information_schema.ddl_jobs join mysql.tidb_ddl_history where ddl_jobs.job_id=tidb_ddl_history.job_id and JOB_TYPE = 'update tiflash replica status' and ddl_jobs.job_id > %d order by ddl_jobs.job_id limit 1;", lastInternalJobId))
		if err != nil {
			return
		}
		if !rows.Next() {
			continue
		}
		err = rows.Scan(&jobmeta)
		if err != nil {
			return
		}
		err = rows.Close()
		if err != nil {
			return
		}

		var job model.Job
		err = json.Unmarshal(jobmeta, &job)
		if err != nil {
			log.Infof("unmarshal error %s", err.Error())
		}
		seq := job.SeqNum
		globalDDLSeqNumMu.Lock()
		log.Warn("update seq", seq, globalDDLSeqNum, job.ID, lastInternalJobId)
		if seq == globalDDLSeqNum+1 {
			globalDDLSeqNum = seq
			lastInternalJobId = job.ID
		}
		globalDDLSeqNumMu.Unlock()
	}
}

func backgroundCheckDDLFinish(ctx context.Context, db *sql.DB, concurrency int) {
	tk := time.NewTicker(time.Duration(120+rand.Intn(20)*concurrency) * time.Second)
	for {
		select {
		case <-ctx.Done():
			log.Infof("Time is up, exit schrddl")
			return
		case <-tk.C:
			tk.Reset(time.Duration(120+rand.Intn(20)*concurrency) * time.Second)
		}
		if rand.Intn(3) != 0 {
			continue
		}
		log.Info("[ddl] check ddl jobs")
		// Stop new ddl.
		globalCheckDDLMu.Lock()
		startTime := time.Now()
		preJobCnt := 0
		for {
			row, err := db.Query("select count(*) from mysql.tidb_ddl_job")
			if err != nil {
				log.Warnf("read tidb_ddl_job failed", err)
				break
			}
			row.Next()
			var jobCnt int
			err = row.Scan(&jobCnt)
			if err != nil {
				log.Fatal("read job cnt from row failed")
			}
			err = row.Close()
			if err != nil {
				log.Warnf("close query failed", err)
				break
			}
			if jobCnt == 0 {
				break
			}
			timeout := time.Duration(concurrency*120)*time.Second + CheckDDLExtraTimeout
			if time.Since(startTime) > timeout && preJobCnt == jobCnt {
				log.Fatalf("cannot finish all DDL in %f seconds", timeout.Seconds())
			}
			preJobCnt = jobCnt
			time.Sleep(3 * time.Second)
		}
		globalCheckDDLMu.Unlock()
	}
}

func backgroundSetVariables(ctx context.Context, db *sql.DB) {
	tk := time.NewTicker(50 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			log.Infof("Time is up, exit schrddl")
			return
		case <-tk.C:
			state := sqlgenerator.NewState()
			setVar, err := sqlgenerator.SetVariable.Eval(state)
			if err != nil {
				db.Exec(setVar)
			}
		}
	}
}

// Execute executes each goroutine (i.e. `testCase`) concurrently.
func (c *DDLCase) Execute(ctx context.Context, dbss [][]*sql.DB, exeDDLFunc ExecuteDDLFunc, exeDMLFunc ExecuteDMLFunc) error {
	log.Infof("[%s] start to test...", c)
	defer func() {
		log.Infof("[%s] test end...", c)
	}()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		backgroundCheckDDLFinish(ctx, c.cases[0].dbs[0], c.cfg.Concurrency)
		wg.Done()
	}()
	go func() {
		backgroundUpdateSeq(ctx, c.cases[0].dbs[0])
		wg.Done()
	}()
	go func() {
		backgroundSetVariables(ctx, c.cases[0].dbs[0])
		wg.Done()
	}()

	for i := 0; i < c.cfg.Concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				err := c.cases[i].execute(ctx, exeDDLFunc, exeDMLFunc)
				if err != nil {
					for _, dbs := range dbss {
						for _, db := range dbs {
							disableTiKVGC(db)
						}
					}
					// os.Exit(-1)
					log.Fatalf("[error] [instance %d] ERROR: %s", i, errors.ErrorStack(err))
				}
				select {
				case <-ctx.Done():
					log.Infof("Time is up, exit schrddl")
					return
				default:
				}
			}
		}(i)
	}
	wg.Wait()
	return nil
}

// Initialize initializes all supported charsets, collates and each concurrent
// goroutine (i.e. `testCase`).
func (c *DDLCase) Initialize(ctx context.Context, dbss [][]*sql.DB, initDB string) error {
	charsets, charsetsCollates, err := getAllCharsetAndCollates(dbss[0][0])
	if err != nil {
		return errors.Trace(err)
	}
	for i := 0; i < c.cfg.Concurrency; i++ {
		c.cases[i].initDB = initDB
		c.cases[i].setCharsetsAndCollates(charsets, charsetsCollates)
		err := c.cases[i].initialize(dbss[i])
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// getAllCharsetAndCollates returns all allowable charsets and collates by executing a
// simple SQL query: `show collation`.
func getAllCharsetAndCollates(db *sql.DB) ([]string, map[string][]string, error) {
	sql := "show collation"
	rows, err := db.Query(sql)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	charsets := make([]string, 0)
	charsetsCollates := make(map[string][]string)
	for rows.Next() {
		var collate, charset, defaultValue, compiled, padAttribute string
		var id, sortLen int
		err := rows.Scan(&collate, &charset, &id, &defaultValue, &compiled, &sortLen, &padAttribute)
		if err != nil {
			return nil, nil, err
		}
		if collates, ok := charsetsCollates[charset]; ok {
			charsetsCollates[charset] = append(collates, collate)
		} else {
			charsets = append(charsets, charset)
			charsetsCollates[charset] = []string{collate}
		}
	}
	return charsets, charsetsCollates, nil
}

// NewDDLCase returns a DDLCase, which contains specified `testCase`s.
func NewDDLCase(cfg *DDLCaseConfig) *DDLCase {
	cases := make([]*testCase, cfg.Concurrency)
	for i := 0; i < cfg.Concurrency; i++ {
		cases[i] = &testCase{
			cfg:       cfg,
			tables:    make(map[string]*ddlTestTable),
			schemas:   make(map[string]*ddlTestSchema),
			views:     make(map[string]*ddlTestView),
			ddlOps:    make([]ddlTestOpExecutor, 0),
			dmlOps:    make([]dmlTestOpExecutor, 0),
			caseIndex: i,
			stop:      0,
			tableMap:  make(map[string]*sqlgenerator.Table),
		}
	}
	b := &DDLCase{
		cfg:   cfg,
		cases: cases,
	}
	return b
}

const (
	ddlTestValueNull    string = "NULL"
	ddlTestValueInvalid int32  = -99
)

type ddlTestOpExecutor struct {
	executeFunc func(interface{}, chan *ddlJobTask) error
	config      interface{}
	ddlKind     DDLKind
}

type dmlTestOpExecutor struct {
	prepareFunc func(interface{}, chan *dmlJobTask) error
	config      interface{}
}

type DMLKind int

const (
	dmlInsert DMLKind = iota
	dmlUpdate
	dmlDelete
	dmlSelect
)

type dmlJobArg unsafe.Pointer

type dmlJobTask struct {
	k            DMLKind
	tblInfo      *ddlTestTable
	sql          string
	assigns      []*ddlTestColumnDescriptor
	whereColumns []*ddlTestColumnDescriptor
	err          error
}

// initialize generates possible DDL and DML operations for one `testCase`.
// Different `testCase`s will be run in parallel according to the concurrent configuration.
func (c *testCase) initialize(dbs []*sql.DB) error {
	var err error
	c.dbs = dbs
	if err = c.generateDDLOps(); err != nil {
		return errors.Trace(err)
	}
	if err = c.generateDMLOps(); err != nil {
		return errors.Trace(err)
	}
	// Create 2 table before executes DDL & DML
	taskCh := make(chan *ddlJobTask, 2)
	c.prepareAddTable(nil, taskCh)
	c.prepareAddTable(nil, taskCh)
	if c.cfg.TestTp == SerialDDLTest {
		err = c.execSerialDDLSQL(taskCh)
		if err != nil {
			return errors.Trace(err)
		}
		err = c.execSerialDDLSQL(taskCh)
	} else {
		err = c.execParaDDLSQL(taskCh, len(taskCh))
	}
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// setCharsetsAndCollates sets the allowable character sets and associated collates for this testCase.
func (c *testCase) setCharsetsAndCollates(charsets []string, charsetsCollates map[string][]string) {
	c.charsets = charsets
	c.charsetsCollates = charsetsCollates
}

/*
ParallelExecuteOperations executes process:
1. Generate many kind of DDL SQLs
2. Parallel send every kind of DDL request to TiDB
3. Wait all DDL SQLs request finish
4. Send `admin show ddl jobs` request to TiDB to confirm parallel DDL requests execute order
5. Do the same DDL change on local with the same DDL requests executed order of TiDB
6. Judge the every DDL execution result of TiDB and local. If both of local and TiDB execute result are no wrong, or both are wrong it will be ok. Otherwise, It must be something wrong.
*/
func ParallelExecuteOperations(c *testCase, ops []ddlTestOpExecutor, postOp func() error) error {
	perm := rand.Perm(len(ops))
	taskCh := make(chan *ddlJobTask, len(ops))
	for _, idx := range perm {
		if c.isStop() {
			return nil
		}
		op := ops[idx]
		if rand.Float64() > mapOfDDLKindProbability[op.ddlKind] {
			continue
		}
		op.executeFunc(op.config, taskCh)
	}
	err := c.execParaDDLSQL(taskCh, len(taskCh))
	if err != nil {
		return errors.Trace(err)
	}
	close(taskCh)
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	return nil
}

func SerialExecuteOperations(c *testCase, ops []ddlTestOpExecutor, postOp func() error) error {
	perm := rand.Perm(len(ops))
	taskCh := make(chan *ddlJobTask, 1)
	for _, idx := range perm {
		if c.isStop() {
			return nil
		}
		op := ops[idx]
		// Test case weight.
		if rand.Float64() > mapOfDDLKindProbability[op.ddlKind] {
			continue
		}
		op.executeFunc(op.config, taskCh)
		err := c.execSerialDDLSQL(taskCh)
		if err != nil {
			return errors.Trace(err)
		}
	}
	close(taskCh)
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	return nil
}

func TransactionExecuteOperations(c *testCase, ops []dmlTestOpExecutor, postOp func() error) error {
	transactionOpsLen := rand.Intn(len(ops))
	if transactionOpsLen < 1 {
		transactionOpsLen = 1
	}
	taskCh := make(chan *dmlJobTask, len(ops))
	opNum := 0
	perm := rand.Perm(len(ops))
	for i, idx := range perm {
		if c.isStop() {
			return nil
		}
		op := ops[idx]
		err := op.prepareFunc(op.config, taskCh)
		if err != nil {
			if err.Error() != "Conflict operation" {
				return errors.Trace(err)
			}
			continue
		}
		opNum++
		if opNum >= transactionOpsLen {
			err = c.execDMLInTransactionSQL(taskCh)
			if err != nil {
				return errors.Trace(err)
			}
			transactionOpsLen = rand.Intn(len(ops))
			if transactionOpsLen < 1 {
				transactionOpsLen = 1
			}
			if transactionOpsLen > (len(ops) - i) {
				transactionOpsLen = len(ops) - i
			}
			opNum = 0
			if postOp != nil && rand.Intn(5) == 0 {
				err = postOp()
				if err != nil {
					return errors.Trace(err)
				}
			}
			time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
		}
	}
	return nil
}

func SerialExecuteDML(c *testCase, ops []dmlTestOpExecutor, postOp func() error) error {
	perm := rand.Perm(len(ops))
	taskCh := make(chan *dmlJobTask, 1)
	for _, idx := range perm {
		if c.isStop() {
			return nil
		}
		op := ops[idx]
		err := op.prepareFunc(op.config, taskCh)
		if err != nil {
			if err.Error() != "Conflict operation" {
				return errors.Trace(err)
			}
			continue
		}
		err = c.execSerialDMLSQL(taskCh)
		if err != nil {
			return errors.Trace(err)
		}
	}
	close(taskCh)
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	return nil
}

// execute iterates over two list of operations concurrently, one is
// ddl operations, one is dml operations.
// When one list completes, it starts over from the beginning again.
// When both of them ONCE complete, it exits.
func (c *testCase) execute(ctx context.Context, executeDDL ExecuteDDLFunc, exeDMLFunc ExecuteDMLFunc) error {
	var (
		ddlAllComplete int32 = 0
		dmlAllComplete int32 = 0
	)

	err := parallel(func() error {
		var err1 error
		for {
			go func() {
				tk := time.Tick(time.Second)
				select {
				case <-ctx.Done():
					return
				case <-tk:
					rs, err := c.dbs[0].Query("select `job_id` from information_schema.ddl_jobs limit 1")
					if err == nil {
						jobID := 0
						rs.Next()
						err = rs.Scan(&jobID)
						if err != nil {
							log.Errorf("unexpected error when scan", err)
							return
						}
						err = rs.Close()
						if err != nil {
							log.Errorf("unexpected error when close", err)
							return
						}
						globalCancelMu.Lock()
						_, err := c.dbs[0].Exec(fmt.Sprintf("admin cancel ddl jobs %d", jobID))
						globalCancelMu.Unlock()
						if err != nil {
							log.Errorf("unexpected error when execute cancel ddl", err)
							return
						}
					}
					_, err = c.dbs[0].Exec("admin show ddl jobs")
					if err != nil {
						log.Errorf("unexpected error when execute admin show ddl jobs", err)
						return
					}
				}
			}()
			c.schemasLock.Lock()
			err1 = executeDDL(c, c.ddlOps, nil)
			atomic.StoreInt32(&ddlAllComplete, 1)
			c.schemasLock.Unlock()
			if atomic.LoadInt32(&ddlAllComplete) != 0 && atomic.LoadInt32(&dmlAllComplete) != 0 || err1 != nil {
				break
			}
			select {
			case <-ctx.Done():
				log.Infof("Time is up, exit schrddl")
				return nil
			default:
			}
		}
		return errors.Trace(err1)
	}, func() error {
		var err2 error
		for {
			err2 = exeDMLFunc(c, c.dmlOps, func() error {
				return nil
			})
			atomic.StoreInt32(&dmlAllComplete, 1)
			if atomic.LoadInt32(&ddlAllComplete) != 0 && atomic.LoadInt32(&dmlAllComplete) != 0 || err2 != nil {
				break
			}
			select {
			case <-ctx.Done():
				log.Infof("Time is up, exit schrddl")
				return nil
			default:
			}
		}
		return errors.Trace(err2)
	})

	if err != nil {
		ddlFailedCounter.Inc()
		return errors.Trace(err)
	}

	log.Infof("[ddl] [instance %d] Round completed", c.caseIndex)
	log.Infof("[ddl] [instance %d] Executing post round operations...", c.caseIndex)

	if !c.cfg.MySQLCompatible {
		err := c.executeAdminCheck()
		if err != nil {
			return errors.Trace(err)
		}
		err = c.readDataFromTiDB()
		if err != nil {
			if !dmlIgnoreError(err) {
				return errors.Trace(err)
			}
		}
	}

	return nil
}

func (c *testCase) readDataFromTiDB() error {
	if len(c.tables) == 0 {
		return nil
	}

	sql := "select * from "
	for _, table := range c.tables {
		readSql := sql + fmt.Sprintf("`%s`", table.name)
		dbIdx := rand.Intn(len(c.dbs))
		db := c.dbs[dbIdx]
		rows, err := db.Query(readSql)
		if err != nil {
			return err
		}
		defer func() {
			rows.Close()
		}()
		metaCols := make([]*ddlTestColumn, 0)
		for ite := table.columns.Iterator(); ite.Next(); {
			metaCols = append(metaCols, ite.Value().(*ddlTestColumn))
		}
		// Read all rows.
		var actualRows [][]string
		for rows.Next() {
			cols, err1 := rows.Columns()
			if err1 != nil {
				return errors.Trace(err)
			}

			//log.Infof("[ddl] [instance %d] rows.Columns():%v, len(cols):%v", c.caseIndex, cols, len(cols))

			// See https://stackoverflow.com/questions/14477941/read-select-columns-into-string-in-go
			rawResult := make([][]byte, len(cols))
			result := make([]string, len(cols))
			dest := make([]interface{}, len(cols))
			for i := range rawResult {
				dest[i] = &rawResult[i]
			}

			err1 = rows.Scan(dest...)
			if err1 != nil {
				return errors.Trace(err)
			}

			for i, raw := range rawResult {
				if raw == nil {
					result[i] = ddlTestValueNull
				} else {
					result[i] = fmt.Sprintf("'%s'", string(raw))
					//if typeNeedQuota(metaCols[i].k) {
					//	result[i] = fmt.Sprintf("'%s'", string(raw))
					//}
					//result[i] = string(raw)
				}
			}

			actualRows = append(actualRows, result)
		}
		c.tableMap[table.name].Values = actualRows
	}

	return nil
}

func readData(ctx context.Context, conn *sql.Conn, query string) ([][]string, error) {
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Annotatef(err, "Error when executing SQL: %s\n%s", query)
	}
	defer func() {
		rows.Close()
	}()
	//metaCols := make([]*ddlTestColumn, 0)
	//for ite := table.columns.Iterator(); ite.Next(); {
	//	metaCols = append(metaCols, ite.Value().(*ddlTestColumn))
	//}
	// Read all rows.
	var actualRows [][]string
	for rows.Next() {
		cols, err1 := rows.Columns()
		if err1 != nil {
			return nil, errors.Trace(err)
		}

		// See https://stackoverflow.com/questions/14477941/read-select-columns-into-string-in-go
		rawResult := make([][]byte, len(cols))
		result := make([]string, len(cols))
		dest := make([]interface{}, len(cols))
		for i := range rawResult {
			dest[i] = &rawResult[i]
		}

		err1 = rows.Scan(dest...)
		if err1 != nil {
			return nil, errors.Trace(err)
		}

		for i, raw := range rawResult {
			if raw == nil {
				result[i] = ddlTestValueNull
			} else {
				result[i] = fmt.Sprintf("'%s'", string(raw))
				//if typeNeedQuota(metaCols[i].k) {
				//	result[i] = fmt.Sprintf("'%s'", string(raw))
				//}
				//result[i] = string(raw)
			}
		}

		actualRows = append(actualRows, result)
	}
	return actualRows, err
}

func trimValue(tp int, val []byte) string {
	// a='{"DnOJQOlx":52,"ZmvzPtdm":82}'
	// eg: set a={"a":"b","b":"c"}
	//     get a={"a": "b", "b": "c"} , so have to remove the space
	if tp == KindJSON {
		for i := 1; i < len(val)-2; i++ {
			if val[i-1] == '"' && val[i] == ':' && val[i+1] == ' ' {
				val = append(val[:i+1], val[i+2:]...)
			}
			if val[i-1] == ',' && val[i] == ' ' && val[i+1] == '"' {
				val = append(val[:i], val[i+1:]...)
			}
		}
	}
	return string(val)
}

func (c *testCase) executeAdminCheck() error {
	if len(c.tables) == 0 {
		return nil
	}

	// build SQL
	sql := "ADMIN CHECK TABLE "
	dbIdx := rand.Intn(len(c.dbs))
	db := c.dbs[dbIdx]
	for _, table := range c.tables {
		_, err := db.Exec(fmt.Sprintf("ADMIN CHECK TABLE `%s`", table.name))
		if err != nil {
			if dmlIgnoreError(err) {
				return nil
			}
			return errors.Annotatef(err, "Error when executing SQL: %s", sql)
		}
	}
	// execute
	log.Infof("[ddl] [instance %d]", c.caseIndex)
	return nil
}
