package framework

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	crc322 "hash/crc32"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/PingCAP-QE/schrddl/dump"
	"github.com/PingCAP-QE/schrddl/norec"
	"github.com/PingCAP-QE/schrddl/reduce"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"

	"github.com/PingCAP-QE/schrddl/sqlgenerator"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/pkg/parser/model"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
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

type CaseConfig struct {
	Concurrency     int
	MySQLCompatible bool
	TablesToCreate  int
	TestTp          DDLTestType
	dbAddr          string
}

var globalBugSeqNum int64 = 0
var globalRunQueryCnt atomic.Int64
var globalSuccessQueryCnt atomic.Int64

type DDLTestType int

const (
	SerialDDLTest DDLTestType = iota
	ParallelDDLTest
)

type DDLCase struct {
	cfg   *CaseConfig
	cases []*testCase
}

func (c *DDLCase) String() string {
	return "ddl"
}

func (c *DDLCase) statloop() {
	tick := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-tick.C:
			subcaseStat := make([]string, len(c.cases))
			subcaseUseMvindex := make([]string, len(c.cases))
			subcaseUseCERT := make([]string, len(c.cases))
			subcaseUseAggIndexJoin := make([]string, len(c.cases))
			for _, c := range c.cases {
				subcaseStat = append(subcaseStat, fmt.Sprintf("%d", len(c.queryPlanMap)))
				subcaseUseMvindex = append(subcaseUseMvindex, fmt.Sprintf("%d", c.planUseMvIndex))
				subcaseUseCERT = append(subcaseUseCERT, fmt.Sprintf("%d", c.checkCERTCnt))
				subcaseUseAggIndexJoin = append(subcaseUseAggIndexJoin, fmt.Sprintf("%d", c.aggregationAsInnerSideOfIndexJoin))

				//i := 0
				//for k, v := range c.queryPlanMap {
				//	logutil.BgLogger().Warn("sample query plan", zap.String("plan", k), zap.String("query", v))
				//	i++
				//	if i >= 10 {
				//		break
				//	}
				//}
			}

			logutil.BgLogger().Info("stat", zap.Int64("run query:", globalRunQueryCnt.Load()),
				zap.Int64("success:", globalSuccessQueryCnt.Load()),
				zap.Int64("fetch json row val:", sqlgenerator.GlobalFetchJsonRowValCnt.Load()),
				zap.Strings("unique query plan", subcaseStat),
				zap.Strings("use mv index", subcaseUseMvindex),
				zap.Strings("use CERT", subcaseUseCERT),
				zap.Strings("use agg index join", subcaseUseAggIndexJoin),
			)
		}
	}
}

// Execute executes each goroutine (i.e. `testCase`) concurrently.
func (c *DDLCase) Execute(ctx context.Context, dbss [][]*sql.DB) error {
	log.Infof("[%s] start to test...", c)
	go func() {
		c.statloop()
	}()
	defer func() {
		log.Infof("[%s] test end...", c)
	}()
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				err := c.cases[i].execute(ctx)
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
// simple SQL query: `show charset`.
func getAllCharsetAndCollates(db *sql.DB) ([]string, map[string][]string, error) {
	sql := "show charset"
	rows, err := db.Query(sql)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	charsets := make([]string, 0)
	charsetsCollates := make(map[string][]string)
	for rows.Next() {
		var collate, charset, description string
		var maxLen int
		err := rows.Scan(&charset, &description, &collate, &maxLen)
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
func NewDDLCase(cfg *CaseConfig) *DDLCase {
	cases := make([]*testCase, cfg.Concurrency)
	fileName := "result-" + time.Now().Format("2006-01-02-15-04-05")

	if GlobalOutPut != "" {
		fileName = filepath.Dir(GlobalOutPut) + "/" + fileName
	}
	outputfile, err := os.Create(fileName)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < cfg.Concurrency; i++ {
		cases[i] = &testCase{
			cfg:          cfg,
			tables:       make(map[string]*ddlTestTable),
			schemas:      make(map[string]*ddlTestSchema),
			views:        make(map[string]*ddlTestView),
			caseIndex:    i,
			stop:         0,
			tableMap:     make(map[string]*sqlgenerator.Table),
			outputWriter: outputfile,
			queryPlanMap: make(map[string]string),
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
	//var err error
	c.dbs = dbs
	c.tidbParser = parser.New()
	return nil
}

// setCharsetsAndCollates sets the allowable character sets and associated collates for this testCase.
func (c *testCase) setCharsetsAndCollates(charsets []string, charsetsCollates map[string][]string) {
	c.charsets = charsets
	c.charsetsCollates = charsetsCollates
}

func (c *testCase) checkError(err error) error {
	if err != nil {
		if c.cfg.MySQLCompatible {
			if strings.Contains(err.Error(), "Duplicate entry") {
				return nil
			}
		}
		return errors.Trace(err)
	}
	return nil
}

func (c *testCase) execSQL(sql string) error {
	_, err := c.dbs[0].Exec(sql)
	if err != nil && dmlIgnoreError(err) || ddlIgnoreError(err) {
		return nil
	}
	if strings.Contains(err.Error(), "plan not match") {
		_, err = c.dbs[0].Exec(sql)
		return err
	}
	return errors.Trace(err)
}

func (c *testCase) execQueryForPlanEstCnt(sql string) (float64, error) {
	sql = "explain format='brief' " + sql
	rows, err := c.dbs[0].Query(sql)
	if err != nil {
		return 0, err
	}
	defer func() {
		rows.Close()
	}()
	var id string
	var estRows float64
	var task string
	var accessObject string
	var operatorInfo string
	rows.Next()
	err = rows.Scan(&id, &estRows, &task, &accessObject, &operatorInfo)
	return estRows, err
}

func (c *testCase) execQueryForCnt(sql string) (int, error) {
	rows, err := c.dbs[0].Query(sql)
	if err != nil {
		return 0, err
	}
	defer func() {
		rows.Close()
	}()
	rs := 0
	for rows.Next() {
		rs++
	}
	if rows.Err() != nil {
		return 0, rows.Err()
	}
	return rs, nil
}

func (c *testCase) execQueryAndCheckEmpty(sql string) bool {
	rows, err := c.dbs[0].Query(sql)
	if err != nil {
		logutil.BgLogger().Error("unexpected error", zap.Error(err))
		return false
	}
	defer func() {
		rows.Close()
	}()

	result := !rows.Next()

	cnt2 := 0
	if !result {
		cols, _ := rows.Columns()
		rawResult := make([][]byte, len(cols))
		dest := make([]interface{}, len(cols))
		for i := range rawResult {
			dest[i] = &rawResult[i]
		}

		err := rows.Scan(dest...)
		if err != nil {
			panic(err)
		}

		for rows.Next() {
			cnt2++
		}

		for _, r := range rawResult {
			c.outputWriter.WriteString(fmt.Sprintf("%s\n", string(r)))
		}

		cnt, _ := c.execQueryForCnt(sql)

		rs, _ := c.execQuery(sql)

		c.outputWriter.WriteString(fmt.Sprintf("sql: %s, cnt: %d, cnt2: %d, cnt3: %d \n", sql, cnt, cnt2+1, len(rs)))
		if rows.Err() != nil {
			c.outputWriter.WriteString(fmt.Sprintf("err %s \n", rows.Err().Error()))
		}
	}
	return result
}

func (c *testCase) execQueryForCRC32(sql string) (map[uint32]struct{}, error) {
	rows, err := c.dbs[0].Query(sql)
	if err != nil {
		return nil, err
	}
	defer func() {
		rows.Close()
	}()

	// Read all rows.
	crc32s := make(map[uint32]struct{}, 0)

	for rows.Next() {
		cols, err1 := rows.Columns()
		if err1 != nil {
			return nil, err
		}

		//log.Infof("[ddl] [instance %d] rows.Columns():%v, len(cols):%v", c.caseIndex, cols, len(cols))

		// See https://stackoverflow.com/questions/14477941/read-select-columns-into-string-in-go
		rawResult := make([][]byte, len(cols))
		result := make([]string, len(cols))
		dest := make([]interface{}, len(cols))
		ct, _ := rows.ColumnTypes()
		for i := range rawResult {
			dest[i] = &rawResult[i]
		}

		err1 = rows.Scan(dest...)
		if err1 != nil {
			return nil, err
		}

		for i, raw := range rawResult {
			if raw == nil {
				result[i] = ddlTestValueNull
			} else {
				//logutil.BgLogger().Warn("type to debug", zap.String("type", ct[i].DatabaseTypeName()))
				if strings.EqualFold(ct[i].DatabaseTypeName(), "double") {
					result[i] = fmt.Sprintf("'%s'", RoundToOneDecimals(string(raw)))
				} else {
					result[i] = fmt.Sprintf("'%s'", strings.ToLower(string(raw)))
				}
				//if typeNeedQuota(metaCols[i].k) {
				//	result[i] = fmt.Sprintf("'%s'", string(raw))
				//}
				//result[i] = string(raw)
			}
		}

		crc32 := crc322.NewIEEE()
		for _, r := range result {
			crc32.Write([]byte(r))
		}

		sum := crc32.Sum32()
		//logutil.BgLogger().Info("crc32", zap.Uint32("crc32", sum), zap.String("data", fmt.Sprintf("%v", result)))

		crc32s[sum] = struct{}{}
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return crc32s, nil
}

func (c *testCase) execQuery(sql string) ([][]string, error) {
	rows, err := c.dbs[0].Query(sql)
	if err != nil {
		return nil, err
	}
	defer func() {
		rows.Close()
	}()

	// Read all rows.
	var actualRows [][]string
	for rows.Next() {
		cols, err1 := rows.Columns()
		if err1 != nil {
			return nil, err
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
			return nil, err
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
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return actualRows, nil
}

func mapLess(m1, m2 map[uint32]struct{}) bool {
	for k := range m2 {
		if _, ok := m1[k]; !ok {
			return true
		}
	}
	return false
}

func mapMore(m1, m2 map[uint32]struct{}) bool {
	for k := range m1 {
		if _, ok := m2[k]; !ok {
			return true
		}
	}
	return false
}

type checker interface {
	check(sql string, isReduce bool) (bool, error)
}

// execute iterates over two list of operations concurrently, one is
// ddl operations, one is dml operations.
// When one list completes, it starts over from the beginning again.
// When both of them ONCE complete, it exits.
func (c *testCase) execute(ctx context.Context) error {
	state := sqlgenerator.NewState()
	state.SetWeight(sqlgenerator.RenameColumn, 0)
	state.SetWeight(sqlgenerator.WindowFunction, 0)
	state.SetWeight(sqlgenerator.WindowClause, 0)
	state.SetWeight(sqlgenerator.WindowFunctionOverW, 0)
	state.SetWeight(sqlgenerator.WhereClause, 1)
	//state.SetWeight(sqlgenerator.Limit, 0)
	state.SetWeight(sqlgenerator.UnionSelect, 0)
	//state.SetWeight(sqlgenerator.PartitionDefinitionHash, 1000)
	state.SetWeight(sqlgenerator.PartitionDefinitionKey, 0)
	//state.SetWeight(sqlgenerator.PartitionDefinitionRange, 1000)

	//state.SetWeight(sqlgenerator.AggSelect, 0)

	// Sub query is hard for NoREC
	//state.SetWeight(sqlgenerator.SubSelect, 0)

	if !EnableApproximateQuerySynthesis && !EnableEET {
		state.SetWeight(sqlgenerator.ScalarSubQuery, 0)
		state.SetWeight(sqlgenerator.SubSelect, 0)
	}

	// bug
	state.SetWeight(sqlgenerator.ColumnDefinitionTypesEnum, 0)
	state.SetWeight(sqlgenerator.ColumnDefinitionTypesSet, 0)
	state.SetWeight(sqlgenerator.ColumnDefinitionTypesYear, 0)
	state.SetWeight(sqlgenerator.ColumnDefinitionTypesBit, 0)

	//state.Hook().Append(sqlgenerator.NewFnHookDebug())

	//state.SetWeight(sqlgenerator.ColumnDefinitionTypesJSON, 0)
	//state.SetWeight(sqlgenerator.JSONPredicate, 0)

	prepareStmtCnt := 50
	for i := 0; i < prepareStmtCnt; i++ {
		startSQL, err := sqlgenerator.Start.Eval(state)
		if err != nil {
			return err
		}
		err = c.execSQL(startSQL)
		//println(fmt.Sprintf("%s;", startSQL))
		if err != nil {
			return err
		}
	}

	err := c.execSQL("set @@max_execution_time=800000")
	if err != nil {
		return err
	}
	err = c.execSQL("set @@group_concat_max_len=10240000")
	if err != nil {
		return err
	}

	tableMetas := make([]*model.TableInfo, 0)
	for i := 0; i < len(state.Tables); i++ {
		addr := c.cfg.dbAddr
		stateAddr := strings.Replace(addr, "4000", "10080", -1)
		path := fmt.Sprintf("%s/schema/%s/%s", stateAddr, c.initDB, state.Tables[i].Name)
		rawMeta, err := exec.Command("curl", path).Output()
		if err != nil {
			log.Infof("curl error %s", err.Error())
			continue
		}
		var meta model.TableInfo
		err = json.Unmarshal(rawMeta, &meta)
		if err != nil {
			logutil.BgLogger().Warn("unmarshal error", zap.Error(err), zap.String("table name", state.Tables[i].Name))
			state.Tables = append(state.Tables[:i], state.Tables[i+1:]...)
			i--
			continue
		}
		tableMetas = append(tableMetas, &meta)
	}
	for _, table := range tableMetas {
		log.Infof("table %s", table.Name.O)
	}
	log.Infof("tableMetas %d", len(tableMetas))
	state.SetTableMeta(tableMetas)
	sqlgenerator.PrepareIndexJoinColumns(state)
	defer sqlgenerator.RemoveIndexJoinColumns(state)

	cnt := 0

	for {
		cnt++
		if cnt%10000 == 0 {
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
		if cnt%10000 == 0 && rand.Intn(2) == 0 {
			break
		}

		// NoREC
		rewriter := &norec.NoRecRewriter{}
		var sb strings.Builder

		doDML := rand.Intn(2) == 0
		if doDML {
			dmlSQL, err := sqlgenerator.DMLStmt.Eval(state)
			//println(fmt.Sprintf("%s;", dmlSQL))
			if err != nil {
				return err
			}
			if rand.Intn(100) == 0 {
				dmlSQL, err = sqlgenerator.SetTiFlashReplica.Eval(state)
				if err != nil {
					return err
				}
			} else if rand.Intn(15) == 0 {
				dmlSQL, err = sqlgenerator.SetVariable.Eval(state)
				if err != nil {
					return err
				}
			}
			if rand.Intn(100) == 0 {
				dmlSQL, err = sqlgenerator.AnalyzeTable.Eval(state)
				if err != nil {
					return err
				}
			}
			err = c.execSQL(dmlSQL)
			if err != nil {
				return err
			}
		} else {
			var ck checker
			if EnableApproximateQuerySynthesis {
				ck = &pinoloChecker{c: c}
			} else if EnableCERT {
				ck = &certChecker{c: c}
			} else if EnableTLP {
				ck = &tlpChecker{c: c}
			} else if EnableEET {
				ck = &eetChecker{c: c}
			} else {
				ck = &norecChecker{c: c, rewriter: *rewriter, sb: sb}
			}

			fn := sqlgenerator.QueryOrCTE
			if EnableCERT {
				fn = sqlgenerator.Query
			}
			querySQL, err := fn.Eval(state)
			if err != nil {
				return errors.Trace(err)
			}

			//println(fmt.Sprintf("%s;", querySQL))

			found, err := ck.check(querySQL, false)
			if err != nil {
				if !dmlIgnoreError(err) {
					if strings.Contains(err.Error(), "plan not match") {
						_, err = c.dbs[0].Exec(querySQL)
						if err == nil {
							continue
						}
					}
					return errors.Trace(err)
				} else {
					continue
				}
			}
			if found {
				reduceSQL := reduce.ReduceSQL(ck.check, querySQL)
				if EnableTLP {
					_, err = c.outputWriter.WriteString(
						fmt.Sprintf("old count of predicate reduce SQL:%d, count of non-predicate reduce SQL:%d,\ncount of null-predicate reduce SQL:%d, count of all-predicate reduce SQL:%d\n"+
							"query of orginal SQL: %s\n"+
							"predicate reduce query: %s\n"+
							"negative predicate reduce query: %s\n"+
							"isnull predicate reduce query: %s\n"+
							"all predicate reduce query: %s\n"+
							"\n\n",
							c.cntOfP, c.cntOfN, c.cntOfNull, c.cntOfAll, c.originalSQL, c.reduceSQL, c.nQuery, c.nullQuery, c.allQuery))
				} else if EnableCERT {
					_, err = c.outputWriter.WriteString(
						fmt.Sprintf("old count of orginal SQL:%f, new count of orginal SQL:%f,\nold count of reduce SQL:%f, new count of reduce SQL:%f,\nold query of orginal SQL: %s\nnew query of reduce SQL: %s\nreduce query: %s\n\n\n",
							c.oldEstCntOriginal, c.newEstCntOriginal, c.oldEstCntReduce, c.newEstCntReduce, c.originalSQL, c.reduceChangedSQL, c.reduceSQL))
				} else if EnableEET {
					_, err = c.outputWriter.WriteString(
						fmt.Sprintf("old count of orginal SQL:%d, new count of orginal SQL:%d,\nold count of reduce SQL:%d, new count of reduce SQL:%d,\nold query of orginal SQL: %s\nnew query of reduce SQL: %s\nreduce query: %s\n\n\n",
							c.cntOfOldOriginal, c.cntOfNewOriginal, c.cntOfOldReduce, c.cntOfNewReduce, c.originalSQL, c.reduceChangedSQL, c.reduceSQL))
				} else {
					_, err = c.outputWriter.WriteString(
						fmt.Sprintf("old count of orginal SQL:%d, new count of orginal SQL:%d,\nold count of reduce SQL:%d, new count of reduce SQL:%d,\nold query of orginal SQL: %s\nnew query of reduce SQL: %s\nreduce query: %s\n\n\n",
							c.cntOfOldOriginal, c.cntOfNewOriginal, c.cntOfOldReduce, c.cntOfNewReduce, c.originalSQL, c.reduceChangedSQL, c.reduceSQL))
				}
				globalBugSeqNum++
				num := globalBugSeqNum

				// Dump data.
				tblNames, err := dump.ExtraFromSQL(reduceSQL)
				if err != nil {
					return err
				}
				pwd := os.Getenv("PWD")
				if GlobalOutPut != "" {
					pwd = filepath.Dir(GlobalOutPut)
				}
				err = dump.DumpToFile("test", tblNames, fmt.Sprintf("local://%s/bug-%s-%d", pwd, time.Now().Format("2006-01-02-15-04-05"), num), c.cfg.dbAddr)
				if err != nil {
					return err
				}
				TestFail = true
				break
			}
		}
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

			log.Infof("[ddl] [instance %d] rows.Columns():%v, len(cols):%v", c.caseIndex, cols, len(cols))

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
		return nil, errors.Annotatef(err, "Error when executing SQL: %s\n", query)
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
	i := 0
	for _, table := range c.tables {
		if i > 0 {
			sql += ", "
		}
		sql += fmt.Sprintf("`%s`", table.name)
		i++
	}
	dbIdx := rand.Intn(len(c.dbs))
	db := c.dbs[dbIdx]
	// execute
	log.Infof("[ddl] [instance %d] %s", c.caseIndex, sql)
	_, err := db.Exec(sql)
	if err != nil {
		if dmlIgnoreError(err) {
			return nil
		}
		return errors.Annotatef(err, "Error when executing SQL: %s", sql)
	}
	return nil
}
