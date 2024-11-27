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
	"time"

	"github.com/PingCAP-QE/schrddl/dump"
	"github.com/PingCAP-QE/schrddl/norec"
	"github.com/PingCAP-QE/schrddl/reduce"
	"github.com/PingCAP-QE/schrddl/sqlgenerator"
	"github.com/PingCAP-QE/schrddl/util"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type CaseType int

const (
	CaseTypeNormal CaseType = iota
	CaseTypePlanCache
)

type testCase struct {
	cfg      *CaseConfig
	caseType CaseType

	dbname           string
	dbs              []*sql.DB
	caseIndex        int
	tables           map[string]*ddlTestTable
	schemas          map[string]*ddlTestSchema
	views            map[string]*ddlTestView
	tablesLock       sync.RWMutex
	schemasLock      sync.Mutex
	stop             int32
	lastDDLID        int
	charsets         []string
	charsetsCollates map[string][]string

	tableMap     map[string]*sqlgenerator.Table
	outputWriter *os.File

	tidbParser *parser.Parser

	// stat info
	queryPlanMap                      map[string]string
	originalSQL                       string
	reduceChangedSQL                  string
	reduceSQL                         string
	aggregationAsInnerSideOfIndexJoin int
	planUseMvIndex                    int
	cntOfOldOriginal                  int
	cntOfNewOriginal                  int
	cntOfOldReduce                    int
	cntOfNewReduce                    int

	// cert
	oldEstCntOriginal float64
	newEstCntOriginal float64
	oldEstCntReduce   float64
	newEstCntReduce   float64
	checkCERTCnt      int

	// tlp
	cntOfP    int
	cntOfN    int
	cntOfNull int
	cntOfAll  int
	nQuery    string
	nullQuery string
	allQuery  string
}

func (c *testCase) InitializeDB(dbname, dbDSN string) error {
	// Parallel send DDL request need more connection to send DDL request concurrently
	db0, err := OpenDB(dbDSN, 50)
	if err != nil {
		log.Fatalf("[ddl] create db client error %v", err)
	}
	db1, err := OpenDB(dbDSN, 50)
	if err != nil {
		log.Fatalf("[ddl] create db client error %v", err)
	}

	c.dbs = []*sql.DB{db0, db1}
	c.tidbParser = parser.New()
	c.dbname = dbname

	for _, db := range c.dbs {
		_, err := db.Exec("set @@max_execution_time=800000")
		if err != nil {
			return err
		}
		_, err = db.Exec("set @@group_concat_max_len=10240000")
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *testCase) DisableKVGC() {
	for _, db := range c.dbs {
		disableTiKVGC(db)
	}
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
	return util.FetchRowsWithDB(c.dbs[0], sql)
}

func (c *testCase) CheckData(tables []*model.TableInfo) error {
	for _, table := range tables {
		sql := fmt.Sprintf("select * from `%s`.`%s`", c.dbname, table.Name.L)
		rows1, err := util.FetchRowsWithDB(c.dbs[0], sql)
		if err != nil {
			return errors.Trace(err)
		}
		sql = fmt.Sprintf("select * from `%s`.`%s`", "testcache", table.Name)
		rows2, err := util.FetchRowsWithDB(c.dbs[0], sql)
		if err != nil {
			return errors.Trace(err)
		}
		same, err := util.CheckResults(rows1, rows2)
		if err != nil {
			log.Fatalf("Error check result")
		}
		if !same {
			return errors.Errorf("Table %s have different data", table.Name)
		}
	}

	return nil
}

// A special function to test instance plan cache
func (c *testCase) testPlanCache(ctx context.Context) error {
	state := sqlgenerator.NewState()

	dbNoCache, dbWithCache := c.dbs[0], c.dbs[1]

	prepareStmtCnt := 100
	for i := 0; i < prepareStmtCnt; i++ {
		startSQL, err := sqlgenerator.PlanCacheDataGen.Eval(state)
		if err != nil {
			return errors.Trace(err)
		}

		_, err1 := dbNoCache.Exec(startSQL)
		_, err2 := dbWithCache.Exec(startSQL)
		err = sqlgenerator.CheckError(err1, err2)
		if err != nil {
			return errors.Trace(err)
		}
	}

	tableMetas := c.fetchTableInfo(state)

	noCache, withCache := 0, 0
	for i := 0; i < 50000; i++ {
		if i%1000 == 0 {
			if err := c.CheckData(tableMetas); err != nil {
				return errors.Trace(err)
			}
			log.Info("Check table data passed")
		}

		useQuery := rand.Intn(2) == 0

		prepare, err := sqlgenerator.GeneratePrepare(state, useQuery)
		if err != nil {
			log.Warn("Generate prepare statement failed, err = %s", err.Error())
			continue
		}
		useCache, err := prepare.UsePlanCache(dbWithCache)
		if err != nil {
			log.Warn("Check use cache failed, err = %s", err.Error())
			continue
		}

		if !useCache {
			noCache++
			continue
		}

		withCache++
		if useQuery {
			err = prepare.CheckQuery(dbWithCache, dbNoCache)
		} else {
			err = prepare.CheckExec(dbWithCache, dbNoCache)
		}

		if err != nil {
			dir, err2 := c.dumpErrorTables(prepare.SQLNoCache)
			if err2 != nil {
				return errors.Trace(err)
			}
			err2 = prepare.RecordError(dir)
			if err2 != nil {
				return errors.Trace(err)
			}
			if strings.Contains(err.Error(), "different error") {
				continue
			}
		}
	}

	log.Infof("WithCache: %d, NoCache: %d\n", withCache, noCache)
	return nil
}

func (c *testCase) fetchTableInfo(state *sqlgenerator.State) []*model.TableInfo {
	tableMetas := make([]*model.TableInfo, 0)
	for i := 0; i < len(state.Tables); i++ {
		addr := c.cfg.DBAddr
		stateAddr := strings.Replace(addr, "4000", "10080", -1)
		path := fmt.Sprintf("%s/schema/%s/%s", stateAddr, c.dbname, state.Tables[i].Name)
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
	return tableMetas
}

// Dump related table data to local.
func (c *testCase) dumpErrorTables(sql string) (string, error) {
	tblNames, err := dump.ExtraFromSQL(sql)
	if err != nil {
		return "", err
	}
	pwd := os.Getenv("PWD")
	if GlobalOutPut != "" {
		pwd = filepath.Dir(GlobalOutPut)
	}
	bugNum := globalBugSeqNum.Add(1)
	dir := fmt.Sprintf("local://%s/bug-%s-%d", pwd, time.Now().Format("2006-01-02-15-04-05"), bugNum)
	return dir, dump.DumpToFile("test", tblNames, dir, c.cfg.DBAddr)
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

	if !EnableApproximateQuerySynthesis {
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

	db := c.dbs[0]

	prepareStmtCnt := 50
	for i := 0; i < prepareStmtCnt; i++ {
		startSQL, err := sqlgenerator.Start.Eval(state)
		if err != nil {
			return err
		}
		err = util.ExecSQLWithDB(db, startSQL)
		//println(fmt.Sprintf("%s;", startSQL))
		if err != nil {
			return err
		}
	}

	_, err := c.dbs[0].Exec("set @@max_execution_time=800000")
	if err != nil {
		return err
	}
	_, err = c.dbs[0].Exec("set @@group_concat_max_len=10240000")
	if err != nil {
		return err
	}

	tableMetas := c.fetchTableInfo(state)
	state.SetTableMeta(tableMetas)
	sqlgenerator.PrepareIndexJoinColumns(state)
	defer sqlgenerator.RemoveIndexJoinColumns(state)

	for cnt := 0; cnt < 20000; cnt++ {
		if cnt%10000 == 0 {
			err := c.readDataFromTiDB()
			if err != nil {
				if !util.DMLIgnoreError(err) {
					return errors.Trace(err)
				}
			}
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
			err = util.ExecSQLWithDB(db, dmlSQL)
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
				if !util.DMLIgnoreError(err) {
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
				} else {
					_, err = c.outputWriter.WriteString(
						fmt.Sprintf("old count of orginal SQL:%d, new count of orginal SQL:%d,\nold count of reduce SQL:%d, new count of reduce SQL:%d,\nold query of orginal SQL: %s\nnew query of reduce SQL: %s\nreduce query: %s\n\n\n",
							c.cntOfOldOriginal, c.cntOfNewOriginal, c.cntOfOldReduce, c.cntOfNewReduce, c.originalSQL, c.reduceChangedSQL, c.reduceSQL))
				}

				if _, err := c.dumpErrorTables(reduceSQL); err != nil {
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
		if err := c.executeAdminCheck(state); err != nil {
			return errors.Trace(err)
		}
		if err := c.readDataFromTiDB(); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (c *testCase) readDataFromTiDB() error {
	for _, table := range c.tables {
		sql := fmt.Sprintf("select * from `%s`", table.name)
		actualRows, err := util.FetchRowsWithDB(c.dbs[0], sql)
		if err != nil {
			return errors.Trace(err)
		}
		c.tableMap[table.name].Values = actualRows
	}

	return nil
}

func (c *testCase) executeAdminCheck(state *sqlgenerator.State) error {
	for _, table := range state.Tables {
		sql := fmt.Sprintf("ADMIN CHECK TABLE `%s`", table.Name)
		log.Infof("[ddl] [instance %d] %s", c.caseIndex, sql)
		if _, err := c.dbs[0].Exec(sql); err != nil {
			return errors.Annotatef(err, "Error when executing SQL: %s", sql)
		}
	}

	return nil
}
