package sqlgenerator

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/PingCAP-QE/schrddl/util"
	sqlutil "github.com/PingCAP-QE/schrddl/util"
	"github.com/juju/errors"
	"github.com/ngaut/log"
)

func isIgnoredError(s string) bool {
	return s == "" ||
		strings.Contains(s, "cannot be pushed down") ||
		strings.Contains(s, "Truncated incorrect") ||
		strings.Contains(s, "Duplicate entry") ||
		strings.Contains(s, "Incorrect datetime value")
}

func CheckError(err1, err2 error) error {
	if err1 == nil && err2 == nil {
		return nil
	}

	errStr1, errStr2 := "", ""
	if err1 != nil {
		errStr1 = err1.Error()
	}
	if err2 != nil {
		errStr2 = err2.Error()
	}

	// Filter some errors
	if isIgnoredError(errStr1) && isIgnoredError(errStr2) {
		return nil
	}

	// Since the SQL is randomly generated, it may inherently fail to execute.
	// In this case, we assume both sessions will return the same error.
	if err1 == nil || err2 == nil {
		log.Warn("Two sessions returns different error")
		log.Warnf("Error1: %v", err1)
		log.Warnf("Error2: %v", err2)
		return errors.Errorf("Two sessions return different error")
	}

	// TODO(joechenrh): pass dbname here
	errStr2 = strings.ReplaceAll(errStr2, "testcache", "test")
	if errStr1 != errStr2 {
		log.Warn("Two sessions returns different error")
		log.Warnf("Error1: %v", err1)
		log.Warnf("Error2: %v", err2)
		return errors.Errorf("Two sessions return different error")
	}

	// Two sessions return same error, check if it can be ignroed.
	if !util.DMLIgnoreError(err1) && !util.DDLIgnoreError(err1) {
		log.Warnf("Two sessions get same error %v", err1)
		return errors.Trace(err1)
	}
	return nil
}

func checkLastUseCache(conn *sql.Conn) bool {
	rows, err := util.FetchRowsWithConn(conn, "select @@last_plan_from_cache")
	if err != nil {
		log.Fatalf("Error fetch rows %v", err)
	}
	return rows[0][0] == "'1'"
}

const Placeholder = "?"

type ValueGenerator interface {
	GenMismatch() string
	GenMatch() string
}

// ColummGenerator is used to generate random value based on given table and column.
type ColumnGenerator struct {
	column *Column
	table  *Table
}

func (g *ColumnGenerator) useTable() bool {
	return len(g.table.Values) > 0 && rand.Intn(3) == 0
}

func (g *ColumnGenerator) GenMatch() string {
	if g.useTable() {
		if v := g.table.GetRandRowVal(g.column); len(v) != 0 {
			return v
		}
	}
	return g.column.RandomValue()
}

func (g *ColumnGenerator) GenMismatch() (v string) {
	return g.column.Tp.RandomMismatch()
}

// ArrayValueGenerator is used to generate random value in column type.
type ArrayValueGenerator struct {
	column *Column
	table  *Table
}

func (g *ArrayValueGenerator) useTable() bool {
	return len(g.table.Values) > 0 && rand.Intn(3) == 0
}

func (g *ArrayValueGenerator) GenMatch() string {
	if g.useTable() {
		if v := g.table.GetRandRowVal(g.column); len(v) != 0 {
			return v
		}
	}
	return RandomValueWithType(g.column.SubType)
}

func (g *ArrayValueGenerator) GenMismatch() (v string) {
	// We don't generate mismatch type of value in JSON
	return g.GenMatch()
}

// Currently, SimpleGenerator is only used in limit N.
type SimpleGenerator struct {
	gen func() string
}

func (g *SimpleGenerator) GenMatch() string {
	return g.gen()
}

func (g *SimpleGenerator) GenMismatch() string {
	return g.gen()
}

// RunAndCheckPlanCache checks whether this statement can be used in plan cache.
//
// Return:
// 1. whether this statement can use plan cache
// 2. error
func RunAndCheckPlanCache(sql string, db *sql.DB) (bool, error) {
	// The generated statement may already have errors, check it first.
	if _, err := db.Exec(sql); err != nil {
		if sqlutil.DMLIgnoreError(err) {
			return true, nil
		}
		return false, errors.Trace(err)
	}

	rows, err := util.FetchRowsWithDB(db, "SHOW WARNINGS")
	if err != nil {
		return false, errors.Trace(err)
	}
	for _, row := range rows {
		if strings.Contains(row[2], "skip plan-cache") {
			return true, nil
		}
	}

	return false, nil
}

type Prepare struct {
	name        string
	generators  []ValueGenerator
	originalSQL string
	prepareSQL  string
	executeSQL  string
	setSQL      string
	SQLNoCache  string
	setSQLs     []string

	// error for no-cache execution and with-cache execution
	err1 error
	err2 error
}

func NewPrepare() *Prepare {
	return &Prepare{
		name: randomStringRunes(16, false),
	}
}

func (p *Prepare) RecordStack() int {
	return len(p.generators)
}

func (p *Prepare) PopStack(l int) {
	if l < len(p.generators) {
		p.generators = p.generators[:l]
	}
}

func (p *Prepare) RecordError(dir string) error {
	dir = dir[8:]
	filePath := filepath.Join(dir, "prepare.sql")

	content := fmt.Sprintf("SQLs:\n\n%s\n%s\n%s\nerr1:%v\nerr2:%v\n",
		p.prepareSQL, p.executeSQL, p.SQLNoCache, p.err1, p.err2)
	content += "\nParameters:\n\n"
	for _, sql := range p.setSQLs {
		content += sql
		content += "\n"
	}

	err := os.WriteFile(filePath, []byte(content), 0644)
	if err != nil {
		return fmt.Errorf("failed to write to file %s: %v", filePath, err)
	}

	return nil
}

func (p *Prepare) generateParams(genMatch bool) {
	replacementsPairs := make([]string, 0, len(p.generators))
	setSQLs := make([]string, len(p.generators))
	for i, gen := range p.generators {
		v := gen.GenMatch()
		if !genMatch {
			v = gen.GenMismatch()
		}
		setSQLs[i] = fmt.Sprintf("@i%d = %s", i, v)
		replacementsPairs = append(replacementsPairs, v)
	}
	p.setSQL = fmt.Sprintf("set %s", strings.Join(setSQLs, ","))
	p.setSQLs = append(p.setSQLs, p.setSQL)

	var sb strings.Builder
	replacementIndex := 0
	for _, char := range p.originalSQL {
		if char == '?' {
			sb.WriteString(replacementsPairs[replacementIndex])
			replacementIndex++
		} else {
			sb.WriteRune(char)
		}
	}
	p.SQLNoCache = sb.String()
}

func getConn(db *sql.DB) (*sql.Conn, error) {
	var (
		conn *sql.Conn
		err  error
	)

	conn, err = db.Conn(context.Background())
	if err != nil {
		return nil, errors.Trace(err)
	}

	maxRetries := 5
	for i := 0; i < maxRetries; i++ {
		if err = conn.PingContext(context.Background()); err != nil {
			conn.Close()
			time.Sleep(10 * time.Millisecond)
			conn, err = db.Conn(context.Background())
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			break
		}
	}

	return conn, errors.Trace(err)
}

// Run the same query in two sessions, one with plan cache and one not.
// Return an error if two sessions return different errors.
func (p *Prepare) execAndCompare(dbWithCache, dbNoCache *sql.DB, genMatch bool, times int) error {
	conn, err := getConn(dbWithCache)
	if err != nil {
		return errors.Trace(err)
	}
	defer conn.Close()

	// Prepare statement
	err = util.ExecSQLWithConn(conn, p.prepareSQL)
	if err != nil {
		return err
	}

	for i := 0; i < times; i++ {
		p.generateParams(genMatch)

		// No cache
		_, p.err1 = dbNoCache.Exec(p.SQLNoCache)

		// With cache
		err := util.ExecSQLWithConn(conn, p.setSQL)
		if err != nil {
			return errors.Trace(err)
		}

		_, p.err2 = conn.ExecContext(context.Background(), p.executeSQL)
		if p.err2 != nil && (strings.Contains(p.err2.Error(), "invalid memory")) {
			return errors.Trace(p.err2)
		}

		lastExecuteUseCache := checkLastUseCache(conn)

		if err := CheckError(p.err1, p.err2); err != nil {
			return errors.Trace(err)
		}

		// Check cache used if no error.
		if i > 0 && !lastExecuteUseCache {
			skipCacheInExecute++
		}
	}

	return nil
}

var skipCacheInExecute = 0

// Run the same query in two sessions, one with plan cache and one not.
// Return an error if either:
// 1. two sessions return different errors.
// 2. two sessions return different results.
func (p *Prepare) queryAndCompare(dbWithCache, dbNoCache *sql.DB, genMatch bool, times int) error {
	conn, err := getConn(dbWithCache)
	if err != nil {
		return errors.Trace(err)
	}
	defer conn.Close()

	// Prepare statement
	err = util.ExecSQLWithConn(conn, p.prepareSQL)
	if err != nil {
		return err
	}

	for i := 0; i < times; i++ {
		p.generateParams(genMatch)

		// No cache
		resWithoutCache, err1 := util.FetchRowsWithDB(dbNoCache, p.SQLNoCache)
		p.err1 = err1

		// With cache
		err := util.ExecSQLWithConn(conn, p.setSQL)
		if err != nil {
			return errors.Trace(err)
		}

		resWithCache, err2 := util.FetchRowsWithConn(conn, p.executeSQL)
		if err2 != nil && (strings.Contains(err2.Error(), "invalid memory")) {
			return errors.Trace(p.err2)
		}

		lastExecuteUseCache := checkLastUseCache(conn)
		p.err2 = err2

		// If both executed successfully, check whether the results are same.
		if err1 == nil && err2 == nil {
			if i > 0 && !lastExecuteUseCache {
				skipCacheInExecute++
			}

			same, err := util.CheckResults(resWithoutCache, resWithCache)
			if err != nil {
				return errors.Trace(err)
			}
			if !same {
				return errors.Errorf("Two sessions' result mismatch")
			}
			return nil
		}

		// Two sessions return different result
		if err := CheckError(err1, err2); err != nil {
			return errors.Trace(err)
		}

		if i > 0 && !lastExecuteUseCache {
			skipCacheInExecute++
		}
	}

	return nil
}

// Check whether this generated SQL can utilize plan cache
func (p *Prepare) UsePlanCache(dbWithCache *sql.DB) (bool, error) {
	if len(p.generators) == 0 {
		return false, nil
	}

	skipped, err := RunAndCheckPlanCache(p.prepareSQL, dbWithCache)
	return !skipped, err
}

func (p *Prepare) CheckQuery(dbWithCache, dbNoCache *sql.DB) error {
	err := p.queryAndCompare(dbWithCache, dbNoCache, true, 1)
	if err != nil {
		return errors.Trace(err)
	}

	err = p.queryAndCompare(dbWithCache, dbNoCache, true, 10)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (p *Prepare) CheckExec(dbWithCache, dbNoCache *sql.DB) error {
	err := p.execAndCompare(dbWithCache, dbNoCache, true, 1)
	if err != nil {
		return errors.Trace(err)
	}

	err = p.execAndCompare(dbWithCache, dbNoCache, true, 3)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Use probability to determine whether to use placeholder
func (p *Prepare) CheckAdd() bool {
	initialProb := 1.0
	decayFactor := 0.75

	prob := initialProb * math.Pow(decayFactor, float64(len(p.generators)))
	return rand.Float64() < prob
}

func (p *Prepare) Add(gen ValueGenerator) {
	p.generators = append(p.generators, gen)
}

func (p *Prepare) SetPrepareSQL(sql string) {
	p.originalSQL = sql
	p.prepareSQL = fmt.Sprintf("prepare `%s` from %s", p.name, strconv.Quote(sql))
	vars := make([]string, len(p.generators))
	for i := range p.generators {
		vars[i] = fmt.Sprintf("@i%d", i)
	}
	p.executeSQL = fmt.Sprintf("execute `%s` using %s", p.name, strings.Join(vars, ","))
}

func (s *State) EnablePrepare() {
	s.prepareStmt = NewPrepare()
}

func (s *State) PopPrepare() *Prepare {
	p := s.prepareStmt
	s.prepareStmt = nil
	return p
}

// Record current number of generators.
// It's used to pop generators after failure.
func (s *State) RecordStack() int {
	if s.prepareStmt == nil {
		return 0
	}
	return s.prepareStmt.RecordStack()
}

func (s *State) PopStack(l int) {
	if s.prepareStmt == nil {
		return
	}
	s.prepareStmt.PopStack(l)
}

// GetValueFn is used to determine the return string.
// When we are generating a prepared statement, it will stores the generator and return a placeholder for later use.
// Otherwise, it directly gets a value from the generator and returns it.
func (s *State) GetValueFn(gen ValueGenerator) Fn {
	if s.prepareStmt != nil && s.prepareStmt.CheckAdd() {
		s.prepareStmt.Add(gen)
		return Str(Placeholder)
	}

	return Str(gen.GenMatch())
}

// Generate a random prepare statement
func GeneratePrepare(state *State, query bool) (*Prepare, error) {
	state.EnablePrepare()

	var planCacheDML Fn

	if query {
		planCacheDML = NewFn(func(state *State) Fn {
			return Or(
				SingleSelect.W(4),
				MultiSelect.W(4),
				UnionSelect.W(4),
				MultiSelectWithSubQuery.W(4),
			)
		})
	} else {
		planCacheDML = NewFn(func(state *State) Fn {
			return Or(
				CommonInsertOrReplace.W(10),
				CommonUpdate.W(10),
				//CommonDelete.W(2),
			)
		})
	}

	rawSQL, err := planCacheDML.Eval(state)
	if err != nil {
		return nil, err
	}

	p := state.PopPrepare()
	if strings.Count(rawSQL, "?") != len(p.generators) {
		return nil, errors.Errorf("Generate prepare statement failed")
	}
	p.SetPrepareSQL(rawSQL)

	return p, nil
}

var PlanCacheDataGen = NewFn(func(state *State) Fn {
	return Or(
		// Data preparation
		CreateTable.W(20).P(NoTooMuchTables),
		CommonInsertOrReplace.W(10).P(HasTables),
		CommonUpdate.W(5).P(HasTables),
		CommonDelete.W(5).P(HasTables),
		// DDL
		AlterTable.W(5).P(HasTables),
		SplitRegion.W(1).P(HasTables),
		SetTiFlashReplica.W(0).P(HasTables),
		// Check
		AdminCheck.W(1).P(HasTables),
		AnalyzeTable.W(0).P(HasTables),
	)
})
