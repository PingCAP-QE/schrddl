package sqlgenerator

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"

	"github.com/PingCAP-QE/schrddl/util"
	sqlutil "github.com/PingCAP-QE/schrddl/util"
	"github.com/juju/errors"
	"github.com/ngaut/log"
)

func checkError(err1, err2 error) error {
	if err1 == nil && err2 == nil {
		return nil
	}

	if err1 == nil || err2 == nil {
		log.Warn("Two sessions returns different error")
		log.Warnf("Error1: %v", err1)
		log.Warnf("Error2: %v", err2)
		return errors.Errorf("Two sessions return different error")
	}

	// Since the SQL is randomly generated, it may inherently fail to execute.
	// In this case, we assume both sessions will return the same error.
	// TODO(joechenrh): pass schema name
	errStr1, errStr2 := err1.Error(), err2.Error()
	errStr2 = strings.ReplaceAll(errStr2, "test2", "test1")
	if errStr1 != errStr2 {
		log.Warn("Two sessions returns different error")
		log.Warnf("Error1: %v", err1)
		log.Warnf("Error2: %v", err2)
		return errors.Errorf("Two sessions return different error")
	}

	if !util.DMLIgnoreError(err1) {
		return errors.Errorf("Two sessions both get non-ignorable error")
	}
	return nil
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

// Check whether this statement can be used in plan cache.
func RunAndCheckPlanCache(sql string, db *sql.DB) (bool, error) {
	_, err := db.Exec(sql)
	if err != nil {
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
	SQLNoCache  string
	assigns     []string
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

func replaceQuestionMarks(original string, replacements []string) string {
	// Create a new string builder for the result
	var result strings.Builder
	replacementIndex := 0

	for _, char := range original {
		if char == '?' {
			// Replace with the corresponding string from the slice
			result.WriteString(replacements[replacementIndex])
			replacementIndex++
		} else {
			// Append the original character
			result.WriteRune(char)
		}
	}

	return result.String()
}

func (p *Prepare) RecordError(o *os.File) {
	o.WriteString(fmt.Sprintf("Prepare statement: %s\n", p.prepareSQL))
	o.WriteString("params:\n")
	for _, p := range p.assigns {
		o.WriteString(fmt.Sprintf("%s\n", p))
	}
}

func (p *Prepare) GenAssignments(genMatch bool) {
	p.assigns = make([]string, 0, len(p.generators))
	for _, gen := range p.generators {
		v := gen.GenMatch()
		if !genMatch {
			v = gen.GenMismatch()
		}
		p.assigns = append(p.assigns, v)
	}
}

// Run the same query in two sessions, one with plan cache and one not.
// Return an error if two sessions return different errors.
func (p *Prepare) execAndCompare(dbWithCache, dbNoCache *sql.DB, genMatch bool) error {
	p.GenAssignments(genMatch)

	replacementsPairs := make([]string, 0, len(p.generators))
	setSQLs := make([]string, len(p.generators))
	for i, v := range p.assigns {
		setSQLs[i] = fmt.Sprintf("set @i%d = %s", i, v)
		replacementsPairs = append(replacementsPairs, v)
	}
	p.SQLNoCache = replaceQuestionMarks(p.originalSQL, replacementsPairs)

	// No cache
	err1 := util.ExecSQLWithDB(dbNoCache, p.SQLNoCache)

	// With cache
	connWithCache, err := dbWithCache.Conn(context.Background())
	if err != nil {
		log.Fatal("Can't open connection")
	}
	defer connWithCache.Close()
	for _, sql := range setSQLs {
		err := util.ExecSQLWithConn(connWithCache, sql)
		if err != nil {
			return errors.Trace(err)
		}
	}
	err2 := util.ExecSQLWithConn(connWithCache, p.prepareSQL)
	if err2 != nil {
		return err2
	}
	err2 = util.ExecSQLWithConn(connWithCache, p.SQLNoCache)

	return checkError(err1, err2)
}

// Run the same query in two sessions, one with plan cache and one not.
// Return an error if either:
// 1. two sessions return different errors.
// 2. two sessions return different results.
func (p *Prepare) queryAndCompare(dbWithCache, dbNoCache *sql.DB, genMatch bool) error {
	p.GenAssignments(genMatch)

	replacementsPairs := make([]string, 0, len(p.generators))
	setSQLs := make([]string, len(p.generators))
	for i, v := range p.assigns {
		setSQLs[i] = fmt.Sprintf("set @i%d = %s", i, v)
		replacementsPairs = append(replacementsPairs, v)
	}
	p.SQLNoCache = replaceQuestionMarks(p.originalSQL, replacementsPairs)

	// No cache
	res1, err1 := util.FetchRowsWithDB(dbNoCache, p.SQLNoCache)

	// With cache
	conn, err := dbWithCache.Conn(context.Background())
	if err != nil {
		log.Fatal("Can't open connection")
	}
	defer conn.Close()
	for _, sql := range setSQLs {
		err := util.ExecSQLWithConn(conn, sql)
		if err != nil {
			return errors.Trace(err)
		}
	}
	err2 := util.ExecSQLWithConn(conn, p.prepareSQL)
	if err2 != nil {
		return err2
	}
	res2, err2 := util.FetchRowsWithConn(conn, p.executeSQL)

	// If both executed successfully, check whether the results are same.
	if err1 == nil && err2 == nil {
		same, err := util.CheckResults(res1, res2)
		if err != nil {
			return err
		}
		if !same {
			return errors.Errorf("Two sessions' result mismatch")
		}
		return nil
	}

	if err1 != nil && strings.Contains(err1.Error(), "context canceled") ||
		err2 != nil && strings.Contains(err2.Error(), "context canceled") {
		log.Warn("Unexpected context canceled")
		return nil
	}
	if err1 != nil && strings.Contains(err1.Error(), "Illegal mix of collation") && err2 == nil {
		log.Warn("no cache get Illegal mix of collations error")
		return nil
	}
	return checkError(err1, err2)
}

func (p *Prepare) UsePlanCache(dbWithCache *sql.DB) (bool, error) {
	if len(p.generators) == 0 {
		return false, nil
	}

	skipped, err := RunAndCheckPlanCache(p.prepareSQL, dbWithCache)
	return !skipped, err
}

func (p *Prepare) CheckQuery(dbWithCache, dbNoCache *sql.DB) error {
	err := p.queryAndCompare(dbWithCache, dbNoCache, true)
	if err != nil {
		return errors.Trace(err)
	}

	for i := 0; i <= 10; i++ {
		err = p.queryAndCompare(dbWithCache, dbNoCache, false)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (p *Prepare) CheckExec(dbWithCache, dbNoCache *sql.DB) error {
	err := p.execAndCompare(dbWithCache, dbNoCache, true)
	if err != nil {
		return errors.Trace(err)
	}

	for i := 0; i <= 5; i++ {
		err = p.execAndCompare(dbWithCache, dbNoCache, false)
		if err != nil {
			return errors.Trace(err)
		}
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
				CommonUpdate.W(5),
				CommonDelete.W(5),
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
