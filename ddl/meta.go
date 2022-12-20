package ddl

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/emirpasic/gods/lists/arraylist"
	"github.com/twinj/uuid"
)

type testCase struct {
	cfg              *DDLCaseConfig
	initDB           string
	dbs              []*sql.DB
	caseIndex        int
	ddlOps           []ddlTestOpExecutor
	dmlOps           []dmlTestOpExecutor
	tables           map[string]*ddlTestTable
	schemas          map[string]*ddlTestSchema
	views            map[string]*ddlTestView
	tablesLock       sync.RWMutex
	schemasLock      sync.Mutex
	stop             int32
	lastDDLID        int
	charsets         []string
	charsetsCollates map[string][]string
}

type ddlTestErrorConflict struct {
}

func (err ddlTestErrorConflict) Error() string {
	return "Conflict operation"
}

func (c *testCase) stopTest() {
	atomic.StoreInt32(&c.stop, 1)
}

func (c *testCase) isStop() bool {
	return atomic.LoadInt32(&c.stop) == 1
}

// schema type, this might be modified to support other operations, but it's not clear
// currently for me.
type ddlTestSchema struct {
	id      string
	name    string
	deleted bool
	charset string
	collate string
}

func (c *testCase) isSchemaDeleted(schema *ddlTestSchema) bool {
	if _, ok := c.schemas[schema.name]; ok {
		return false
	}
	return true
}

func (schema *ddlTestSchema) setDeleted() {
	schema.deleted = true
}

func (schema *ddlTestSchema) isDeleted() bool {
	return schema.deleted
}

// pickupRandomSchema picks a schema randomly from `c.schemas`.
func (c *testCase) pickupRandomSchema() *ddlTestSchema {
	schemaLen := len(c.schemas)
	if schemaLen == 0 {
		return nil
	}
	loc := rand.Intn(schemaLen)
	for _, schema := range c.schemas {
		if loc == 0 {
			return schema
		}
		loc--
	}
	return nil
}

// pickupRandomTables picks a table randomly. The callee should ensure that
// during this function call the table list is not modified.
//
// Normally the DML op callee should acquire a lock before calling this function
// because the table list may be modified by another parallel DDL op. However
// the DDL op callee doesn't need to acquire a lock because no one will modify the
// table list in parallel ---- DDL ops are executed one by one.
func (c *testCase) pickupRandomTable() *ddlTestTable {
	tableNames := make([]string, 0)
	for name, table := range c.tables {
		if table.isDeleted() {
			continue
		}
		tableNames = append(tableNames, name)
	}
	if len(tableNames) == 0 {
		return nil
	}
	name := tableNames[rand.Intn(len(tableNames))]
	return c.tables[name]
}

func (c *testCase) pickupRandomCharsetAndCollate() (string, string) {
	// When a table created by a binary charset and collate, it would
	// convert char type column to binary type which would cause the
	// the following sql execution result not so intuitive:
	//
	//  create table test (id int primary key, name char(10)) charset="binary", collate="binary";
	//  show create table test;
	//  +-------+--------------------------------------------+
	//  | test  | CREATE TABLE `test` (
	//      `id` int(11) NOT NULL,
	//      `name` binary(10) DEFAULT NULL,
	//      PRIMARY KEY(`id`)
	//  ) ENGINE=InnoDB DEFAULT CHARSET=binary |
	//  +-------+---------------------------------------------+
	//  insert into test set id=1, name="hello";
	//  select * from test where name="hello";
	//      -> empty set.
	//  select * from test where name="hello\0\0\0\0\0";
	//      -> one row
	//
	// and when running `delete from test where name="hello",
	// db doesn't delete the row, the corresponding dml test would
	// remove the row.
	//
	// So we don't create table with binary charset and collate for simplify.

	charset := "binary"
	var collation string
	for charset == "binary" {
		charset = c.charsets[rand.Intn(len(c.charsets))]
		collations := c.charsetsCollates[charset]
		collation = collations[rand.Intn(len(collations))]
	}
	return charset, collation
}

func (c *testCase) isTableDeleted(table *ddlTestTable) bool {
	if _, ok := c.tables[table.name]; ok {
		return false
	}
	return true
}

func (c *testCase) isColumnDeleted(column *ddlTestColumn, table *ddlTestTable) bool {
	for ite := table.columns.Iterator(); ite.Next(); {
		col := ite.Value().(*ddlTestColumn)
		if col.name == column.name {
			return false
		}
	}
	return true
}

type ddlTestTable struct {
	deleted      int32
	name         string
	id           string // table_id , get from admin show ddl jobs
	columns      *arraylist.List
	indexes      []*ddlTestIndex
	numberOfRows int
	shardRowId   int64 // shard_row_id_bits
	autoIncID    int64
	comment      string // table comment
	charset      string
	collate      string
	lock         *sync.RWMutex
	replicaCnt   int
}

func (table *ddlTestTable) isDeleted() bool {
	return atomic.LoadInt32(&table.deleted) != 0
}

func (table *ddlTestTable) setDeleted() {
	atomic.StoreInt32(&table.deleted, 1)
}

func (table *ddlTestTable) filterColumns(predicate func(*ddlTestColumn) bool) []*ddlTestColumn {
	retColumns := make([]*ddlTestColumn, 0)
	for ite := table.columns.Iterator(); ite.Next(); {
		col := ite.Value().(*ddlTestColumn)
		if predicate(col) && !col.isDeleted() {
			retColumns = append(retColumns, col)
		}
	}
	return retColumns
}

func (table *ddlTestTable) predicateAll(col *ddlTestColumn) bool {
	return true
}

func (table *ddlTestTable) predicateNotGenerated(col *ddlTestColumn) bool {
	return col.notGenerated()
}

func (table *ddlTestTable) predicatePrimaryKey(col *ddlTestColumn) bool {
	return col.isPrimaryKey
}

func (table *ddlTestTable) predicateNonPrimaryKey(col *ddlTestColumn) bool {
	return !col.isPrimaryKey
}

func (table *ddlTestTable) predicateNonPrimaryKeyAndCanBeWhere(col *ddlTestColumn) bool {
	return !col.isPrimaryKey && col.canBeWhere()
}

func (table *ddlTestTable) predicateNonPrimaryKeyAndNotGen(col *ddlTestColumn) bool {
	return !col.isPrimaryKey && col.notGenerated()
}

// pickupRandomColumns picks columns from `table.columns` randomly and return them.
func (table *ddlTestTable) pickupRandomColumns() []*ddlTestColumn {
	columns := table.filterColumns(table.predicateAll)
	pickedCols := make([]*ddlTestColumn, 0)
	for _, col := range columns {
		if rand.Float64() > 0.6 {
			pickedCols = append(pickedCols, col)
		}
	}
	return pickedCols
}

// pickupRandomColumn picks a column from `table.columns` randomly and
// returns the column index and the column.
func (table *ddlTestTable) pickupRandomColumn() (int, *ddlTestColumn) {
	columns := table.filterColumns(table.predicateAll)
	if len(columns) == 0 {
		return 0, nil
	}
	index := rand.Intn(len(columns))
	return index, columns[index]
}

// isColumnDeleted checks the col is deleted in this table
// col.isDeleted() will be true before when dropColumnJob(),
// but the col is really deleted after remote TiDB successful execute drop column ddl, and then, the col will be deleted from table.columns.
func (table *ddlTestTable) isColumnDeleted(col *ddlTestColumn) bool {
	for i := 0; i < table.columns.Size(); i++ {
		columnI := getColumnFromArrayList(table.columns, i)
		if col.name == columnI.name {
			return false
		}
	}
	return true
}

// newRandAutoID returns a feasible new random auto_increment id according to
// shard_row_id_bits of this table.
func (table *ddlTestTable) newRandAutoID() int64 {
	maxAutoID := int64(1<<(64-uint(table.shardRowId)-1)) - 1
	return rand.Int63n(maxAutoID)
}

func (table *ddlTestTable) debugPrintToString() string {
	var buffer bytes.Buffer
	table.lock.RLock()
	buffer.WriteString(fmt.Sprintf("======== DEBUG BEGIN  ========\n"))
	buffer.WriteString(fmt.Sprintf("Dumping expected contents for table `%s`:\n", table.name))
	if table.isDeleted() {
		buffer.WriteString("[WARN] This table is marked as DELETED.\n")
	}
	buffer.WriteString(fmt.Sprintf("Comment: %s\nCharset: %s, Collate: %s\nShardRowId: %d\nAutoID: %d\n",
		table.comment, table.charset, table.collate, table.shardRowId, table.autoIncID))
	buffer.WriteString("## Non-Primary Indexes: \n")
	for i, index := range table.indexes {
		buffer.WriteString(fmt.Sprintf("Index #%d: Name = `%s`, Columnns = [", i, index.name))
		for _, column := range index.columns {
			buffer.WriteString(fmt.Sprintf("`%s`, ", column.name))
		}
		buffer.WriteString("]\n")
	}
	buffer.WriteString("## Columns: \n")
	for i := 0; i < table.columns.Size(); i++ {
		column := getColumnFromArrayList(table.columns, i)
		buffer.WriteString(fmt.Sprintf("Column #%d", i))
		if column.isDeleted() {
			buffer.WriteString(" [DELETED]")
		}
		buffer.WriteString(fmt.Sprintf(": Name = `%s`, Definition = %s, isPrimaryKey = %v, used in %d indexes\n",
			column.name, column.getDefinition(), column.isPrimaryKey, column.indexReferences))
	}
	buffer.WriteString(fmt.Sprintf("## Values (number of rows = %d): \n", table.numberOfRows))
	for i := 0; i < table.numberOfRows; i++ {
		buffer.WriteString("#")
		buffer.WriteString(PadRight(fmt.Sprintf("%d", i), " ", 4))
		buffer.WriteString(": ")
		for j := 0; j < table.columns.Size(); j++ {
			col := getColumnFromArrayList(table.columns, j)
			buffer.WriteString(PadLeft(fmt.Sprintf("%v", getRowFromArrayList(col.rows, i)), " ", 11))
			buffer.WriteString(", ")
		}
		buffer.WriteString("\n")
	}
	buffer.WriteString("======== DEBUG END ========\n")
	table.lock.RUnlock()
	return buffer.String()
}

type ddlTestView struct {
	id      string
	name    string
	columns []*ddlTestColumn
	table   *ddlTestTable // the table that this view references.
}

type ddlTestColumnDescriptor struct {
	column *ddlTestColumn
	value  interface{}
}

func (ddlt *ddlTestColumnDescriptor) getValueString() string {
	// make bit data visible
	if ddlt.column.k == KindBit {
		return fmt.Sprintf("b'%v'", ddlt.value)
	} else {
		return fmt.Sprintf("'%v'", ddlt.value)
	}
}

func (ddlt *ddlTestColumnDescriptor) buildConditionSQL() string {
	var sql string
	if ddlt.value == ddlTestValueNull || ddlt.value == nil {
		sql += fmt.Sprintf("`%s` IS NULL", ddlt.column.name)
	} else {
		switch ddlt.column.k {
		case KindFloat:
			sql += fmt.Sprintf("abs(`%s` - %v) < 0.0000001", ddlt.column.name, ddlt.getValueString())
		case KindDouble:
			sql += fmt.Sprintf("abs(`%s` - %v) < 0.0000000000000001", ddlt.column.name, ddlt.getValueString())
		default:
			sql += fmt.Sprintf("`%s` = %v", ddlt.column.name, ddlt.getValueString())
		}
	}
	return sql
}

type ddlTestColumn struct {
	k         int
	deleted   int32
	renamed   int32
	name      string
	fieldType string

	filedTypeM      int //such as:  VARCHAR(10) ,    filedTypeM = 10
	filedTypeD      int //such as:  DECIMAL(10,5) ,  filedTypeD = 5
	filedPrecision  int
	defaultValue    interface{}
	isPrimaryKey    bool
	rows            *arraylist.List
	indexReferences int

	dependenciedCols []*ddlTestColumn
	dependency       *ddlTestColumn
	mValue           map[string]interface{}
	nameOfGen        string

	setValue []string //for enum , set data type
}

func (col *ddlTestColumn) isDeleted() bool {
	return atomic.LoadInt32(&col.deleted) != 0
}

func (col *ddlTestColumn) setDeleted() {
	atomic.StoreInt32(&col.deleted, 1)
}

func (col *ddlTestColumn) isRenamed() bool {
	return atomic.LoadInt32(&col.renamed) != 0
}

func (col *ddlTestColumn) setRenamed() {
	atomic.StoreInt32(&col.renamed, 1)
}

func (col *ddlTestColumn) delRenamed() {
	atomic.StoreInt32(&col.renamed, 0)
}

func (col *ddlTestColumn) setDeletedRecover() {
	atomic.StoreInt32(&col.deleted, 0)
}

func (col *ddlTestColumn) getMatchedColumnDescriptor(descriptors []*ddlTestColumnDescriptor) *ddlTestColumnDescriptor {
	for _, d := range descriptors {
		if d.column == col {
			return d
		}
	}
	return nil
}

func (col *ddlTestColumn) getDefinition() string {
	if col.isPrimaryKey {
		if col.canHaveDefaultValue() {
			return fmt.Sprintf("%s DEFAULT %v", col.fieldType, getDefaultValueString(col.k, col.defaultValue))
		}
		return col.fieldType
	}

	if col.isGenerated() {
		return fmt.Sprintf("%s AS (JSON_EXTRACT(`%s`,'$.%s'))", col.fieldType, col.dependency.name, col.nameOfGen)
	}

	if col.canHaveDefaultValue() {
		return fmt.Sprintf("%s NULL DEFAULT %v", col.fieldType, getDefaultValueString(col.k, col.defaultValue))
	} else {
		return fmt.Sprintf("%s NULL", col.fieldType)
	}

}

func (col *ddlTestColumn) getSelectName() string {
	if col.k == KindBit {
		return fmt.Sprintf("bin(`%s`)", col.name)
	} else {
		return fmt.Sprintf("`%s`", col.name)
	}
}

// getDefaultValueString returns a string representation of `defaultValue` according
// to the type `k` of column.
func getDefaultValueString(k int, defaultValue interface{}) string {
	if k == KindBit {
		return fmt.Sprintf("b'%v'", defaultValue)
	} else {
		return fmt.Sprintf("'%v'", defaultValue)
	}
}

func (col *ddlTestColumn) isEqual(r int, str string) bool {
	vstr := fmt.Sprintf("%v", getRowFromArrayList(col.rows, r))
	return strings.Compare(vstr, str) == 0
}

func (col *ddlTestColumn) getDependenciedColsValue(genCol *ddlTestColumn) interface{} {
	if col.mValue == nil {
		return nil
	}
	v := col.mValue[genCol.nameOfGen]
	switch genCol.k {
	case KindChar, KindVarChar, KindTEXT, KindBLOB:
		v = fmt.Sprintf("\"%v\"", v)
	}
	return v
}

// canBeModified returns whether this column can be changed by a SQL query `change column` or `modify column`.
// Only a few type columns are supported. See https://pingcap.com/docs/sql/ddl/ for more detail.
func (col *ddlTestColumn) canBeModified() bool {
	typeSupported := false
	switch col.k {
	case KindTINYINT, KindSMALLINT, KindMEDIUMINT, KindInt32, KindBigInt:
		typeSupported = true
	case KindDECIMAL:
		typeSupported = true
	case KindChar, KindVarChar:
		typeSupported = true
	case KindTEXT, KindBLOB:
		typeSupported = true
	}
	return typeSupported
}

func getDDLTestColumn(n int) *ddlTestColumn {
	column := &ddlTestColumn{
		k:         n,
		name:      uuid.NewV4().String(),
		fieldType: ALLFieldType[n],
		rows:      arraylist.New(),
		deleted:   0,
	}
	switch n {
	case KindChar, KindVarChar, KindBLOB, KindTEXT, KindBit:
		maxLen := GetMaxLenByKind(n)
		column.filedTypeM = int(rand.Intn(maxLen))
		for column.filedTypeM == 0 && column.k == KindBit {
			column.filedTypeM = int(rand.Intn(maxLen))
		}

		for column.filedTypeM < 3 && column.k != KindBit { // len('""') = 2
			column.filedTypeM = int(rand.Intn(maxLen))
		}
		column.fieldType = fmt.Sprintf("%s(%d)", ALLFieldType[n], column.filedTypeM)
	case KindTINYBLOB, KindMEDIUMBLOB, KindLONGBLOB, KindTINYTEXT, KindMEDIUMTEXT, KindLONGTEXT:
		column.filedTypeM = GetMaxLenByKind(n)
		column.fieldType = fmt.Sprintf("%s(%d)", ALLFieldType[n], column.filedTypeM)
	case KindDECIMAL:
		column.filedTypeM, column.filedTypeD = RandMD()
		column.fieldType = fmt.Sprintf("%s(%d,%d)", ALLFieldType[n], column.filedTypeM, column.filedTypeD)
	case KindEnum, KindSet:
		maxLen := GetMaxLenByKind(n)
		l := maxLen + 1
		column.setValue = make([]string, l)
		m := make(map[string]struct{})
		column.fieldType += "("
		for i := 0; i < l; i++ {
			column.setValue[i] = RandEnumString(m)
			if i > 0 {
				column.fieldType += ", "
			}
			column.fieldType += fmt.Sprintf("\"%s\"", column.setValue[i])
		}
		column.fieldType += ")"
	}

	if column.canHaveDefaultValue() {
		column.defaultValue = column.randValue()
	}

	return column
}

func getRandDDLTestColumn() *ddlTestColumn {
	var n int
	for {
		n = RandDataType()
		if n != KindJSON {
			break
		}
	}
	return getDDLTestColumn(n)
}

// generateRandModifiedColumn returns a random column to modify column `col`.
// It doesn't change any properties of column `col` and instead, it first
// generates a copy of column `col` and then modifies some properties of the
// generated column randomly. The parameter `renameCol` indicates whether to modify
// column name.
func generateRandModifiedColumn(col *ddlTestColumn, renameCol bool) *ddlTestColumn {
	// Shadow copy of column col.
	modifiedColumn := *col
	if renameCol {
		modifiedColumn.name = uuid.NewV4().String()
	} else {
		modifiedColumn.name = col.name
	}
	// ModifyColumn operator has some limitations.
	// See https://pingcap.com/docs/sql/ddl/ for more detail.
	// For example, only supports type changes where original type
	// is belong to integer types, string types or Blob types
	// and only supports extending length of the original type.
	// Charset and collate cannot be changed.
	switch col.k {
	case KindTINYINT, KindSMALLINT, KindMEDIUMINT, KindInt32, KindBigInt:
		modifiedColumn.k = rand.Intn(KindBigInt-col.k+1) + col.k
		modifiedColumn.fieldType = ALLFieldType[modifiedColumn.k]
	case KindDECIMAL:
		// Decimal column are not allowed to modify its precision.
		// We ignore here.
	case KindChar, KindVarChar:
		modifiedColumn.k = rand.Intn(KindVarChar-col.k+1) + col.k
		modifiedColumn.filedTypeM = rand.Intn(GetMaxLenByKind(modifiedColumn.k)-col.filedTypeM) + col.filedTypeM
		modifiedColumn.fieldType = fmt.Sprintf("%s(%d)", ALLFieldType[modifiedColumn.k], modifiedColumn.filedTypeM)
	case KindTEXT, KindBLOB:
		// Only types on `TestFieldType` are considered.
		// See ./datatype.go for more detail.
		modifiedColumn.filedTypeM = rand.Intn(GetMaxLenByKind(col.k)-col.filedTypeM) + col.filedTypeM
		modifiedColumn.fieldType = fmt.Sprintf("%s(%d)", ALLFieldType[col.k], modifiedColumn.filedTypeM)
	}
	if modifiedColumn.canHaveDefaultValue() {
		modifiedColumn.defaultValue = modifiedColumn.randValue()
	}
	return &modifiedColumn
}

// generateRandModifiedColumn2 returns a totally new column, may with the old name.
func generateRandModifiedColumn2(col *ddlTestColumn, renameCol bool) *ddlTestColumn {
	newColumn := getRandDDLTestColumn()
	if renameCol {
		newColumn.name = uuid.NewV4().String()
	} else {
		newColumn.name = col.name
	}
	return newColumn
}

func getRandDDLTestColumnForJson() *ddlTestColumn {
	var n int
	for {
		n = RandDataType()
		if n != KindJSON && n != KindBit && n != KindSet && n != KindEnum {
			break
		}
	}
	return getDDLTestColumn(n)
}

func getRandDDLTestColumns() []*ddlTestColumn {
	n := RandDataType()
	cols := make([]*ddlTestColumn, 0)

	if n == KindJSON {
		// Json column itself doesn't mean a lot, it's value is in generated column.
		// eg: create table t(a json, b int as(json_extract(`a`, '$.haha'))).
		// for this instance: a is the target column, b is depending on column a.
		cols = getRandJsonCol()
	} else {
		column := getDDLTestColumn(n)
		cols = append(cols, column)
	}
	return cols
}

const JsonFieldNum = 5

func getRandJsonCol() []*ddlTestColumn {
	fieldNum := rand.Intn(JsonFieldNum) + 1

	cols := make([]*ddlTestColumn, 0, fieldNum+1)

	column := &ddlTestColumn{
		k:         KindJSON,
		name:      uuid.NewV4().String(),
		fieldType: ALLFieldType[KindJSON],
		rows:      arraylist.New(),
		deleted:   0,

		dependenciedCols: make([]*ddlTestColumn, 0, fieldNum),
	}

	m := make(map[string]interface{}, 0)
	for i := 0; i < fieldNum; i++ {
		col := getRandDDLTestColumnForJson()
		col.nameOfGen = RandFieldName(m)
		m[col.nameOfGen] = col.randValue()
		col.dependency = column

		column.dependenciedCols = append(column.dependenciedCols, col)
		cols = append(cols, col)
	}
	column.mValue = m
	cols = append(cols, column)
	return cols
}

func (col *ddlTestColumn) isGenerated() bool {
	return col.dependency != nil
}

func (col *ddlTestColumn) notGenerated() bool {
	return col.dependency == nil
}

func (col *ddlTestColumn) hasGenerateCol() bool {
	return len(col.dependenciedCols) > 0
}

// randValue return a rand value of the column
func (col *ddlTestColumn) randValue() interface{} {
	switch col.k {
	case KindTINYINT:
		return rand.Int31n(1<<8) - 1<<7
	case KindSMALLINT:
		return rand.Int31n(1<<16) - 1<<15
	case KindMEDIUMINT:
		return rand.Int31n(1<<24) - 1<<23
	case KindInt32:
		return rand.Int63n(1<<32) - 1<<31
	case KindBigInt:
		if rand.Intn(2) == 1 {
			return rand.Int63()
		}
		return -1 - rand.Int63()
	case KindBit:
		if col.filedTypeM >= 64 {
			return fmt.Sprintf("%b", rand.Uint64())
		} else {
			m := col.filedTypeM
			if col.filedTypeM > 7 { // it is a bug
				m = m - 1
			}
			n := (int64)((1 << (uint)(m)) - 1)
			return fmt.Sprintf("%b", rand.Int63n(n))
		}
	case KindFloat:
		return rand.Float32() + 1
	case KindDouble:
		return rand.Float64() + 1
	case KindDECIMAL:
		return RandDecimal(col.filedTypeM, col.filedTypeD)
	case KindChar, KindVarChar, KindBLOB, KindTINYBLOB, KindMEDIUMBLOB, KindLONGBLOB, KindTEXT, KindTINYTEXT, KindMEDIUMTEXT, KindLONGTEXT:
		if col.filedTypeM == 0 {
			return ""
		} else {
			if col.isGenerated() {
				if col.filedTypeM <= 2 {
					return ""
				}
				return RandSeq(rand.Intn(col.filedTypeM - 2))
			}
			return RandSeq(rand.Intn(col.filedTypeM))
		}
	case KindBool:
		return rand.Intn(2)
	case KindDATE:
		randTime := time.Unix(MinDATETIME.Unix()+rand.Int63n(GapDATETIMEUnix), 0)
		return randTime.Format(TimeFormatForDATE)
	case KindTIME:
		randTime := time.Unix(MinTIMESTAMP.Unix()+rand.Int63n(GapTIMESTAMPUnix), 0)
		return randTime.Format(TimeFormatForTIME)
	case KindDATETIME:
		randTime := randTime(MinDATETIME, GapDATETIMEUnix)
		return randTime.Format(TimeFormat)
	case KindTIMESTAMP:
		randTime := randTime(MinTIMESTAMP, GapTIMESTAMPUnix)
		return randTime.Format(TimeFormat)
	case KindYEAR:
		return rand.Intn(254) + 1901 //1901 ~ 2155
	case KindJSON:
		return col.randJsonValue()
	case KindEnum:
		setLen := len(col.setValue)
		if setLen == 0 {
			// Type is change.
			return col.randValue()
		}
		i := rand.Intn(len(col.setValue))
		return col.setValue[i]
	case KindSet:
		var l int
		for l == 0 {
			setLen := len(col.setValue)
			if setLen == 0 {
				// Type is change.
				return col.randValue()
			}
			l = rand.Intn(len(col.setValue))
		}
		idxs := make([]int, l)
		m := make(map[int]struct{})
		for i := 0; i < l; i++ {
			idx := rand.Intn(len(col.setValue))
			_, ok := m[idx]
			for ok {
				idx = rand.Intn(len(col.setValue))
				_, ok = m[idx]
			}
			m[idx] = struct{}{}
			idxs[i] = idx
		}
		sort.Ints(idxs)
		s := ""
		for i := range idxs {
			if i > 0 {
				s += ","
			}
			s += col.setValue[idxs[i]]
		}
		return s
	default:
		return nil
	}
}

func randTime(minTime time.Time, gap int64) time.Time {
	// https://github.com/chronotope/chrono-tz/issues/23
	// see all invalid time: https://timezonedb.com/time-zones/Asia/Shanghai
	var randTime time.Time
	for {
		randTime = time.Unix(minTime.Unix()+rand.Int63n(gap), 0).In(Local)
		if NotAmbiguousTime(randTime) {
			break
		}
	}
	return randTime
}

func (col *ddlTestColumn) randJsonValue() string {
	for _, dCol := range col.dependenciedCols {
		col.mValue[dCol.nameOfGen] = dCol.randValue()
	}
	jsonRow, _ := json.Marshal(col.mValue)
	return string(jsonRow)
}

// randValueUnique use for primary key column to get unique value
func (col *ddlTestColumn) randValueUnique(rows *arraylist.List) (interface{}, bool) {
	// retry times
	for i := 0; i < 10; i++ {
		v := col.randValue()
		flag := true
		if rows.Contains(v) {
			flag = false
		}
		if flag {
			return v, true
		}
	}
	return nil, false
}

func (col *ddlTestColumn) canBePrimary() bool {
	return col.canBeIndex() && col.notGenerated()
}

func (col *ddlTestColumn) canBeIndex() bool {
	switch col.k {
	case KindChar, KindVarChar:
		if col.filedTypeM == 0 {
			return false
		} else {
			return true
		}
	case KindBLOB, KindTINYBLOB, KindMEDIUMBLOB, KindLONGBLOB, KindTEXT, KindTINYTEXT, KindMEDIUMTEXT, KindLONGTEXT, KindJSON:
		return false
	default:
		return true
	}
}

func (col *ddlTestColumn) canBeSet() bool {
	return col.notGenerated()
}

func (col *ddlTestColumn) canBeWhere() bool {
	switch col.k {
	case KindJSON:
		return false
	default:
		return true
	}
}

// BLOB, TEXT, GEOMETRY or JSON column 'b' can't have a default value")
func (col *ddlTestColumn) canHaveDefaultValue() bool {
	switch col.k {
	case KindBLOB, KindTINYBLOB, KindMEDIUMBLOB, KindLONGBLOB, KindTEXT, KindTINYTEXT, KindMEDIUMTEXT, KindLONGTEXT, KindJSON:
		return false
	default:
		return true
	}
}

type ddlTestIndex struct {
	name      string
	signature string
	columns   []*ddlTestColumn
}

func (col *ddlTestColumn) normalizeDataType() string {
	switch col.k {
	case KindTINYINT:
		return "tinyint(4)"
	case KindSMALLINT:
		return "smallint(6)"
	case KindMEDIUMINT:
		return "mediumint(9)"
	case KindInt32:
		return "int(11)"
	case KindBigInt:
		return "bigint(20)"
	case KindBit:
		return fmt.Sprintf("bit(%d)", col.filedTypeM)
	case KindDECIMAL:
		return fmt.Sprintf("decimal(%d,%d)", col.filedTypeM, col.filedTypeD)
	case KindFloat:
		return "float"
	case KindDouble:
		return "double"
	case KindChar:
		return fmt.Sprintf("char(%d)", col.filedTypeM)
	case KindVarChar:
		return fmt.Sprintf("varchar(%d)", col.filedTypeM)
	case KindBLOB, KindTINYBLOB, KindMEDIUMBLOB, KindLONGBLOB:
		return "xxx"
	case KindTEXT, KindTINYTEXT, KindMEDIUMTEXT, KindLONGTEXT:
		return "xxx"
	case KindBool:
		return "tinyint(1)"
	case KindDATE:
		return "date"
	case KindTIME:
		return "time"
	case KindDATETIME:
		return "datetime"
	case KindTIMESTAMP:
		return "timestamp"
	case KindYEAR:
		return "year(4)"
	case KindJSON:
		return "json"
	case KindEnum:
		s := strings.Replace(col.fieldType, "ENUM", "enum", 1)
		s = strings.Replace(col.fieldType, "\"", "'", -1)
		return strings.Replace(s, ", ", ",", -1)
	case KindSet:
		s := strings.Replace(col.fieldType, "SET", "set", 1)
		s = strings.Replace(col.fieldType, "\"", "'", -1)
		return strings.Replace(s, ", ", ",", -1)
	default:
		return col.fieldType
	}
}

// Use it after we support column charset.
func getCharsetLen(cs string) int {
	switch cs {
	case "utf8":
		return 3
	case "utf8mb4":
		return 4
	case "ascii":
		return 1
	case "latin1":
		return 1
	case "binary":
		return 1
	case "gbk":
		return 2
	default:
		return 1
	}
}
