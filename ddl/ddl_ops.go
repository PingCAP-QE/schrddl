package ddl

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"runtime/debug"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/emirpasic/gods/lists/arraylist"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/twinj/uuid"
)

var globalDDLSeqNumMu sync.Mutex
var globalDDLSeqNum uint64
var globalCancelMu sync.Mutex
var globalCheckDDLMu sync.Mutex

type DDLKind = int

const (
	ddlAddTable DDLKind = iota
	ddlAddIndex
	ddlAddColumn
	ddlCreateSchema
	ddlCreateView

	ddlDropTable
	ddlDropIndex
	ddlDropColumn
	ddlDropSchema

	ddlRenameTable
	ddlRenameIndex
	ddlTruncateTable
	ddlShardRowID
	ddlRebaseAutoID
	ddlSetDefaultValue
	ddlModifyColumn
	ddlModifyTableComment
	ddlModifyTableCharsetAndCollate
	// ddlModifyColumn2 is used to test column type change.
	ddlModifyColumn2
	ddlSetTiflashReplica

	ddlMultiSchemaChange

	ddlKindNil
)

var mapOfDDLKind = map[string]DDLKind{
	"create schema": ddlCreateSchema,
	"create table":  ddlAddTable,
	"add index":     ddlAddIndex,
	"add column":    ddlAddColumn,

	"drop schema": ddlDropSchema,
	"drop table":  ddlDropTable,
	"drop index":  ddlDropIndex,
	"drop column": ddlDropColumn,

	"create view": ddlCreateView,

	"rename table":                     ddlRenameTable,
	"rename index":                     ddlRenameIndex,
	"truncate table":                   ddlTruncateTable,
	"shard row ID":                     ddlShardRowID,
	"rebase auto_increment ID":         ddlRebaseAutoID,
	"set default value":                ddlSetDefaultValue,
	"modify table comment":             ddlModifyTableComment,
	"modify table charset and collate": ddlModifyTableCharsetAndCollate,

	"modify column":  ddlModifyColumn,
	"modify column2": ddlModifyColumn2,

	"multi schema change": ddlMultiSchemaChange,
	"set tiflash replica": ddlSetTiflashReplica,
}

var mapOfDDLKindToString = map[DDLKind]string{
	ddlCreateSchema: "create schema",
	ddlAddTable:     "create table",
	ddlAddIndex:     "add index",
	ddlAddColumn:    "add column",

	ddlDropSchema: "drop schema",
	ddlDropTable:  "drop table",
	ddlDropIndex:  "drop index",
	ddlDropColumn: "drop column",

	ddlCreateView: "create view",

	ddlRenameTable:                  "rename table",
	ddlRenameIndex:                  "rename index",
	ddlTruncateTable:                "truncate table",
	ddlShardRowID:                   "shard row ID",
	ddlRebaseAutoID:                 "rebase auto_increment ID",
	ddlSetDefaultValue:              "set default value",
	ddlModifyTableComment:           "modify table comment",
	ddlModifyTableCharsetAndCollate: "modify table charset and collate",
	ddlModifyColumn:                 "modify column",
	ddlModifyColumn2:                "modify column2",
	ddlMultiSchemaChange:            "multi schema change",
	ddlSetTiflashReplica:            "set tiflash replica",
}

type ddlJob struct {
	id         int
	schemaName string
	tableName  string
	k          DDLKind
	jobState   string
	tableID    string
	schemaID   string
}

type ddlJobArg unsafe.Pointer

type ddlJobTask struct {
	ddlID      int
	k          DDLKind
	tblInfo    *ddlTestTable
	schemaInfo *ddlTestSchema
	viewInfo   *ddlTestView
	sql        string
	arg        ddlJobArg
	err        error // err is an error executed by the remote TiDB.
	isSubJob   bool
}

func getStartDDLSeqNum(db *sql.DB) (uint64, error) {
	var seq uint64
	conn, err := db.Conn(context.Background())
	if err != nil {
		return 0, err
	}
	conn.ExecContext(context.Background(), "use test")
	conn.ExecContext(context.Background(), "create table test_get_start_ddl_seq_num(a int)")
	conn.ExecContext(context.Background(), "drop table test_get_start_ddl_seq_num")
	rows, err := conn.QueryContext(context.Background(), "select json_extract(@@tidb_last_ddl_info, '$.seq_num');")
	if err != nil {
		return 0, err
	}
	rows.Next()
	err = rows.Scan(&seq)
	if err != nil {
		return 0, err
	}
	err = rows.Close()
	if err != nil {
		return 0, err
	}
	err = conn.Close()
	if err != nil {
		return 0, err
	}
	return seq, nil
}

func (c *testCase) checkSchema() error {
	var charset string
	var collate string
	for _, schema := range c.schemas {
		row, err := c.dbs[0].Query(fmt.Sprintf("select DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME from information_schema.schemata where schema_name='%s'", schema.name))
		if err != nil {
			return err
		}
		row.Next()
		err = row.Scan(&charset, &collate)
		if err != nil {
			return err
		}
		err = row.Close()
		if err != nil {
			return err
		}
		if !strings.EqualFold(charset, schema.charset) || !strings.EqualFold(collate, schema.collate) {
			return errors.Errorf("schema charset or collation doesn't match, expected charset:%s, collate:%s, got charset:%s, collate:%s", schema.charset, schema.collate, charset, collate)
		}
	}
	return nil
}

func (c *testCase) checkTableColumns(table *ddlTestTable) error {
	var columnCnt int
	var defaultValueRaw interface{}
	var defaultValue string
	var dateType string

	row, err := c.dbs[0].Query(fmt.Sprintf("select count(*) from information_schema.columns where table_name='%s';", table.name))
	if err != nil {
		return err
	}
	row.Next()
	err = row.Scan(&columnCnt)
	if err != nil {
		return err
	}
	if columnCnt != table.columns.Size() {
		return errors.Errorf("table %s column cnt are not same, expected cnt: %d, get cnt: %d, \n %s", table.name, table.columns.Size(), columnCnt, table.debugPrintToString())
	}
	row.Close()

	for ite := table.columns.Iterator(); ite.Next(); {
		column := ite.Value().(*ddlTestColumn)
		row, err = c.dbs[0].Query(fmt.Sprintf("select COLUMN_DEFAULT, COLUMN_TYPE from information_schema.columns where table_name='%s' and column_name='%s'", table.name, column.name))
		if err != nil {
			return err
		}
		ok := row.Next()
		if !ok {
			return errors.New(fmt.Sprintf("no data for column %s, table %s", column.name, table.name))
		}
		err = row.Scan(&defaultValueRaw, &dateType)
		if err != nil {
			log.Errorf("error %s, stack %s", err.Error(), debug.Stack())
			return err
		}
		row.Close()
		if defaultValueRaw == nil {
			defaultValue = "NULL"
		} else {
			defaultValue = fmt.Sprintf("%s", defaultValueRaw)
		}
		expectedDefault := getDefaultValueString(column.k, column.defaultValue)
		expectedDefault = strings.Trim(expectedDefault, "'")
		if column.k == KindTIMESTAMP {
			t, err := time.ParseInLocation(TimeFormat, expectedDefault, Local)
			if err != nil {
				log.Errorf("error %s, stack %s", err.Error(), debug.Stack())
				return err
			}
			expectedDefault = t.Format(TimeFormat)
		}
		if !column.canHaveDefaultValue() {
			expectedDefault = "NULL"
		}
		if !strings.EqualFold(defaultValue, expectedDefault) {
			return errors.Errorf("column default value doesn't match, table %s, column %s, expected default:%s, got default:%s", table.name, column.name, strings.Trim(expectedDefault, "'"), defaultValue)
		}
		expectedFieldType := column.normalizeDataType()
		if expectedFieldType == "xxx" {
			// We don't know the column's charset for now, so skip the check for text/blob.
			dateType = "xxx"
		}
		if !strings.EqualFold(dateType, expectedFieldType) {
			return errors.Errorf("column field type doesn't match, table %s, column %s, expected default:%s, got default:%s", table.name, column.name, expectedFieldType, dateType)
		}
	}
	return nil
}

func (c *testCase) checkTableIndexes(table *ddlTestTable) error {
	var indexCnt int
	var columnNames string
	row, err := c.dbs[0].Query(fmt.Sprintf("select count(*) from (select distinct index_name from information_schema.statistics where table_name='%s' and index_name != 'PRIMARY') as tmp;;", table.name))
	if err != nil {
		return err
	}
	row.Next()
	err = row.Scan(&indexCnt)
	if err != nil {
		return err
	}
	if indexCnt != len(table.indexes) {
		return errors.Errorf("table %s index cnt are not same, expected cnt: %d, got cnt: %d \n %s", table.name, len(table.indexes), indexCnt, table.debugPrintToString())
	}
	row.Close()
	for _, idx := range table.indexes {
		row, err = c.dbs[0].Query(fmt.Sprintf("select GROUP_CONCAT(column_name ORDER BY seq_in_index) from information_schema.statistics where table_name='%s' and index_name='%s';", table.name, idx.name))
		if err != nil {
			return err
		}
		row.Next()
		err = row.Scan(&columnNames)
		row.Close()
		if err != nil {
			return err
		}
		if idx.signature != columnNames {
			return errors.Errorf("table index columns doesn't match, index name: %s, expected: %s, got: %s", idx.name, idx.signature, columnNames)
		}
	}
	return nil
}

func (c *testCase) checkTable() error {
	var collate string
	var comment string
	for _, table := range c.tables {
		row, err := c.dbs[0].Query(fmt.Sprintf("select TABLE_COLLATION, TABLE_COMMENT from information_schema.tables where table_name='%s'", table.name))
		if err != nil {
			return err
		}
		row.Next()
		err = row.Scan(&collate, &comment)
		if err != nil {
			log.Errorf("error %s, stack %s", err.Error(), debug.Stack())
			return err
		}
		row.Close()
		if !strings.EqualFold(collate, table.collate) || !strings.EqualFold(comment, table.comment) {
			return errors.Errorf("table collate or comment doesn't match, table name: %s, expected collate:%s, comment:%s, got collate:%s, comment:%s", table.name, table.collate, table.comment, collate, comment)
		}
		// Check columns
		if err = c.checkTableColumns(table); err != nil {
			return err
		}
		// Check indexes
		if err = c.checkTableIndexes(table); err != nil {
			return err

		}
	}
	return nil
}

func getLastDDLInfo(conn *sql.Conn) (uint64, string, error) {
	row, err := conn.QueryContext(context.Background(), "select json_extract(@@tidb_last_ddl_info, '$.seq_num'), json_extract(@@tidb_last_ddl_info, '$.query');")
	if err != nil {
		return 0, "", err
	}
	var seqNum uint64
	var query string
	row.Next()
	err = row.Scan(&seqNum, &query)
	if err != nil {
		return 0, "", err
	}
	return seqNum, query, row.Close()
}

func (c *testCase) getTable(t interface{}) *ddlTestTable {
	if t == nil {
		return c.pickupRandomTable()
	} else {
		return t.(*multiSchemaChangeCtx).tblInfo
	}
}

func (c *testCase) generateCreateSchema(repeat int) error {
	for i := 0; i < repeat; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.prepareCreateSchema, nil, ddlCreateSchema})
	}
	return nil
}

var dbSchemaSyntax = [...]string{"DATABASE", "SCHEMA"}

func (c *testCase) prepareCreateSchema(_ interface{}, taskCh chan *ddlJobTask) error {
	charset, collate := c.pickupRandomCharsetAndCollate()
	schema := ddlTestSchema{
		name:    uuid.NewV4().String()[:8],
		deleted: false,
		charset: charset,
		collate: collate,
	}
	sql := fmt.Sprintf("CREATE %s `%s` CHARACTER SET '%s' COLLATE '%s'", dbSchemaSyntax[rand.Intn(len(dbSchemaSyntax))], schema.name,
		charset, collate)
	task := &ddlJobTask{
		k:          ddlCreateSchema,
		sql:        sql,
		schemaInfo: &schema,
	}
	taskCh <- task
	return nil
}

func (c *testCase) createSchemaJob(task *ddlJobTask) error {
	c.schemas[task.schemaInfo.name] = task.schemaInfo
	return nil
}

func (c *testCase) generateDropSchema(repeat int) error {
	for i := 0; i < repeat; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.prepareDropSchema, nil, ddlDropSchema})
	}
	return nil
}

func (c *testCase) prepareDropSchema(_ interface{}, taskCh chan *ddlJobTask) error {
	schema := c.pickupRandomSchema()
	if schema == nil {
		return nil
	}
	schema.setDeleted()
	sql := fmt.Sprintf("DROP %s `%s`", dbSchemaSyntax[rand.Intn(len(dbSchemaSyntax))], schema.name)
	task := &ddlJobTask{
		k:          ddlDropSchema,
		sql:        sql,
		schemaInfo: schema,
	}
	taskCh <- task
	return nil
}

func (c *testCase) dropSchemaJob(task *ddlJobTask) error {
	if c.isSchemaDeleted(task.schemaInfo) {
		return fmt.Errorf("schema %s doesn't exist", task.schemaInfo.name)
	}
	delete(c.schemas, task.schemaInfo.name)
	return nil
}

func (c *testCase) generateAddTable(repeat int) error {
	for i := 0; i < repeat; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.prepareAddTable, nil, ddlAddTable})
	}
	return nil
}

func (c *testCase) prepareAddTable(cfg interface{}, taskCh chan *ddlJobTask) error {
	columnCount := rand.Intn(c.cfg.TablesToCreate) + 2
	tableColumns := arraylist.New()
	var partitionColumnName string
	for i := 0; i < columnCount; i++ {
		columns := getRandDDLTestColumns()
		for _, column := range columns {
			tableColumns.Add(column)
			if column.k <= KindBigInt && partitionColumnName == "" {
				partitionColumnName = column.name
			}
		}
	}

	// Generate primary key with [0, 3) size
	primaryKeyFields := rand.Intn(3)
	primaryKeys := make([]int, 0)
	if primaryKeyFields > 0 {
		// Random elections column as primary key, but also check the column whether can be primary key.
		perm := rand.Perm(tableColumns.Size())[0:primaryKeyFields]
		for _, columnIndex := range perm {
			column := getColumnFromArrayList(tableColumns, columnIndex)
			if column.canBePrimary() {
				column.isPrimaryKey = true
				primaryKeys = append(primaryKeys, columnIndex)
			}
		}
		primaryKeyFields = len(primaryKeys)
	}

	charset, collate := c.pickupRandomCharsetAndCollate()

	tableInfo := ddlTestTable{
		name:         uuid.NewV4().String()[:8],
		columns:      tableColumns,
		indexes:      make([]*ddlTestIndex, 0),
		numberOfRows: 0,
		deleted:      0,
		comment:      uuid.NewV4().String()[:8],
		charset:      charset,
		collate:      collate,
		lock:         new(sync.RWMutex),
	}

	sql := fmt.Sprintf("CREATE TABLE `%s` (", tableInfo.name)
	for i := 0; i < tableInfo.columns.Size(); i++ {
		if i > 0 {
			sql += ", "
		}
		column := getColumnFromArrayList(tableColumns, i)
		sql += fmt.Sprintf("`%s` %s", column.name, column.getDefinition())
	}
	if primaryKeyFields > 0 {
		sql += ", PRIMARY KEY ("
		for i, columnIndex := range primaryKeys {
			if i > 0 {
				sql += ", "
			}
			column := getColumnFromArrayList(tableColumns, columnIndex)
			sql += fmt.Sprintf("`%s`", column.name)
		}
		sql += ")"
	}
	sql += ")"

	sql += fmt.Sprintf("COMMENT '%s' CHARACTER SET '%s' COLLATE '%s'",
		tableInfo.comment, charset, collate)

	if rand.Intn(3) == 0 && partitionColumnName != "" {
		sql += fmt.Sprintf(" partition by hash(`%s`) partitions %d ", partitionColumnName, rand.Intn(10)+1)
	}

	task := &ddlJobTask{
		k:       ddlAddTable,
		sql:     sql,
		tblInfo: &tableInfo,
	}
	taskCh <- task
	return nil
}

func (c *testCase) addTableInfo(task *ddlJobTask) error {
	c.tablesLock.Lock()
	defer c.tablesLock.Unlock()
	c.tables[task.tblInfo.name] = task.tblInfo
	c.tableMap[task.tblInfo.name] = task.tblInfo.mapTableToRandTestTable()
	return nil
}

func (c *testCase) generateRenameTable(repeat int) error {
	for i := 0; i < repeat; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.prepareRenameTable, nil, ddlRenameTable})
	}
	return nil
}

var toAsSyntax = [...]string{"TO", "AS"}

func (c *testCase) prepareRenameTable(_ interface{}, taskCh chan *ddlJobTask) error {
	c.tablesLock.Lock()
	defer c.tablesLock.Unlock()
	table := c.pickupRandomTable()
	if table == nil {
		return nil
	}
	// Shadow copy the table.
	table.lock.Lock()
	defer table.lock.Unlock()
	newName := uuid.NewV4().String()[:8]
	sql := fmt.Sprintf("ALTER TABLE `%s` RENAME %s `%s`", table.name,
		toAsSyntax[rand.Intn(len(toAsSyntax))], newName)
	task := &ddlJobTask{
		k:       ddlRenameTable,
		sql:     sql,
		tblInfo: table,
		arg:     ddlJobArg(&newName),
	}
	taskCh <- task
	return nil
}

func (c *testCase) renameTableJob(task *ddlJobTask) error {
	c.tablesLock.Lock()
	defer c.tablesLock.Unlock()
	table := task.tblInfo
	if c.isTableDeleted(table) {
		return fmt.Errorf("table %s is not exists", table.name)
	}
	delete(c.tables, table.name)
	delete(c.tableMap, table.name)
	table.name = *(*string)(task.arg)
	c.tables[table.name] = table
	c.tableMap[table.name] = table.mapTableToRandTestTable()
	return nil
}

func (c *testCase) generateTruncateTable(repeat int) error {
	for i := 0; i < repeat; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.prepareTruncateTable, nil, ddlTruncateTable})
	}
	return nil
}

func (c *testCase) prepareTruncateTable(_ interface{}, taskCh chan *ddlJobTask) error {
	tableToTruncate := c.pickupRandomTable()
	if tableToTruncate == nil {
		return nil
	}
	sql := fmt.Sprintf("TRUNCATE TABLE `%s`", tableToTruncate.name)
	task := &ddlJobTask{
		k:       ddlTruncateTable,
		sql:     sql,
		tblInfo: tableToTruncate,
	}
	taskCh <- task
	return nil
}

func (c *testCase) truncateTableJob(task *ddlJobTask) error {
	table := task.tblInfo
	table.lock.Lock()
	defer table.lock.Unlock()
	if c.isTableDeleted(table) {
		return fmt.Errorf("table %s is not exists", task.tblInfo.name)
	}
	table.numberOfRows = 0
	for ite := table.columns.Iterator(); ite.Next(); {
		column := ite.Value().(*ddlTestColumn)
		if !column.isGenerated() {
			column.rows.Clear()
		}
	}
	return nil
}

func (c *testCase) generateModifyTableComment(repeat int) error {
	for i := 0; i < repeat; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.prepareModifyTableComment, nil, ddlModifyTableComment})
	}
	return nil
}

func (c *testCase) prepareModifyTableComment(_ interface{}, taskCh chan *ddlJobTask) error {
	table := c.pickupRandomTable()
	if table == nil {
		return nil
	}
	newComm := uuid.NewV4().String()[:8]
	sql := fmt.Sprintf("ALTER TABLE `%s` COMMENT '%s'", table.name, newComm)
	task := &ddlJobTask{
		k:       ddlModifyTableComment,
		tblInfo: table,
		sql:     sql,
		arg:     ddlJobArg(&newComm),
	}
	taskCh <- task
	return nil
}

func (c *testCase) modifyTableCommentJob(task *ddlJobTask) error {
	table := task.tblInfo
	if c.isTableDeleted(table) {
		return fmt.Errorf("table %s is not exists", table.name)
	}
	newComm := *((*string)(task.arg))
	table.comment = newComm
	return nil
}

type ddlModifyTableCharsetAndCollateJob struct {
	newCharset string
	newCollate string
}

func (c *testCase) generateModifyTableCharsetAndCollate(repeat int) error {
	for i := 0; i < repeat; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.prepareModifyTableCharsetAndCollate,
			nil, ddlModifyTableCharsetAndCollate})
	}
	return nil
}

func (c *testCase) prepareModifyTableCharsetAndCollate(_ interface{}, taskCh chan *ddlJobTask) error {
	table := c.pickupRandomTable()
	if table == nil {
		return nil
	}

	// Currently only support converting utf8 to utf8mb4.
	// But since tidb has bugs when converting utf8 to utf8mb4 if table has blob column.
	// See https://github.com/pingcap/tidb/pull/10477 for more detail.
	// So if table has blob column, we doesn't change its charset.
	// TODO: Remove blob column check when the tidb bug are fixed.
	hasBlob := false
	for ite := table.columns.Iterator(); ite.Next(); {
		col := ite.Value().(*ddlTestColumn)
		if col.k == KindBLOB || col.k == KindTINYBLOB || col.k == KindMEDIUMBLOB || col.k == KindLONGBLOB {
			hasBlob = true
			break
		}
	}
	if hasBlob {
		return nil
	}
	charset, collate := c.pickupRandomCharsetAndCollate()
	if table.charset != "utf8" || charset != "utf8mb4" {
		return nil
	}
	sql := fmt.Sprintf("ALTER TABLE `%s` CHARACTER SET '%s' COLLATE '%s'",
		table.name, charset, collate)
	task := &ddlJobTask{
		k:       ddlModifyTableCharsetAndCollate,
		sql:     sql,
		tblInfo: table,
		arg: ddlJobArg(&ddlModifyTableCharsetAndCollateJob{
			newCharset: charset,
			newCollate: collate,
		}),
	}
	taskCh <- task
	return nil
}

func (c *testCase) modifyTableCharsetAndCollateJob(task *ddlJobTask) error {
	c.tablesLock.Lock()
	defer c.tablesLock.Unlock()
	table := task.tblInfo
	if c.isTableDeleted(table) {
		return fmt.Errorf("table %s is not exists", table.name)
	}
	arg := (*ddlModifyTableCharsetAndCollateJob)(task.arg)
	table.charset = arg.newCharset
	table.collate = arg.newCollate
	c.tableMap[table.name].Collate = toCollation(table.collate)
	return nil
}

const MaxShardRowIDBits int = 7

func (c *testCase) generateShardRowID(repeat int) error {
	for i := 0; i < repeat; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.prepareShardRowID, nil, ddlShardRowID})
	}
	return nil
}

func (c *testCase) prepareShardRowID(_ interface{}, taskCh chan *ddlJobTask) error {
	// For table has auto_increment column, cannot set shard_row_id_bits to a non-zero value.
	// Since current create table, add column, and modify column job wouldn't create
	// auto_increment column, so ignore checking whether table has an auto_increment column
	// and just execute the set shard_row_id_bits job. This needed to be changed when auto_increment
	// column is generated possibly.
	table := c.pickupRandomTable()
	if table == nil {
		return nil
	}
	// Don't make shard row bits too large.
	shardRowId := rand.Intn(MaxShardRowIDBits)
	sql := fmt.Sprintf("ALTER TABLE `%s` SHARD_ROW_ID_BITS = %d", table.name, shardRowId)
	task := &ddlJobTask{
		k:       ddlShardRowID,
		tblInfo: table,
		sql:     sql,
		arg:     ddlJobArg(&shardRowId),
	}
	taskCh <- task
	return nil
}

func (c *testCase) shardRowIDJob(task *ddlJobTask) error {
	table := task.tblInfo
	if c.isTableDeleted(table) {
		return fmt.Errorf("table %s is not exists", table.name)
	}
	shardRowId := *((*int)(task.arg))
	table.shardRowId = int64(shardRowId)
	return nil
}

func (c *testCase) generateRebaseAutoID(repeat int) error {
	for i := 0; i < repeat; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.prepareRebaseAutoID, nil, ddlRebaseAutoID})
	}
	return nil
}

func (c *testCase) prepareRebaseAutoID(_ interface{}, taskCh chan *ddlJobTask) error {
	table := c.pickupRandomTable()
	if table == nil {
		return nil
	}
	newAutoID := table.newRandAutoID()
	if newAutoID < 0 {
		return nil
	}
	sql := fmt.Sprintf("alter table `%s` auto_increment=%d", table.name, newAutoID)
	task := &ddlJobTask{
		k:       ddlRebaseAutoID,
		sql:     sql,
		tblInfo: table,
	}
	taskCh <- task
	return nil
}

func (c *testCase) rebaseAutoIDJob(task *ddlJobTask) error {
	// The autoID might be different from what we specified in task, so instead, do
	// a simple query to fetch the AutoID.
	table := task.tblInfo
	table.lock.Lock()
	defer table.lock.Unlock()
	if c.isTableDeleted(table) {
		return fmt.Errorf("table %s is not exists", table.name)
	}
	sql := fmt.Sprintf("select auto_increment from information_schema.tables "+
		"where table_schema='test' and table_name='%s'", table.name)
	// Ignore check error, it doesn't matter.
	c.dbs[0].QueryRow(sql).Scan(&table.autoIncID)
	return nil
}

func (c *testCase) generateDropTable(repeat int) error {
	for i := 0; i < repeat; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.prepareDropTable, nil, ddlDropTable})
	}
	return nil
}

func (c *testCase) prepareDropTable(cfg interface{}, taskCh chan *ddlJobTask) error {
	c.tablesLock.Lock()
	defer c.tablesLock.Unlock()
	tableToDrop := c.pickupRandomTable()
	if len(c.tables) <= 1 || tableToDrop == nil {
		return nil
	}
	tableToDrop.setDeleted()
	sql := fmt.Sprintf("DROP TABLE `%s`", tableToDrop.name)

	task := &ddlJobTask{
		k:       ddlDropTable,
		sql:     sql,
		tblInfo: tableToDrop,
	}
	taskCh <- task
	return nil
}

func (c *testCase) dropTableJob(task *ddlJobTask) error {
	c.tablesLock.Lock()
	defer c.tablesLock.Unlock()
	if c.isTableDeleted(task.tblInfo) {
		return fmt.Errorf("table %s is not exists", task.tblInfo.name)
	}
	delete(c.tables, task.tblInfo.name)
	delete(c.tableMap, task.tblInfo.name)
	return nil
}

func (c *testCase) generateCreateView(repeat int) error {
	for i := 0; i < repeat; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.prepareCreateView, nil, ddlCreateView})
	}
	return nil
}

func (c *testCase) prepareCreateView(_ interface{}, taskCh chan *ddlJobTask) error {
	table := c.pickupRandomTable()
	if table == nil {
		return nil
	}
	columns := table.pickupRandomColumns()
	if len(columns) == 0 {
		return nil
	}
	view := &ddlTestView{
		name:    uuid.NewV4().String()[:8],
		columns: columns,
		table:   table,
	}
	sql := fmt.Sprintf("create view `%s` as select ", view.name)
	var i = 0
	for ; i < len(columns)-1; i++ {
		sql += fmt.Sprintf("`%s`, ", columns[i].name)
	}
	sql += fmt.Sprintf("`%s` from `%s`", columns[i].name, table.name)
	task := &ddlJobTask{
		k:        ddlCreateView,
		sql:      sql,
		viewInfo: view,
	}
	taskCh <- task
	return nil
}

func (c *testCase) createViewJob(task *ddlJobTask) error {
	c.views[task.viewInfo.name] = task.viewInfo
	return nil
}

type ddlTestIndexStrategy = int

const (
	ddlTestIndexStrategyBegin ddlTestIndexStrategy = iota
	ddlTestIndexStrategySingleColumnAtBeginning
	ddlTestIndexStrategySingleColumnAtEnd
	ddlTestIndexStrategySingleColumnRandom
	ddlTestIndexStrategyMultipleColumnRandom
	ddlTestIndexStrategyEnd
)

type ddlIndexJobArg struct {
	index *ddlTestIndex
}

func (c *testCase) generateAddIndex(repeat int) error {
	for i := 0; i < repeat; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.prepareAddIndex, nil, ddlAddIndex})
	}
	return nil
}

func generateIndexSignture(index ddlTestIndex) string {
	signature := ""
	for i, col := range index.columns {
		signature += col.name
		if i != len(index.columns)-1 {
			signature += ","
		}
	}
	return signature
}

func (c *testCase) prepareAddIndex(ctx interface{}, taskCh chan *ddlJobTask) error {
	table := c.getTable(ctx)
	if table == nil {
		return nil
	}
	strategy := rand.Intn(ddlTestIndexStrategyMultipleColumnRandom) + ddlTestIndexStrategySingleColumnAtBeginning
	// build index definition
	index := ddlTestIndex{
		name:      uuid.NewV4().String()[:8],
		signature: "",
		columns:   make([]*ddlTestColumn, 0),
		uniques:   rand.Intn(3) == 0,
	}

	switch strategy {
	case ddlTestIndexStrategySingleColumnAtBeginning:
		column0 := getColumnFromArrayList(table.columns, 0)
		if !column0.canBeIndex() {
			return nil
		}
		index.columns = append(index.columns, column0)
	case ddlTestIndexStrategySingleColumnAtEnd:
		lastColumn := getColumnFromArrayList(table.columns, table.columns.Size()-1)
		if !lastColumn.canBeIndex() {
			return nil
		}
		index.columns = append(index.columns, lastColumn)
	case ddlTestIndexStrategySingleColumnRandom:
		col := getColumnFromArrayList(table.columns, rand.Intn(table.columns.Size()))
		if !col.canBeIndex() {
			return nil
		}
		index.columns = append(index.columns, col)
	case ddlTestIndexStrategyMultipleColumnRandom:
		numberOfColumns := rand.Intn(table.columns.Size()) + 1
		// Multiple columns of one index should no more than 16.
		if numberOfColumns > 10 {
			numberOfColumns = 10
		}
		perm := rand.Perm(table.columns.Size())[:numberOfColumns]
		for _, idx := range perm {
			column := getColumnFromArrayList(table.columns, idx)
			if column.canBeIndex() {
				index.columns = append(index.columns, column)
			}
		}
	}

	for _, column := range index.columns {
		if !checkAddDropColumn(ctx, column) {
			return nil
		}
	}

	if len(index.columns) == 0 {
		return nil
	}
	index.signature = generateIndexSignture(index)

	// check whether index duplicates
	for _, idx := range table.indexes {
		if idx.signature == index.signature {
			return nil
		}
	}

	uniqueString := ""
	if index.uniques {
		uniqueString = "unique"
	}
	// build SQL
	sql := fmt.Sprintf("ALTER TABLE `%s` ADD %s INDEX `%s` (", table.name, uniqueString, index.name)
	for i, column := range index.columns {
		if i > 0 {
			sql += ", "
		}
		sql += fmt.Sprintf("`%s`", column.name)
	}
	sql += ")"

	arg := &ddlIndexJobArg{index: &index}
	task := &ddlJobTask{
		k:       ddlAddIndex,
		sql:     sql,
		tblInfo: table,
		arg:     ddlJobArg(arg),
	}
	taskCh <- task
	return nil
}

func (c *testCase) addIndexJob(task *ddlJobTask) error {
	c.tablesLock.Lock()
	defer c.tablesLock.Unlock()

	jobArg := (*ddlIndexJobArg)(task.arg)
	tblInfo := task.tblInfo

	if c.isTableDeleted(tblInfo) {
		return fmt.Errorf("table %s is not exists", tblInfo.name)
	}

	for _, column := range jobArg.index.columns {
		if tblInfo.isColumnDeleted(column) {
			return fmt.Errorf("local Execute add index %s on column %s error , column is deleted", jobArg.index.name, column.name)
		}
	}
	tblInfo.indexes = append(tblInfo.indexes, jobArg.index)
	for _, column := range jobArg.index.columns {
		column.indexReferences++
	}
	val := c.tableMap[tblInfo.name].Values
	c.tableMap[tblInfo.name] = tblInfo.mapTableToRandTestTable()
	copyRowToRandTestTable(c.tableMap[tblInfo.name], val)
	return nil
}

type ddlRenameIndexArg struct {
	index    *ddlTestIndex
	newIndex string
}

func (c *testCase) generateRenameIndex(repeat int) error {
	for i := 0; i < repeat; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.prepareRenameIndex, nil, ddlRenameIndex})
	}
	return nil
}

func (c *testCase) prepareRenameIndex(ctx interface{}, taskCh chan *ddlJobTask) error {
	table := c.getTable(ctx)
	if table == nil || len(table.indexes) == 0 {
		return nil
	}
	loc := rand.Intn(len(table.indexes))
	index := table.indexes[loc]
	newIndex := uuid.NewV4().String()[:8]
	if !checkModifyIdx(ctx, index) {
		return nil
	}
	sql := fmt.Sprintf("ALTER TABLE `%s` RENAME INDEX `%s` to `%s`",
		table.name, index.name, newIndex)
	task := &ddlJobTask{
		k:       ddlRenameIndex,
		sql:     sql,
		tblInfo: table,
		arg:     ddlJobArg(&ddlRenameIndexArg{index, newIndex}),
	}
	taskCh <- task
	return nil
}

func (c *testCase) renameIndexJob(task *ddlJobTask) error {
	c.tablesLock.Lock()
	defer c.tablesLock.Unlock()
	table := task.tblInfo
	table.lock.Lock()
	defer table.lock.Unlock()

	arg := (*ddlRenameIndexArg)(task.arg)
	if c.isTableDeleted(table) {
		return fmt.Errorf("table %s is not exists", table.name)
	}

	iOfRenameIndex := -1
	for i := range table.indexes {
		if arg.index.name == table.indexes[i].name {
			iOfRenameIndex = i
			break
		}
	}
	if iOfRenameIndex == -1 {
		if !task.isSubJob {
			return fmt.Errorf("table %s, index %s is not exists", table.name, arg.index.name)
		} else {
			return nil
		}
	}

	table.indexes[iOfRenameIndex].name = arg.newIndex
	val := c.tableMap[table.name].Values
	c.tableMap[table.name] = table.mapTableToRandTestTable()
	copyRowToRandTestTable(c.tableMap[table.name], val)
	return nil
}

func (c *testCase) generateDropIndex(repeat int) error {
	for i := 0; i < repeat; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.prepareDropIndex, nil, ddlDropIndex})
	}
	return nil
}

func (c *testCase) prepareDropIndex(ctx interface{}, taskCh chan *ddlJobTask) error {
	table := c.getTable(ctx)
	if table == nil {
		return nil
	}
	if len(table.indexes) == 0 {
		return nil
	}
	indexToDropIndex := rand.Intn(len(table.indexes))
	indexToDrop := table.indexes[indexToDropIndex]
	if !checkModifyIdx(ctx, indexToDrop) {
		return nil
	}
	sql := fmt.Sprintf("ALTER TABLE `%s` DROP INDEX `%s`", table.name, indexToDrop.name)

	arg := &ddlIndexJobArg{index: indexToDrop}
	task := &ddlJobTask{
		k:       ddlDropIndex,
		sql:     sql,
		tblInfo: table,
		arg:     ddlJobArg(arg),
	}
	taskCh <- task
	return nil
}

func (c *testCase) dropIndexJob(task *ddlJobTask) error {
	c.tablesLock.Lock()
	defer c.tablesLock.Unlock()
	jobArg := (*ddlIndexJobArg)(task.arg)
	tblInfo := task.tblInfo
	tblInfo.lock.Lock()
	defer tblInfo.lock.Unlock()

	if c.isTableDeleted(tblInfo) {
		return fmt.Errorf("table %s is not exists", tblInfo.name)
	}

	iOfDropIndex := -1
	for i := range tblInfo.indexes {
		if jobArg.index.name == tblInfo.indexes[i].name {
			iOfDropIndex = i
			break
		}
	}
	if iOfDropIndex == -1 {
		if !task.isSubJob {
			return fmt.Errorf("table %s , index %s is not exists", tblInfo.name, jobArg.index.name)
		} else {
			return nil
		}
	}

	for _, column := range jobArg.index.columns {
		column.indexReferences--
		if column.indexReferences < 0 {
			return fmt.Errorf("drop index, index.column %s Unexpected index reference", column.name)
		}
	}
	tblInfo.indexes = append(tblInfo.indexes[:iOfDropIndex], tblInfo.indexes[iOfDropIndex+1:]...)
	val := c.tableMap[tblInfo.name].Values
	c.tableMap[tblInfo.name] = tblInfo.mapTableToRandTestTable()
	copyRowToRandTestTable(c.tableMap[tblInfo.name], val)
	return nil
}

type ddlTestAddDropColumnStrategy = int

const (
	ddlTestAddDropColumnStrategyBegin ddlTestAddDropColumnStrategy = iota
	ddlTestAddDropColumnStrategyAtBeginning
	ddlTestAddDropColumnStrategyAtEnd
	ddlTestAddDropColumnStrategyAtRandom
	ddlTestAddDropColumnStrategyEnd
)

type ddlTestAddDropColumnConfig struct {
	strategy ddlTestAddDropColumnStrategy
}

type ddlColumnJobArg struct {
	origColumn        *ddlTestColumn
	column            *ddlTestColumn
	strategy          ddlTestAddDropColumnStrategy
	insertAfterColumn *ddlTestColumn
	// updateDefault is used in alter column to indicate if it updates the default value.
	updateDefault bool
}

func (c *testCase) generateAddColumn(repeat int) error {
	for i := 0; i < repeat; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.prepareAddColumn, nil, ddlAddColumn})
	}
	return nil
}

func (c *testCase) prepareAddColumn(ctx interface{}, taskCh chan *ddlJobTask) error {
	table := c.getTable(ctx)
	if table == nil {
		return nil
	}
	strategy := rand.Intn(ddlTestAddDropColumnStrategyAtRandom) + ddlTestAddDropColumnStrategyAtBeginning
	newColumn := getRandDDLTestColumn()
	if !checkAddDropColumn(ctx, newColumn) {
		return nil
	}
	insertAfterPosition := -1
	// build SQL
	sql := fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s", table.name, newColumn.name, newColumn.getDefinition())
	switch strategy {
	case ddlTestAddDropColumnStrategyAtBeginning:
		sql += " FIRST"
	case ddlTestAddDropColumnStrategyAtEnd:
		// do nothing
	case ddlTestAddDropColumnStrategyAtRandom:
		insertAfterPosition = rand.Intn(table.columns.Size())
		column := getColumnFromArrayList(table.columns, insertAfterPosition)
		if !checkRelatedColumn(ctx, column) {
			return nil
		}
		sql += fmt.Sprintf(" AFTER `%s`", column.name)
	}

	arg := &ddlColumnJobArg{
		column:   newColumn,
		strategy: strategy,
	}
	if insertAfterPosition != -1 {
		arg.insertAfterColumn = getColumnFromArrayList(table.columns, insertAfterPosition)
	}
	task := &ddlJobTask{
		k:       ddlAddColumn,
		sql:     sql,
		tblInfo: table,
		arg:     ddlJobArg(arg),
	}
	taskCh <- task
	return nil
}

func (c *testCase) addColumnJob(task *ddlJobTask) error {
	c.tablesLock.Lock()
	defer c.tablesLock.Unlock()
	jobArg := (*ddlColumnJobArg)(task.arg)
	table := task.tblInfo
	table.lock.Lock()
	defer table.lock.Unlock()

	if c.isTableDeleted(table) {
		return fmt.Errorf("table %s is not exists", table.name)
	}
	newColumn := jobArg.column
	strategy := jobArg.strategy

	newColumn.rows = arraylist.New()
	for i := 0; i < table.numberOfRows; i++ {
		newColumn.rows.Add(newColumn.defaultValue)
	}

	switch strategy {
	case ddlTestAddDropColumnStrategyAtBeginning:
		table.columns.Insert(0, newColumn)
	case ddlTestAddDropColumnStrategyAtEnd:
		table.columns.Add(newColumn)
	case ddlTestAddDropColumnStrategyAtRandom:
		insertAfterPosition := -1
		for i := 0; i < table.columns.Size(); i++ {
			column := getColumnFromArrayList(table.columns, i)
			if jobArg.insertAfterColumn.name == column.name {
				insertAfterPosition = i
				break
			}
		}
		if insertAfterPosition == -1 {
			return fmt.Errorf("table %s ,insert column %s after column, column %s is not exists ", table.name, newColumn.name, jobArg.insertAfterColumn.name)
		}
		table.columns.Insert(insertAfterPosition+1, newColumn)
	}
	val := c.tableMap[table.name].Values
	c.tableMap[table.name] = table.mapTableToRandTestTable()
	copyRowToRandTestTable(c.tableMap[table.name], val)
	return nil
}

func (c *testCase) generateModifyColumn2(repeat int) error {
	for i := 0; i < repeat; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.prepareModifyColumn2, nil, ddlModifyColumn2})
	}
	return nil
}

func (c *testCase) prepareModifyColumn2(ctx interface{}, taskCh chan *ddlJobTask) error {
	table := c.getTable(ctx)
	if table == nil {
		return nil
	}
	table.lock.Lock()
	defer table.lock.Unlock()
	origColIndex, origColumn := table.pickupRandomColumn()
	if origColumn == nil {
		return nil
	}
	var (
		sql            string
		modifiedColumn *ddlTestColumn
	)
	if rand.Float64() > 0.5 {
		// If a column has dependency, it cannot be renamed.
		if origColumn.hasGenerateCol() {
			return nil
		}
		// for change column
		modifiedColumn = generateRandModifiedColumn2(origColumn, true)
		if !checkAddDropColumn(ctx, origColumn) || !checkAddDropColumn(ctx, modifiedColumn) {
			return nil
		}
		origColumn.setRenamed()
		sql = fmt.Sprintf("alter table `%s` change column `%s` `%s` %s", table.name,
			origColumn.name, modifiedColumn.name, modifiedColumn.getDefinition())
	} else {
		if !checkModifyColumn(ctx, origColumn) {
			return nil
		}
		// for modify column
		modifiedColumn = generateRandModifiedColumn2(origColumn, false)
		sql = fmt.Sprintf("alter table `%s` modify column `%s` %s", table.name,
			origColumn.name, modifiedColumn.getDefinition())
	}
	// Inject the new offset for the new column.
	strategy := rand.Intn(ddlTestAddDropColumnStrategyAtRandom) + ddlTestAddDropColumnStrategyAtBeginning
	var insertAfterColumn *ddlTestColumn = nil
	switch strategy {
	case ddlTestAddDropColumnStrategyAtBeginning:
		sql += " FIRST"
	case ddlTestAddDropColumnStrategyAtEnd:
		endColumn := getColumnFromArrayList(table.columns, table.columns.Size()-1)
		if !checkRelatedColumn(ctx, endColumn) {
			origColumn.delRenamed()
			return nil
		}
		if endColumn.name != origColumn.name {
			sql += fmt.Sprintf(" AFTER `%s`", endColumn.name)
		}
	case ddlTestAddDropColumnStrategyAtRandom:
		insertPosition := rand.Intn(table.columns.Size())
		insertAfterColumn = getColumnFromArrayList(table.columns, insertPosition)
		if !checkRelatedColumn(ctx, insertAfterColumn) {
			origColumn.delRenamed()
			return nil
		}
		if insertPosition != origColIndex {
			sql += fmt.Sprintf(" AFTER `%s`", insertAfterColumn.name)
		}
	}
	if modifiedColumn.name == origColumn.name {
		modifiedColumn.name = ""
	}
	task := &ddlJobTask{
		// Column Type Change.
		k:       ddlModifyColumn2,
		tblInfo: table,
		sql:     sql,
		arg: ddlJobArg(&ddlColumnJobArg{
			origColumn:        origColumn,
			column:            modifiedColumn,
			strategy:          strategy,
			insertAfterColumn: insertAfterColumn,
			updateDefault:     modifiedColumn.defaultValue != origColumn.defaultValue,
		}),
	}
	taskCh <- task
	return nil
}

func (c *testCase) generateModifyColumn(repeat int) error {
	for i := 0; i < repeat; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.prepareModifyColumn, nil, ddlModifyColumn})
	}
	return nil
}

func (c *testCase) prepareModifyColumn(ctx interface{}, taskCh chan *ddlJobTask) error {
	table := c.getTable(ctx)
	if table == nil {
		return nil
	}
	table.lock.Lock()
	defer table.lock.Unlock()
	origColIndex, origColumn := table.pickupRandomColumn()
	if origColumn == nil || !origColumn.canBeModified() {
		return nil
	}
	var modifiedColumn *ddlTestColumn
	var sql string
	if rand.Float64() > 0.5 {
		// If a column has dependency, it cannot be renamed.
		if origColumn.hasGenerateCol() {
			return nil
		}
		modifiedColumn = generateRandModifiedColumn(origColumn, true)
		if !checkAddDropColumn(ctx, origColumn) || !checkAddDropColumn(ctx, modifiedColumn) {
			return nil
		}
		origColumn.setRenamed()
		sql = fmt.Sprintf("alter table `%s` change column `%s` `%s` %s", table.name,
			origColumn.name, modifiedColumn.name, modifiedColumn.getDefinition())
	} else {
		modifiedColumn = generateRandModifiedColumn(origColumn, false)
		if !checkModifyColumn(ctx, origColumn) {
			return nil
		}
		sql = fmt.Sprintf("alter table `%s` modify column `%s` %s", table.name,
			origColumn.name, modifiedColumn.getDefinition())
	}
	strategy := rand.Intn(ddlTestAddDropColumnStrategyAtRandom) + ddlTestAddDropColumnStrategyAtBeginning
	var insertAfterColumn *ddlTestColumn = nil
	switch strategy {
	case ddlTestAddDropColumnStrategyAtBeginning:
		sql += " FIRST"
	case ddlTestAddDropColumnStrategyAtEnd:
		endColumn := getColumnFromArrayList(table.columns, table.columns.Size()-1)
		if !checkRelatedColumn(ctx, endColumn) {
			origColumn.delRenamed()
			return nil
		}
		if endColumn.name != origColumn.name {
			sql += fmt.Sprintf(" AFTER `%s`", endColumn.name)
		}
	case ddlTestAddDropColumnStrategyAtRandom:
		insertPosition := rand.Intn(table.columns.Size())
		insertAfterColumn = getColumnFromArrayList(table.columns, insertPosition)
		if !checkRelatedColumn(ctx, insertAfterColumn) {
			origColumn.delRenamed()
			return nil
		}
		if insertPosition != origColIndex {
			sql += fmt.Sprintf(" AFTER `%s`", insertAfterColumn.name)
		}
	}
	if modifiedColumn.name == origColumn.name {
		modifiedColumn.name = ""
	}
	task := &ddlJobTask{
		k:       ddlModifyColumn,
		tblInfo: table,
		sql:     sql,
		arg: ddlJobArg(&ddlColumnJobArg{
			origColumn:        origColumn,
			column:            modifiedColumn,
			strategy:          strategy,
			insertAfterColumn: insertAfterColumn,
			updateDefault:     modifiedColumn.defaultValue != origColumn.defaultValue,
		}),
	}
	taskCh <- task
	return nil
}

func (c *testCase) modifyColumnJob(task *ddlJobTask) error {
	c.tablesLock.Lock()
	defer c.tablesLock.Unlock()
	table := task.tblInfo
	table.lock.Lock()
	defer table.lock.Unlock()
	if c.isTableDeleted(table) {
		return fmt.Errorf("table %s is not exists", table.name)
	}
	arg := (*ddlColumnJobArg)(task.arg)
	if c.isColumnDeleted(arg.origColumn, table) {
		return fmt.Errorf("column %s on table %s is not exists", arg.origColumn.name, table.name)
	}

	origColumnIndex := 0
	for i := 0; i < table.columns.Size(); i++ {
		col := getColumnFromArrayList(table.columns, i)
		if col.name == arg.origColumn.name {
			origColumnIndex = i
			break
		}
	}
	arg.origColumn.k = arg.column.k
	if arg.column.name != "" {
		// Rename
		arg.origColumn.name = arg.column.name
	}
	arg.origColumn.fieldType = arg.column.fieldType
	arg.origColumn.filedTypeM = arg.column.filedTypeM
	arg.origColumn.filedTypeD = arg.column.filedTypeD
	if arg.updateDefault {
		arg.origColumn.defaultValue = arg.column.defaultValue
	}
	arg.origColumn.setValue = arg.column.setValue
	table.columns.Remove(origColumnIndex)
	switch arg.strategy {
	case ddlTestAddDropColumnStrategyAtBeginning:
		table.columns.Insert(0, arg.origColumn)
	case ddlTestAddDropColumnStrategyAtEnd:
		table.columns.Add(arg.origColumn)
	case ddlTestAddDropColumnStrategyAtRandom:
		insertPosition := origColumnIndex - 1
		for i := 0; i < table.columns.Size(); i++ {
			col := getColumnFromArrayList(table.columns, i)
			if col.name == arg.insertAfterColumn.name {
				insertPosition = i
				break
			}
		}
		table.columns.Insert(insertPosition+1, arg.origColumn)
	}
	for _, idx := range table.indexes {
		idx.signature = generateIndexSignture(*idx)
	}
	val := c.tableMap[table.name].Values
	c.tableMap[table.name] = table.mapTableToRandTestTable()
	copyRowToRandTestTable(c.tableMap[table.name], val)
	return nil
}

func (c *testCase) generateDropColumn(repeat int) error {
	for i := 0; i < repeat; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.prepareDropColumn, nil, ddlDropColumn})
	}
	return nil
}

func (c *testCase) prepareDropColumn(ctx interface{}, taskCh chan *ddlJobTask) error {
	table := c.getTable(ctx)
	if table == nil {
		return nil
	}

	columnsSnapshot := table.filterColumns(table.predicateAll)
	if len(columnsSnapshot) <= 1 {
		return nil
	}

	strategy := rand.Intn(ddlTestAddDropColumnStrategyAtRandom) + ddlTestAddDropColumnStrategyAtBeginning
	columnToDropIndex := -1
	switch strategy {
	case ddlTestAddDropColumnStrategyAtBeginning:
		columnToDropIndex = 0
	case ddlTestAddDropColumnStrategyAtEnd:
		columnToDropIndex = table.columns.Size() - 1
	case ddlTestAddDropColumnStrategyAtRandom:
		columnToDropIndex = rand.Intn(table.columns.Size())
	}

	columnToDrop := getColumnFromArrayList(table.columns, columnToDropIndex)

	if !checkAddDropColumn(ctx, columnToDrop) {
		return nil
	}

	// Primary key columns cannot be dropped
	if columnToDrop.isPrimaryKey {
		return nil
	}

	// Column cannot be dropped if the column has generated column dependency
	if columnToDrop.hasGenerateCol() {
		return nil
	}

	columnToDrop.setDeleted()
	sql := fmt.Sprintf("ALTER TABLE `%s` DROP COLUMN `%s`", table.name, columnToDrop.name)

	arg := &ddlColumnJobArg{
		column:            columnToDrop,
		strategy:          strategy,
		insertAfterColumn: nil,
	}
	task := &ddlJobTask{
		k:       ddlDropColumn,
		sql:     sql,
		tblInfo: table,
		arg:     ddlJobArg(arg),
	}
	taskCh <- task
	return nil
}

func (c *testCase) dropColumnJob(task *ddlJobTask) error {
	c.tablesLock.Lock()
	defer c.tablesLock.Unlock()
	jobArg := (*ddlColumnJobArg)(task.arg)
	table := task.tblInfo
	table.lock.Lock()
	defer table.lock.Unlock()
	if c.isTableDeleted(table) {
		return fmt.Errorf("table %s is not exists", table.name)
	}
	columnToDrop := jobArg.column

	// Drop index as well.
	dropIndexCnt := 0
	tempIdx := table.indexes[:0]
	for _, idx := range table.indexes {
		if len(idx.columns) == 1 && idx.columns[0].name == columnToDrop.name {
			dropIndexCnt++
		} else {
			tempIdx = append(tempIdx, idx)
		}
	}
	table.indexes = tempIdx
	if columnToDrop.indexReferences != dropIndexCnt {
		return fmt.Errorf("local Execute drop column %s on table %s error , column has index reference %d, drop index cnt %d", jobArg.column.name, table.name, columnToDrop.indexReferences, dropIndexCnt)
	}

	dropColumnPosition := -1
	for i := 0; i < table.columns.Size(); i++ {
		column := getColumnFromArrayList(table.columns, i)
		if columnToDrop.name == column.name {
			dropColumnPosition = i
			break
		}
	}
	if dropColumnPosition == -1 {
		return fmt.Errorf("table %s ,drop column , column %s is not exists ", table.name, columnToDrop.name)
	}
	// update table definitions
	table.columns.Remove(dropColumnPosition)
	// if the drop column is a generated column , we should update the dependency column
	if columnToDrop.isGenerated() {
		col := columnToDrop.dependency
		i := 0
		for i = range col.dependenciedCols {
			if col.dependenciedCols[i].name == columnToDrop.name {
				break
			}
		}
		col.dependenciedCols = append(col.dependenciedCols[:i], col.dependenciedCols[i+1:]...)
	}
	val := c.tableMap[table.name].Values
	c.tableMap[table.name] = table.mapTableToRandTestTable()
	copyRowToRandTestTable(c.tableMap[table.name], val)
	return nil
}

type ddlSetDefaultValueArg struct {
	columnIndex     int
	column          *ddlTestColumn
	newDefaultValue interface{}
}

func (c *testCase) generateSetDefaultValue(repeat int) error {
	for i := 0; i < repeat; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.prepareSetDefaultValue, nil, ddlSetDefaultValue})
	}
	return nil
}

func (c *testCase) prepareSetDefaultValue(_ interface{}, taskCh chan *ddlJobTask) error {
	table := c.pickupRandomTable()
	if table == nil {
		return nil
	}
	columns := table.filterColumns(table.predicateAll)
	if len(columns) == 0 {
		return nil
	}
	loc := rand.Intn(len(columns))
	column := columns[loc]
	// If the chosen column cannot have default value, just return nil.
	if !column.canHaveDefaultValue() {
		return nil
	}
	newDefaultValue := column.randValue()
	sql := fmt.Sprintf("ALTER TABLE `%s` ALTER `%s` SET DEFAULT %s", table.name,
		column.name, getDefaultValueString(column.k, newDefaultValue))
	task := &ddlJobTask{
		k:       ddlSetDefaultValue,
		sql:     sql,
		tblInfo: table,
		arg: ddlJobArg(&ddlSetDefaultValueArg{
			columnIndex:     loc,
			column:          column,
			newDefaultValue: newDefaultValue,
		}),
	}
	taskCh <- task
	return nil
}

func (c *testCase) setDefaultValueJob(task *ddlJobTask) error {
	c.tablesLock.Lock()
	defer c.tablesLock.Unlock()
	table := task.tblInfo
	table.lock.Lock()
	defer table.lock.Unlock()
	if c.isTableDeleted(table) {
		return fmt.Errorf("table %s is not exists", table.name)
	}
	arg := (*ddlSetDefaultValueArg)(task.arg)
	if c.isColumnDeleted(arg.column, table) {
		return fmt.Errorf("column %s on table %s is not exists", arg.column.name, table.name)
	}
	arg.column.defaultValue = arg.newDefaultValue
	val := c.tableMap[table.name].Values
	c.tableMap[table.name] = table.mapTableToRandTestTable()
	copyRowToRandTestTable(c.tableMap[table.name], val)
	return nil
}

func (c *testCase) generateSetTilfahReplica(repeat int) error {
	for i := 0; i < repeat; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.prepareSetTiflashReplica, nil, ddlSetTiflashReplica})
	}
	return nil
}

type ddlSetTiflashReplicaArg struct {
	cnt int
}

func (c *testCase) prepareSetTiflashReplica(_ interface{}, taskCh chan *ddlJobTask) error {
	table := c.pickupRandomTable()
	if table == nil {
		return nil
	}

	cnt := rand.Intn(6)
	sql := fmt.Sprintf("ALTER TABLE `%s` SET TIFLASH REPLICA %d", table.name, cnt)
	task := &ddlJobTask{
		k:       ddlSetTiflashReplica,
		sql:     sql,
		tblInfo: table,
		arg: ddlJobArg(&ddlSetTiflashReplicaArg{
			cnt,
		}),
	}
	taskCh <- task
	return nil
}

func (c *testCase) setTiflashReplicaJob(task *ddlJobTask) error {
	table := task.tblInfo
	table.lock.Lock()
	defer table.lock.Unlock()
	if c.isTableDeleted(table) {
		return fmt.Errorf("table %s is not exists", table.name)
	}
	arg := (*ddlSetTiflashReplicaArg)(task.arg)
	table.replicaCnt = arg.cnt
	return nil
}

type ddlJobTasks []*ddlJobTask

func (tasks ddlJobTasks) Swap(i, j int) {
	tasks[i], tasks[j] = tasks[j], tasks[i]
}

func (tasks ddlJobTasks) Len() int {
	return len(tasks)
}

func (tasks ddlJobTasks) Less(i, j int) bool {
	return tasks[i].ddlID < tasks[j].ddlID
}
