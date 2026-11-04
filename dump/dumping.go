package dump

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/PingCAP-QE/schrddl/dbconfig"
)

// DumpToFile uses the workload's verified DB connector for diagnostic dumps too.
// Dumpling's default connector permits an unauthenticated plaintext fallback.
func DumpToFile(dbName string, tables []string, directory string, address string) error {
	directory = strings.TrimPrefix(strings.TrimPrefix(directory, "local://"), "file://")
	if strings.Contains(directory, "://") {
		return fmt.Errorf("diagnostic dump requires a local directory")
	}
	db, err := dbconfig.Open(address, dbName)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	for _, table := range tables {
		if err := dumpTable(ctx, tx, dbName, table, directory); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func identifier(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }

func privateFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
}

func dumpTable(ctx context.Context, tx *sql.Tx, database, table, directory string) error {
	qualified := identifier(database) + "." + identifier(table)
	schema, err := tx.QueryContext(ctx, "SHOW CREATE TABLE "+qualified)
	if err != nil {
		return err
	}
	cols, err := schema.Columns()
	if err != nil {
		schema.Close()
		return err
	}
	if len(cols) < 2 {
		schema.Close()
		return fmt.Errorf("invalid table schema response")
	}
	values := make([]sql.RawBytes, len(cols))
	destinations := make([]any, len(cols))
	for i := range values {
		destinations[i] = &values[i]
	}
	if !schema.Next() {
		err := schema.Err()
		schema.Close()
		if err != nil {
			return err
		}
		return fmt.Errorf("missing table schema")
	}
	if err := schema.Scan(destinations...); err != nil {
		schema.Close()
		return err
	}
	createSQL := string(values[1])
	isView := strings.EqualFold(cols[1], "Create View")
	schema.Close()
	base := url.PathEscape(database) + "." + url.PathEscape(table)
	f, err := privateFile(filepath.Join(directory, base+"-schema.sql"))
	if err != nil {
		return err
	}
	_, writeErr := fmt.Fprintln(f, createSQL+";")
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	if isView {
		return nil
	}
	// Generated columns cannot be assigned during restore.
	metadata, err := tx.QueryContext(ctx, "SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND EXTRA NOT LIKE '%VIRTUAL GENERATED%' AND EXTRA NOT LIKE '%STORED GENERATED%' ORDER BY ORDINAL_POSITION", database, table)
	if err != nil {
		return err
	}
	var names []string
	for metadata.Next() {
		var name string
		if err := metadata.Scan(&name); err != nil {
			metadata.Close()
			return err
		}
		names = append(names, identifier(name))
	}
	err = metadata.Err()
	metadata.Close()
	if err != nil {
		return err
	}
	if len(names) == 0 {
		return fmt.Errorf("no writable columns for dump")
	}
	columns := strings.Join(names, ",")
	rows, err := tx.QueryContext(ctx, "SELECT "+columns+" FROM "+qualified)
	if err != nil {
		return err
	}
	defer rows.Close()
	types, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	f, err = privateFile(filepath.Join(directory, base+".sql"))
	if err != nil {
		return err
	}
	defer f.Close()
	values = make([]sql.RawBytes, len(names))
	destinations = make([]any, len(names))
	for i := range values {
		destinations[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(destinations...); err != nil {
			return err
		}
		literals := make([]string, len(names))
		for i, value := range values {
			literals[i] = dumpLiteral(value, types[i].DatabaseTypeName())
		}
		if _, err := fmt.Fprintf(f, "INSERT INTO %s (%s) VALUES (%s);\n", identifier(table), columns, strings.Join(literals, ",")); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return f.Close()
}

// Hex encoding avoids SQL-mode-dependent string escaping. Convert textual values
// back to text so JSON, numbers and temporal columns retain their input semantics.
func dumpLiteral(value []byte, kind string) string {
	if value == nil {
		return "NULL"
	}
	literal := "X'" + hex.EncodeToString(value) + "'"
	switch kind {
	case "BINARY", "VARBINARY", "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "BIT":
		return literal
	case "GEOMETRY":
		// MySQL's wire value prepends a little-endian SRID to the WKB.
		if len(value) >= 4 {
			return fmt.Sprintf("ST_GeomFromWKB(X'%s',%d)", hex.EncodeToString(value[4:]), binary.LittleEndian.Uint32(value[:4]))
		}
		return "ST_GeomFromWKB(" + literal + ")"
	default:
		return "CONVERT(" + literal + " USING utf8mb4)"
	}
}
