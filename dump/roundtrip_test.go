package dump

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/PingCAP-QE/schrddl/dbconfig"
)

// Opt in with a disposable database and the normal SCHRDDL authentication/TLS
// settings. This verifies the dump is restorable, including generated columns,
// binary bytes, SQL-mode-independent text escaping, JSON and temporal values.
func TestDumpRoundTrip(t *testing.T) {
	address := os.Getenv("SCHRDDL_TEST_ADDR")
	if address == "" {
		t.Skip("requires disposable MySQL/TiDB")
	}
	const database = "security_dump"
	db, err := dbconfig.Open(address, database)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table := "dump` ; $(touch INJECTED)"
	qualified := identifier(table)
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := db.Exec(query, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec("DROP TABLE IF EXISTS " + qualified)
	defer db.Exec("DROP TABLE IF EXISTS " + qualified)
	exec("CREATE TABLE " + qualified + " (id INT PRIMARY KEY, s TEXT, b BLOB, d DECIMAL(20,5), j JSON, dt DATETIME(6), flag BIT(9), n INT, g GEOMETRY, gen INT AS (id+1) STORED)")
	exec("INSERT INTO "+qualified+" (id,s,b,d,j,dt,flag,n,g) VALUES (?,?,?,?,?,?,?,?,ST_GeomFromText('POINT(1 2)'))", -123, "text\n'\\\x00; DROP TABLE victim; --", []byte{0, 255, 39, 10}, "-123456789.12345", `{"message":"quoted'\\ and unicode中文"}`, "2026-11-04 12:34:56.123456", 257, nil)
	read := func() []string {
		t.Helper()
		rows, err := db.Query("SELECT id,s,HEX(b),d,CAST(j AS CHAR),CAST(dt AS CHAR),HEX(flag),n,gen,ST_AsText(g) FROM " + qualified)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatal("no row")
		}
		values := make([]any, 10)
		pointers := make([]any, 10)
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			t.Fatal(err)
		}
		result := make([]string, 10)
		for i, v := range values {
			if b, ok := v.([]byte); ok {
				result[i] = string(b)
			} else {
				result[i] = fmt.Sprint(v)
			}
		}
		return result
	}
	before := read()
	directory := filepath.Join(t.TempDir(), "space ; $(touch INJECTED)")
	if err := DumpToFile(database, []string{table}, directory, address); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	var schemaSQL, dataSQL string
	for _, file := range files {
		info, err := file.Info()
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0600 {
			t.Fatal("public dump file")
		}
		text, err := os.ReadFile(filepath.Join(directory, file.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasSuffix(file.Name(), "-schema.sql") {
			schemaSQL = string(text)
		} else {
			dataSQL = string(text)
		}
	}
	if schemaSQL == "" || dataSQL == "" {
		t.Fatal("missing dump")
	}
	exec("DROP TABLE " + qualified)
	exec("SET SESSION sql_mode='NO_BACKSLASH_ESCAPES,STRICT_ALL_TABLES'")
	exec(schemaSQL)
	for _, statement := range strings.Split(strings.TrimSpace(dataSQL), "\n") {
		exec(statement)
	}
	if after := read(); !reflect.DeepEqual(before, after) {
		t.Fatalf("roundtrip changed values:\nbefore=%q\nafter=%q", before, after)
	}
	// Ensure a missing secret cannot accidentally trigger a root/empty-password dump.
	t.Setenv("SCHRDDL_DB_PASSWORD_FILE", "")
	t.Setenv("SCHRDDL_ALLOW_INSECURE", "")
	if err := DumpToFile(database, []string{table}, t.TempDir(), address); err == nil {
		t.Fatal("dump accepted missing password")
	}
}
