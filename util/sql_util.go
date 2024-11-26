package util

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

func execWithConn(conn *sql.Conn, query string) (sql.Result, error) {
	var (
		err error
		res sql.Result
	)
	for retry := 0; retry < 10; retry++ {
		res, err = conn.ExecContext(context.Background(), query)
		if err != nil && strings.Contains(err.Error(), "context canceled") {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return res, err
	}
	return res, err
}

func QueryWithRetry(conn *sql.Conn, sql string) (*sql.Rows, error) {
	var err error
	for retry := 0; retry < 10; retry++ {
		res, err := conn.QueryContext(context.Background(), sql)
		if err != nil && strings.Contains(err.Error(), "context canceled") {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return res, err
	}

	return nil, err
}

// Execute SQL with no returning rows.
// This function will check if the error can be ignored.
func ExecSQLWithConn(conn *sql.Conn, sql string) error {
	_, err := execWithConn(conn, sql)
	if err != nil && DMLIgnoreError(err) || DDLIgnoreError(err) {
		return nil
	}
	if strings.Contains(err.Error(), "plan not match") {
		_, err = execWithConn(conn, sql)
		return err
	}
	return errors.Trace(err)
}

// Execute SQL with no returning rows.
// This function will check if the error can be ignored.
func ExecSQLWithDB(db *sql.DB, sql string) error {
	conn, err := db.Conn(context.Background())
	if err != nil {
		log.Fatal("Can't open connection")
	}
	defer conn.Close()
	_, err = execWithConn(conn, sql)
	if err != nil && DMLIgnoreError(err) || DDLIgnoreError(err) {
		return nil
	}
	if strings.Contains(err.Error(), "plan not match") {
		_, err = execWithConn(conn, sql)
		return err
	}
	return errors.Trace(err)
}

// Execute SQL and return all the rows.
func FetchRowsWithConn(conn *sql.Conn, sql string) ([][]string, error) {
	rows, err := QueryWithRetry(conn, sql)
	if err != nil {
		return nil, err
	}

	defer func() {
		rows.Close()
	}()

	// Read all rows.
	var actualRows [][]string
	for rows.Next() {
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		// See https://stackoverflow.com/questions/14477941/read-select-columns-into-string-in-go
		rawResult := make([][]byte, len(cols))
		result := make([]string, len(cols))
		dest := make([]interface{}, len(cols))
		for i := range rawResult {
			dest[i] = &rawResult[i]
		}

		err = rows.Scan(dest...)
		if err != nil {
			return nil, err
		}

		for i, raw := range rawResult {
			if raw == nil {
				result[i] = "NULL"
			} else {
				result[i] = fmt.Sprintf("'%s'", string(raw))
			}
		}

		actualRows = append(actualRows, result)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return actualRows, nil
}

// Execute SQL and return all the rows.
func FetchRowsWithDB(db *sql.DB, sql string) ([][]string, error) {
	conn, err := db.Conn(context.Background())
	if err != nil {
		log.Fatal("Can't open connection")
	}
	defer conn.Close()

	rows, err := QueryWithRetry(conn, sql)
	if err != nil {
		return nil, err
	}

	defer func() {
		rows.Close()
	}()

	// Read all rows.
	var actualRows [][]string
	for rows.Next() {
		cols, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		// See https://stackoverflow.com/questions/14477941/read-select-columns-into-string-in-go
		rawResult := make([][]byte, len(cols))
		result := make([]string, len(cols))
		dest := make([]interface{}, len(cols))
		for i := range rawResult {
			dest[i] = &rawResult[i]
		}

		err = rows.Scan(dest...)
		if err != nil {
			return nil, err
		}

		for i, raw := range rawResult {
			if raw == nil {
				result[i] = "NULL"
			} else {
				result[i] = fmt.Sprintf("'%s'", string(raw))
			}
		}

		actualRows = append(actualRows, result)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return actualRows, nil
}

func CheckResults(res1, res2 [][]string) (bool, error) {
	if len(res1) != len(res2) {
		return false, nil
	}
	sort.Slice(res1, func(i, j int) bool {
		for c := range res1[i] {
			if res1[i][c] != res1[j][c] {
				return res1[i][c] < res1[j][c]
			}
		}
		return true
	})
	sort.Slice(res2, func(i, j int) bool {
		for c := range res2[i] {
			if res2[i][c] != res2[j][c] {
				return res2[i][c] < res2[j][c]
			}
		}
		return true
	})
	for i, row1 := range res1 {
		row2 := res2[i]
		if len(row1) != len(row2) {
			return false, nil
		}
		for j, e := range row1 {
			if e != row2[j] {
				return false, nil
			}
		}
	}
	return true, nil
}
