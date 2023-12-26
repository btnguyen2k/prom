// go run Commons.go PromFetchRows.go
package main

import "fmt"

func main() {
	sqlC := newSqlConnect()
	defer func() { _ = sqlC.Close() }()

	sqls := []string{
		"DROP TABLE IF EXISTS tbl_temp",
		"CREATE TABLE tbl_temp (id BIGINT PRIMARY KEY, name VARCHAR(64))",
		"INSERT INTO tbl_temp (id, name) VALUES (0, 'name0')",
		"INSERT INTO tbl_temp (id, name) VALUES (1, 'name1')",
		"INSERT INTO tbl_temp (id, name) VALUES (2, 'name2')",
		"INSERT INTO tbl_temp (id, name) VALUES (3, 'name3')",
		"INSERT INTO tbl_temp (id, name) VALUES (4, 'name4')",
		"INSERT INTO tbl_temp (id, name) VALUES (5, 'name5')",
		"INSERT INTO tbl_temp (id, name) VALUES (6, 'name6')",
		"INSERT INTO tbl_temp (id, name) VALUES (7, 'name7')",
		"INSERT INTO tbl_temp (id, name) VALUES (8, 'name8')",
		"INSERT INTO tbl_temp (id, name) VALUES (9, 'name9')",
	}
	for _, sqlStm := range sqls {
		_, err := sqlC.GetDB().Exec(sqlStm)
		if err != nil {
			panic(err)
		}
	}

	dbrows, err := sqlC.GetDB().Query("SELECT id, name FROM tbl_temp WHERE 3 < id AND id < 9 ORDER BY id")
	if err != nil {
		panic(err)
	}
	defer func() { _ = dbrows.Close() }()

	// fetch all queried rows. Note: FetchRows will NOT close dbrows!
	rows, err := sqlC.FetchRows(dbrows)
	if err != nil {
		panic(err)
	}
	for i, row := range rows {
		// row is a map[string]interface{} type
		fmt.Printf("Row #%d: %#v / Id: %T / Name: %T\n", i, row, row["id"], row["name"])
	}
}
