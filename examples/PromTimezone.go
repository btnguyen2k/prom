// go run Commons.go PromTimezone.go
package main

import (
	"fmt"
	promsql "github.com/btnguyen2k/prom/sql"
	"time"
)

func main() {
	locationStrs := []string{"Asia/Ho_Chi_Minh", "America/Los_Angeles", "Australia/Sydney"}
	locations := make([]*time.Location, 0)
	sqlCArr := make([]*promsql.SqlConnect, 0)
	for _, locStr := range locationStrs {
		loc, err := time.LoadLocation(locStr)
		if err != nil {
			panic(err)
		}
		locations = append(locations, loc)

		sqlC := newSqlConnect()
		sqlC.SetLocation(loc)
		sqlCArr = append(sqlCArr, sqlC)
	}
	defer func() {
		for _, sqlC := range sqlCArr {
			_ = sqlC.Close()
		}
	}()

	v, _ := time.ParseInLocation("2006-01-02 15:04:05", "2023-12-26 09:27:18", locations[0])
	executeSql(sqlCArr[0], "DROP TABLE IF EXISTS tbl_temp")
	executeSql(sqlCArr[0], "CREATE TABLE tbl_temp (id BIGINT PRIMARY KEY, t TIMESTAMP(0), tz TIMESTAMP(0) WITH TIME ZONE)")
	executeSql(sqlCArr[0], "INSERT INTO tbl_temp (id, t, tz) VALUES ($1, $2, $3)", 0, v, v)

	fmt.Printf("Original value (%s): %s\n", locations[0], v)
	for _, sqlC := range sqlCArr {
		dbrows := executeQuery(sqlC, "SELECT * FROM tbl_temp WHERE id=$1", 0)
		rows, err := sqlC.FetchRows(dbrows)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Fetched value (%s):\n\tt : %s\n\ttz: %s\n", sqlC.GetLocation(), rows[0]["t"], rows[0]["tz"])
		_ = dbrows.Close()
	}
}
