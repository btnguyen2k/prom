// go run Commons.go PromLogAndMetrics.go
package main

import (
	"fmt"
	"github.com/btnguyen2k/prom"
)

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
		_, err := sqlC.GetDBProxy().Exec(sqlStm)
		if err != nil {
			panic(err)
		}
	}
	metrics, err := sqlC.Metrics(prom.MetricsCatAll, prom.MetricsOpts{ReturnLatestCommands: 100})
	if err != nil {
		panic(err)
	}

	fmt.Printf("Last %d queries:\n", len(metrics.LastNCmds))
	for i, cmd := range metrics.LastNCmds {
		fmt.Printf("  %02d/Execution time (ms): %06.3f: %s\n", i+1, cmd.Cost/1000, cmd.CmdRequest)
	}

	fmt.Printf("Execution time (ms) - P99: %.3f, P95: %.3f, P90: %.3f, P75: %.3f, P50: %.3f\n",
		metrics.P99Cost/1000, metrics.P95Cost/1000, metrics.P90Cost/1000, metrics.P75Cost/1000, metrics.P50Cost/1000)
}
