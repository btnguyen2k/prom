package prom

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func _sqlcVerifyLastCommand(f _testFailedWithMsgFunc, testName string, sqlc *SqlConnect, cmdName string, cmdCats ...string) {
	for _, cat := range cmdCats {
		m, err := sqlc.Metrics(cat, MetricsOpts{ReturnLatestCommands: 1})
		if err != nil {
			f(fmt.Sprintf("%s failed: error [%e]", testName+"/Metrics("+cat+")", err))
		}
		if m == nil {
			f(fmt.Sprintf("%s failed: cannot obtain metrics info", testName+"/Metrics("+cat+")"))
		}
		if e, v := 1, len(m.LastNCmds); e != v {
			f(fmt.Sprintf("%s failed: expected %v last command returned but received %v", testName+"/Metrics("+cat+")", e, v))
		}
		cmd := m.LastNCmds[0]
		cmd.CmdRequest, cmd.CmdResponse, cmd.CmdMeta = nil, nil, nil
		if cmd.CmdName != cmdName || cmd.Result != CmdResultOk || cmd.Error != nil || cmd.Cost < 0 {
			f(fmt.Sprintf("%s failed: invalid last command metrics.\nExpected: [Name=%v / Result=%v / Error = %e / Cost %v]\nReceived: [Name=%v / Result=%v / Error = %s / Cost %v]",
				testName+"/Metrics("+cat+")",
				cmdName, CmdResultOk, error(nil), ">= 0",
				cmd.CmdName, cmd.Result, cmd.Error, cmd.Cost))
		}
	}
}

/*---------- DBProxy ----------*/

func TestDBProxy_Ping(t *testing.T) {
	testName := "TestDBProxy_Ping"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if err := sqlc.GetDBProxy().Ping(); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			}
			_sqlcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, sqlc, "ping", MetricsCatAll, MetricsCatOther)
		})
	}
}

func TestDBProxy_PingContext(t *testing.T) {
	testName := "TestDBProxy_PingContext"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if err := sqlc.GetDBProxy().PingContext(context.TODO()); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			}
			_sqlcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, sqlc, "ping", MetricsCatAll, MetricsCatOther)
		})
	}
}

func TestDBProxy_Close(t *testing.T) {
	testName := "TestDBProxy_Close"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if err := sqlc.GetDBProxy().Close(); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			}
			_sqlcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, sqlc, "close", MetricsCatAll, MetricsCatOther)
		})
	}
}

func TestDBProxy_Prepare(t *testing.T) {
	testName := "TestDBProxy_Prepare"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if err := sqlInitTable(sqlc, testSqlTableName, false); err != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err)
			}
			if _, err := sqlc.GetDBProxy().Prepare("SELECT * FROM " + testSqlTableName); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			}
			_sqlcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, sqlc, "prepare", MetricsCatAll, MetricsCatOther)
		})
	}
}

func TestDBProxy_PrepareContext(t *testing.T) {
	testName := "TestDBProxy_PrepareContext"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if err := sqlInitTable(sqlc, testSqlTableName, false); err != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err)
			}
			if _, err := sqlc.GetDBProxy().PrepareContext(context.TODO(), "SELECT * FROM "+testSqlTableName); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			}
			_sqlcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, sqlc, "prepare", MetricsCatAll, MetricsCatOther)
		})
	}
}

func TestDBProxy_Exec(t *testing.T) {
	testName := "TestDBProxy_Exec"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	type testInfo struct {
		cmd, query string
		params     []interface{}
		metricCat  string
	}
	testInfoList := []testInfo{
		{cmd: "SELECT", query: `select * from ${table}`, params: []interface{}{}, metricCat: MetricsCatOther},
		{cmd: "INSERT", query: `insert into ${table} (${colId}) VALUES (${valId})`, params: []interface{}{}, metricCat: MetricsCatDML},
		{cmd: "UPDATE", query: `update ${table} set ${setClause} where ${whereClauseId}`, params: []interface{}{}, metricCat: MetricsCatDML},
		{cmd: "DELETE", query: `delete from ${table} where ${whereClauseId}`, params: []interface{}{}, metricCat: MetricsCatDML},
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if err := sqlInitTable(sqlc, testSqlTableName, false); err != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err)
			}
			for _, ti := range testInfoList {
				ti.query = strings.ReplaceAll(ti.query, `${table}`, testSqlTableName)
				ti.query = strings.ReplaceAll(ti.query, `${colId}`, sqlTableColNames[1])
				if sqlc.GetDbFlavor() == FlavorCosmosDb {
					if ti.cmd == "SELECT" {
						continue
					}
					ti.query = strings.ReplaceAll(ti.query, `${valId}`, `"\"id\""`)
					ti.query = strings.ReplaceAll(ti.query, `${setClause}`, sqlTableColNames[2]+`="\"value\""`)
					ti.query = strings.ReplaceAll(ti.query, `${whereClauseId}`, "id=id")
					ti.params = append(ti.params, "id")
				} else {
					ti.query = strings.ReplaceAll(ti.query, `${valId}`, `'id'`)
					ti.query = strings.ReplaceAll(ti.query, `${setClause}`, sqlTableColNames[2]+"='value'")
					ti.query = strings.ReplaceAll(ti.query, `${whereClauseId}`, sqlTableColNames[1]+"='id'")
				}
				if _, err := sqlc.GetDBProxy().Exec(ti.query, ti.params...); err != nil {
					t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
				}
				_sqlcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, sqlc, ti.cmd, MetricsCatAll, ti.metricCat)
			}
		})
	}
}

func TestDBProxy_ExecContext(t *testing.T) {
	testName := "TestDBProxy_ExecContext"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	type testInfo struct {
		cmd, query string
		params     []interface{}
		metricCat  string
	}
	testInfoList := []testInfo{
		{cmd: "SELECT", query: `select * from ${table}`, params: []interface{}{}, metricCat: MetricsCatOther},
		{cmd: "INSERT", query: `insert into ${table} (${colId}) VALUES (${valId})`, params: []interface{}{}, metricCat: MetricsCatDML},
		{cmd: "UPDATE", query: `update ${table} set ${setClause} where ${whereClauseId}`, params: []interface{}{}, metricCat: MetricsCatDML},
		{cmd: "DELETE", query: `delete from ${table} where ${whereClauseId}`, params: []interface{}{}, metricCat: MetricsCatDML},
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if err := sqlInitTable(sqlc, testSqlTableName, false); err != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err)
			}
			for _, ti := range testInfoList {
				ti.query = strings.ReplaceAll(ti.query, `${table}`, testSqlTableName)
				ti.query = strings.ReplaceAll(ti.query, `${colId}`, sqlTableColNames[1])
				if sqlc.GetDbFlavor() == FlavorCosmosDb {
					if ti.cmd == "SELECT" {
						continue
					}
					ti.query = strings.ReplaceAll(ti.query, `${valId}`, `"\"id\""`)
					ti.query = strings.ReplaceAll(ti.query, `${setClause}`, sqlTableColNames[2]+`="\"value\""`)
					ti.query = strings.ReplaceAll(ti.query, `${whereClauseId}`, "id=id")
					ti.params = append(ti.params, "id")
				} else {
					ti.query = strings.ReplaceAll(ti.query, `${valId}`, `'id'`)
					ti.query = strings.ReplaceAll(ti.query, `${setClause}`, sqlTableColNames[2]+"='value'")
					ti.query = strings.ReplaceAll(ti.query, `${whereClauseId}`, sqlTableColNames[1]+"='id'")
				}
				if _, err := sqlc.GetDBProxy().ExecContext(context.TODO(), ti.query, ti.params...); err != nil {
					t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
				}
				_sqlcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, sqlc, ti.cmd, MetricsCatAll, ti.metricCat)
			}
		})
	}
}

func TestDBProxy_Query(t *testing.T) {
	testName := "TestDBProxy_Query"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	type testInfo struct {
		cmd, query string
		params     []interface{}
		metricCat  string
	}
	testInfoList := []testInfo{
		{cmd: "SELECT", query: `select * from ${table}`, params: []interface{}{}, metricCat: MetricsCatDQL},
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if err := sqlInitTable(sqlc, testSqlTableName, false); err != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err)
			}
			for _, ti := range testInfoList {
				ti.query = strings.ReplaceAll(ti.query, `${table}`, testSqlTableName)
				if sqlc.GetDbFlavor() == FlavorCosmosDb {
					if ti.cmd != "SELECT" {
						continue
					}
					ti.query += " WITH cross_partition=true"
				}
				if _, err := sqlc.GetDBProxy().Query(ti.query, ti.params...); err != nil {
					t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
				}
				_sqlcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, sqlc, ti.cmd, MetricsCatAll, ti.metricCat)
			}
		})
	}
}

func TestDBProxy_QueryContext(t *testing.T) {
	testName := "TestDBProxy_QueryContext"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	type testInfo struct {
		cmd, query string
		params     []interface{}
		metricCat  string
	}
	testInfoList := []testInfo{
		{cmd: "SELECT", query: `select * from ${table}`, params: []interface{}{}, metricCat: MetricsCatDQL},
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if err := sqlInitTable(sqlc, testSqlTableName, false); err != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err)
			}
			for _, ti := range testInfoList {
				ti.query = strings.ReplaceAll(ti.query, `${table}`, testSqlTableName)
				if sqlc.GetDbFlavor() == FlavorCosmosDb {
					if ti.cmd != "SELECT" {
						continue
					}
					ti.query += " WITH cross_partition=true"
				}
				if _, err := sqlc.GetDBProxy().QueryContext(context.TODO(), ti.query, ti.params...); err != nil {
					t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
				}
				_sqlcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, sqlc, ti.cmd, MetricsCatAll, ti.metricCat)
			}
		})
	}
}

func TestDBProxy_QueryRow(t *testing.T) {
	testName := "TestDBProxy_QueryRow"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	type testInfo struct {
		cmd, query string
		params     []interface{}
		metricCat  string
	}
	testInfoList := []testInfo{
		{cmd: "SELECT", query: `select * from ${table}`, params: []interface{}{}, metricCat: MetricsCatDQL},
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if err := sqlInitTable(sqlc, testSqlTableName, false); err != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err)
			}
			for _, ti := range testInfoList {
				ti.query = strings.ReplaceAll(ti.query, `${table}`, testSqlTableName)
				if sqlc.GetDbFlavor() == FlavorCosmosDb {
					if ti.cmd != "SELECT" {
						continue
					}
					ti.query += " WITH cross_partition=true"
				}
				if row := sqlc.GetDBProxy().QueryRow(ti.query, ti.params...); row.Err() != nil {
					t.Fatalf("%s failed: %s", testName+"/"+dbtype, row.Err())
				}
				_sqlcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, sqlc, ti.cmd, MetricsCatAll, ti.metricCat)
			}
		})
	}
}

func TestDBProxy_QueryRowContext(t *testing.T) {
	testName := "TestDBProxy_QueryRowContext"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	type testInfo struct {
		cmd, query string
		params     []interface{}
		metricCat  string
	}
	testInfoList := []testInfo{
		{cmd: "SELECT", query: `select * from ${table}`, params: []interface{}{}, metricCat: MetricsCatDQL},
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if err := sqlInitTable(sqlc, testSqlTableName, false); err != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err)
			}
			for _, ti := range testInfoList {
				ti.query = strings.ReplaceAll(ti.query, `${table}`, testSqlTableName)
				if sqlc.GetDbFlavor() == FlavorCosmosDb {
					if ti.cmd != "SELECT" {
						continue
					}
					ti.query += " WITH cross_partition=true"
				}
				if row := sqlc.GetDBProxy().QueryRowContext(context.TODO(), ti.query, ti.params...); row.Err() != nil {
					t.Fatalf("%s failed: %s", testName+"/"+dbtype, row.Err())
				}
				_sqlcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, sqlc, ti.cmd, MetricsCatAll, ti.metricCat)
			}
		})
	}
}

/*---------- ConnProxy ----------*/

func TestConnProxy_PingContext(t *testing.T) {
	testName := "TestConnProxy_PingContext"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			conn, err := sqlc.ConnProxy(nil)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			}
			if err = conn.PingContext(context.TODO()); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			}
			_sqlcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, sqlc, "ping", MetricsCatAll, MetricsCatOther)
		})
	}
}

func TestConnProxy_Close(t *testing.T) {
	testName := "TestConnProxy_Close"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			conn, err := sqlc.ConnProxy(nil)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			}
			if err = conn.Close(); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			}
			_sqlcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, sqlc, "close", MetricsCatAll, MetricsCatOther)
		})
	}
}

func TestConnProxy_PrepareContext(t *testing.T) {
	testName := "TestConnProxy_PrepareContext"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			conn, err := sqlc.ConnProxy(nil)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			}
			if err = sqlInitTable(sqlc, testSqlTableName, false); err != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err)
			}
			if _, err = conn.PrepareContext(context.TODO(), "SELECT * FROM "+testSqlTableName); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			}
			_sqlcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, sqlc, "prepare", MetricsCatAll, MetricsCatOther)
		})
	}
}

func TestConnProxy_ExecContext(t *testing.T) {
	testName := "TestConnProxy_ExecContext"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	type testInfo struct {
		cmd, query string
		params     []interface{}
		metricCat  string
	}
	testInfoList := []testInfo{
		{cmd: "SELECT", query: `select * from ${table}`, params: []interface{}{}, metricCat: MetricsCatOther},
		{cmd: "INSERT", query: `insert into ${table} (${colId}) VALUES (${valId})`, params: []interface{}{}, metricCat: MetricsCatDML},
		{cmd: "UPDATE", query: `update ${table} set ${setClause} where ${whereClauseId}`, params: []interface{}{}, metricCat: MetricsCatDML},
		{cmd: "DELETE", query: `delete from ${table} where ${whereClauseId}`, params: []interface{}{}, metricCat: MetricsCatDML},
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			conn, err := sqlc.ConnProxy(nil)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			}
			if err = sqlInitTable(sqlc, testSqlTableName, false); err != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err)
			}
			for _, ti := range testInfoList {
				ti.query = strings.ReplaceAll(ti.query, `${table}`, testSqlTableName)
				ti.query = strings.ReplaceAll(ti.query, `${colId}`, sqlTableColNames[1])
				if sqlc.GetDbFlavor() == FlavorCosmosDb {
					if ti.cmd == "SELECT" {
						continue
					}
					ti.query = strings.ReplaceAll(ti.query, `${valId}`, `"\"id\""`)
					ti.query = strings.ReplaceAll(ti.query, `${setClause}`, sqlTableColNames[2]+`="\"value\""`)
					ti.query = strings.ReplaceAll(ti.query, `${whereClauseId}`, "id=id")
					ti.params = append(ti.params, "id")
				} else {
					ti.query = strings.ReplaceAll(ti.query, `${valId}`, `'id'`)
					ti.query = strings.ReplaceAll(ti.query, `${setClause}`, sqlTableColNames[2]+"='value'")
					ti.query = strings.ReplaceAll(ti.query, `${whereClauseId}`, sqlTableColNames[1]+"='id'")
				}
				if _, err = conn.ExecContext(context.TODO(), ti.query, ti.params...); err != nil {
					t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
				}
				_sqlcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, sqlc, ti.cmd, MetricsCatAll, ti.metricCat)
			}
		})
	}
}

func TestConnProxy_QueryContext(t *testing.T) {
	testName := "TestConnProxy_QueryContext"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	type testInfo struct {
		cmd, query string
		params     []interface{}
		metricCat  string
	}
	testInfoList := []testInfo{
		{cmd: "SELECT", query: `select * from ${table}`, params: []interface{}{}, metricCat: MetricsCatDQL},
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			conn, err := sqlc.ConnProxy(nil)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			}
			if err = sqlInitTable(sqlc, testSqlTableName, false); err != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err)
			}
			for _, ti := range testInfoList {
				ti.query = strings.ReplaceAll(ti.query, `${table}`, testSqlTableName)
				if sqlc.GetDbFlavor() == FlavorCosmosDb {
					if ti.cmd != "SELECT" {
						continue
					}
					ti.query += " WITH cross_partition=true"
				}
				if _, err = conn.QueryContext(context.TODO(), ti.query, ti.params...); err != nil {
					t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
				}
				_sqlcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, sqlc, ti.cmd, MetricsCatAll, ti.metricCat)
			}
		})
	}
}

func TestConnProxy_QueryRowContext(t *testing.T) {
	testName := "TestConnProxy_QueryRowContext"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	type testInfo struct {
		cmd, query string
		params     []interface{}
		metricCat  string
	}
	testInfoList := []testInfo{
		{cmd: "SELECT", query: `select * from ${table}`, params: []interface{}{}, metricCat: MetricsCatDQL},
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			conn, err := sqlc.ConnProxy(nil)
			if err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			}
			if err = sqlInitTable(sqlc, testSqlTableName, false); err != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err)
			}
			for _, ti := range testInfoList {
				ti.query = strings.ReplaceAll(ti.query, `${table}`, testSqlTableName)
				if sqlc.GetDbFlavor() == FlavorCosmosDb {
					if ti.cmd != "SELECT" {
						continue
					}
					ti.query += " WITH cross_partition=true"
				}
				if row := conn.QueryRowContext(context.TODO(), ti.query, ti.params...); row.Err() != nil {
					t.Fatalf("%s failed: %s", testName+"/"+dbtype, row.Err())
				}
				_sqlcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, sqlc, ti.cmd, MetricsCatAll, ti.metricCat)
			}
		})
	}
}

/*---------- TxProxy ----------*/

func _testTxProxy_Commit(testName, dbtype string, sqlc *SqlConnect, tx *TxProxy, f _testFailedWithMsgFunc) {
	if err := tx.Commit(); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	}
	_sqlcVerifyLastCommand(f, testName, sqlc, "commit", MetricsCatAll, MetricsCatOther)
}

func _testTxProxy_DBBegin_Commit(testName, dbtype string, sqlc *SqlConnect, db *DBProxy, f _testFailedWithMsgFunc) {
	if tx, err := db.BeginProxy(); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		_testTxProxy_Commit(testName, dbtype, sqlc, tx, f)
	}
}

func _testTxProxy_DBBeginTx_Commit(testName, dbtype string, sqlc *SqlConnect, db *DBProxy, f _testFailedWithMsgFunc) {
	if tx, err := db.BeginTxProxy(context.TODO(), nil); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		_testTxProxy_Commit(testName, dbtype, sqlc, tx, f)
	}
}

func _testTxProxy_ConnBeginTx_Commit(testName, dbtype string, sqlc *SqlConnect, conn *ConnProxy, f _testFailedWithMsgFunc) {
	if tx, err := conn.BeginTxProxy(context.TODO(), nil); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		_testTxProxy_Commit(testName, dbtype, sqlc, tx, f)
	}
}

func TestTxProxy_DB_Commit(t *testing.T) {
	testName := "TestTxProxy_DB_Commit"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if sqlc.GetDbFlavor() == FlavorCosmosDb {
				t.SkipNow()
			}
			_testTxProxy_DBBegin_Commit(testName, dbtype, sqlc, sqlc.GetDBProxy(), func(msg string) { t.Fatalf(msg) })
			_testTxProxy_DBBeginTx_Commit(testName, dbtype, sqlc, sqlc.GetDBProxy(), func(msg string) { t.Fatalf(msg) })
		})
	}
}

func TestTxProxy_Conn_Commit(t *testing.T) {
	testName := "TestTxProxy_Conn_Commit"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if sqlc.GetDbFlavor() == FlavorCosmosDb {
				t.SkipNow()
			}
			if conn, err := sqlc.ConnProxy(nil); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			} else {
				defer conn.Close()
				_testTxProxy_ConnBeginTx_Commit(testName, dbtype, sqlc, conn, func(msg string) { t.Fatalf(msg) })
				conn.Close()
			}
			if conn, err := sqlc.GetDBProxy().ConnProxy(context.TODO()); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			} else {
				defer conn.Close()
				_testTxProxy_ConnBeginTx_Commit(testName, dbtype, sqlc, conn, func(msg string) { t.Fatalf(msg) })
				conn.Close()
			}
		})
	}
}

func _testTxProxy_Rollback(testName, dbtype string, sqlc *SqlConnect, tx *TxProxy, f _testFailedWithMsgFunc) {
	if err := tx.Rollback(); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	}
	_sqlcVerifyLastCommand(f, testName, sqlc, "rollback", MetricsCatAll, MetricsCatOther)
}

func _testTxProxy_DBBegin_Rollback(testName, dbtype string, sqlc *SqlConnect, db *DBProxy, f _testFailedWithMsgFunc) {
	if tx, err := db.BeginProxy(); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		_testTxProxy_Rollback(testName, dbtype, sqlc, tx, f)
	}
}

func _testTxProxy_DBBeginTx_Rollback(testName, dbtype string, sqlc *SqlConnect, db *DBProxy, f _testFailedWithMsgFunc) {
	if tx, err := db.BeginTxProxy(context.TODO(), nil); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		_testTxProxy_Rollback(testName, dbtype, sqlc, tx, f)
	}
}

func _testTxProxy_ConnBeginTx_Rollback(testName, dbtype string, sqlc *SqlConnect, conn *ConnProxy, f _testFailedWithMsgFunc) {
	if tx, err := conn.BeginTxProxy(context.TODO(), nil); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		_testTxProxy_Rollback(testName, dbtype, sqlc, tx, f)
	}
}

func TestTxProxy_DB_Rollback(t *testing.T) {
	testName := "TestTxProxy_DB_Rollback"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if sqlc.GetDbFlavor() == FlavorCosmosDb {
				t.SkipNow()
			}
			_testTxProxy_DBBegin_Rollback(testName, dbtype, sqlc, sqlc.GetDBProxy(), func(msg string) { t.Fatalf(msg) })
			_testTxProxy_DBBeginTx_Rollback(testName, dbtype, sqlc, sqlc.GetDBProxy(), func(msg string) { t.Fatalf(msg) })
		})
	}
}

func TestTxProxy_Conn_Rollback(t *testing.T) {
	testName := "TestTxProxy_Conn_Rollback"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if sqlc.GetDbFlavor() == FlavorCosmosDb {
				t.SkipNow()
			}
			if conn, err := sqlc.ConnProxy(nil); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			} else {
				defer conn.Close()
				_testTxProxy_ConnBeginTx_Rollback(testName, dbtype, sqlc, conn, func(msg string) { t.Fatalf(msg) })
				conn.Close()
			}
			if conn, err := sqlc.GetDBProxy().ConnProxy(context.TODO()); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			} else {
				defer conn.Close()
				_testTxProxy_ConnBeginTx_Rollback(testName, dbtype, sqlc, conn, func(msg string) { t.Fatalf(msg) })
				conn.Close()
			}
		})
	}
}

func _testTxProxy_Prepare(testName, dbtype string, sqlc *SqlConnect, tx *TxProxy, f _testFailedWithMsgFunc) {
	if err := sqlInitTable(sqlc, testSqlTableName, false); err != nil {
		f(fmt.Sprintf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err))
	}
	if _, err := tx.Prepare("SELECT * FROM " + testSqlTableName); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	}
	_sqlcVerifyLastCommand(f, testName, sqlc, "prepare", MetricsCatAll, MetricsCatOther)
}

func _testTxProxy_DBBegin_Prepare(testName, dbtype string, sqlc *SqlConnect, db *DBProxy, f _testFailedWithMsgFunc) {
	if tx, err := db.BeginProxy(); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_Prepare(testName, dbtype, sqlc, tx, f)
	}
}

func _testTxProxy_DBBeginTx_Prepare(testName, dbtype string, sqlc *SqlConnect, db *DBProxy, f _testFailedWithMsgFunc) {
	if tx, err := db.BeginTxProxy(context.TODO(), nil); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_Prepare(testName, dbtype, sqlc, tx, f)
	}
}

func _testTxProxy_ConnBeginTx_Prepare(testName, dbtype string, sqlc *SqlConnect, conn *ConnProxy, f _testFailedWithMsgFunc) {
	if tx, err := conn.BeginTxProxy(context.TODO(), nil); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_Prepare(testName, dbtype, sqlc, tx, f)
	}
}

func TestTxProxy_DB_Prepare(t *testing.T) {
	testName := "TestTxProxy_DB_Prepare"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if sqlc.GetDbFlavor() == FlavorCosmosDb {
				t.SkipNow()
			}
			_testTxProxy_DBBegin_Prepare(testName, dbtype, sqlc, sqlc.GetDBProxy(), func(msg string) { t.Fatalf(msg) })
			_testTxProxy_DBBeginTx_Prepare(testName, dbtype, sqlc, sqlc.GetDBProxy(), func(msg string) { t.Fatalf(msg) })
		})
	}
}

func TestTxProxy_Conn_Prepare(t *testing.T) {
	testName := "TestTxProxy_Conn_Prepare"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if sqlc.GetDbFlavor() == FlavorCosmosDb {
				t.SkipNow()
			}
			if conn, err := sqlc.ConnProxy(nil); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			} else {
				defer conn.Close()
				_testTxProxy_ConnBeginTx_Prepare(testName, dbtype, sqlc, conn, func(msg string) { t.Fatalf(msg) })
				conn.Close()
			}
			if conn, err := sqlc.GetDBProxy().ConnProxy(context.TODO()); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			} else {
				defer conn.Close()
				_testTxProxy_ConnBeginTx_Prepare(testName, dbtype, sqlc, conn, func(msg string) { t.Fatalf(msg) })
				conn.Close()
			}
		})
	}
}

func _testTxProxy_PrepareContext(testName, dbtype string, sqlc *SqlConnect, tx *TxProxy, f _testFailedWithMsgFunc) {
	if err := sqlInitTable(sqlc, testSqlTableName, false); err != nil {
		f(fmt.Sprintf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err))
	}
	if _, err := tx.PrepareContext(context.TODO(), "SELECT * FROM "+testSqlTableName); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	}
	_sqlcVerifyLastCommand(f, testName, sqlc, "prepare", MetricsCatAll, MetricsCatOther)
}

func _testTxProxy_DBBegin_PrepareContext(testName, dbtype string, sqlc *SqlConnect, db *DBProxy, f _testFailedWithMsgFunc) {
	if tx, err := db.BeginProxy(); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_PrepareContext(testName, dbtype, sqlc, tx, f)
	}
}

func _testTxProxy_DBBeginTx_PrepareContext(testName, dbtype string, sqlc *SqlConnect, db *DBProxy, f _testFailedWithMsgFunc) {
	if tx, err := db.BeginTxProxy(context.TODO(), nil); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_PrepareContext(testName, dbtype, sqlc, tx, f)
	}
}

func _testTxProxy_ConnBeginTx_PrepareContext(testName, dbtype string, sqlc *SqlConnect, conn *ConnProxy, f _testFailedWithMsgFunc) {
	if tx, err := conn.BeginTxProxy(context.TODO(), nil); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_PrepareContext(testName, dbtype, sqlc, tx, f)
	}
}

func TestTxProxy_DB_PrepareContext(t *testing.T) {
	testName := "TestTxProxy_DB_PrepareContext"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if sqlc.GetDbFlavor() == FlavorCosmosDb {
				t.SkipNow()
			}
			_testTxProxy_DBBegin_PrepareContext(testName, dbtype, sqlc, sqlc.GetDBProxy(), func(msg string) { t.Fatalf(msg) })
			_testTxProxy_DBBeginTx_PrepareContext(testName, dbtype, sqlc, sqlc.GetDBProxy(), func(msg string) { t.Fatalf(msg) })
		})
	}
}

func TestTxProxy_Conn_PrepareContext(t *testing.T) {
	testName := "TestTxProxy_Conn_PrepareContext"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if sqlc.GetDbFlavor() == FlavorCosmosDb {
				t.SkipNow()
			}
			if conn, err := sqlc.ConnProxy(nil); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			} else {
				defer conn.Close()
				_testTxProxy_ConnBeginTx_PrepareContext(testName, dbtype, sqlc, conn, func(msg string) { t.Fatalf(msg) })
				conn.Close()
			}
			if conn, err := sqlc.GetDBProxy().ConnProxy(context.TODO()); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			} else {
				defer conn.Close()
				_testTxProxy_ConnBeginTx_PrepareContext(testName, dbtype, sqlc, conn, func(msg string) { t.Fatalf(msg) })
				conn.Close()
			}
		})
	}
}

func _testTxProxy_Exec(testName, dbtype string, sqlc *SqlConnect, tx *TxProxy, f _testFailedWithMsgFunc) {
	type testInfo struct {
		cmd, query string
		params     []interface{}
		metricCat  string
	}
	testInfoList := []testInfo{
		{cmd: "SELECT", query: `select * from ${table}`, params: []interface{}{}, metricCat: MetricsCatOther},
		{cmd: "INSERT", query: `insert into ${table} (${colId}) VALUES (${valId})`, params: []interface{}{}, metricCat: MetricsCatDML},
		{cmd: "UPDATE", query: `update ${table} set ${setClause} where ${whereClauseId}`, params: []interface{}{}, metricCat: MetricsCatDML},
		{cmd: "DELETE", query: `delete from ${table} where ${whereClauseId}`, params: []interface{}{}, metricCat: MetricsCatDML},
	}
	if err := sqlInitTable(sqlc, testSqlTableName, false); err != nil {
		f(fmt.Sprintf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err))
	}
	for _, ti := range testInfoList {
		ti.query = strings.ReplaceAll(ti.query, `${table}`, testSqlTableName)
		ti.query = strings.ReplaceAll(ti.query, `${colId}`, sqlTableColNames[1])
		if sqlc.GetDbFlavor() == FlavorCosmosDb {
			if ti.cmd == "SELECT" {
				continue
			}
			ti.query = strings.ReplaceAll(ti.query, `${valId}`, `"\"id\""`)
			ti.query = strings.ReplaceAll(ti.query, `${setClause}`, sqlTableColNames[2]+`="\"value\""`)
			ti.query = strings.ReplaceAll(ti.query, `${whereClauseId}`, "id=id")
			ti.params = append(ti.params, "id")
		} else {
			ti.query = strings.ReplaceAll(ti.query, `${valId}`, `'id'`)
			ti.query = strings.ReplaceAll(ti.query, `${setClause}`, sqlTableColNames[2]+"='value'")
			ti.query = strings.ReplaceAll(ti.query, `${whereClauseId}`, sqlTableColNames[1]+"='id'")
		}
		if _, err := tx.Exec(ti.query, ti.params...); err != nil {
			f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
		}
		_sqlcVerifyLastCommand(f, testName, sqlc, ti.cmd, MetricsCatAll, ti.metricCat)
	}
}

func _testTxProxy_DBBegin_Exec(testName, dbtype string, sqlc *SqlConnect, db *DBProxy, f _testFailedWithMsgFunc) {
	if tx, err := db.BeginProxy(); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_Exec(testName, dbtype, sqlc, tx, f)
	}
}

func _testTxProxy_DBBeginTx_Exec(testName, dbtype string, sqlc *SqlConnect, db *DBProxy, f _testFailedWithMsgFunc) {
	if tx, err := db.BeginTxProxy(context.TODO(), nil); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_Exec(testName, dbtype, sqlc, tx, f)
	}
}

func _testTxProxy_ConnBeginTx_Exec(testName, dbtype string, sqlc *SqlConnect, conn *ConnProxy, f _testFailedWithMsgFunc) {
	if tx, err := conn.BeginTxProxy(context.TODO(), nil); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_Exec(testName, dbtype, sqlc, tx, f)
	}
}

func TestTxProxy_DB_Exec(t *testing.T) {
	testName := "TestTxProxy_DB_Exec"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if sqlc.GetDbFlavor() == FlavorCosmosDb {
				t.SkipNow()
			}
			_testTxProxy_DBBegin_Exec(testName, dbtype, sqlc, sqlc.GetDBProxy(), func(msg string) { t.Fatalf(msg) })
			_testTxProxy_DBBeginTx_Exec(testName, dbtype, sqlc, sqlc.GetDBProxy(), func(msg string) { t.Fatalf(msg) })
		})
	}
}

func TestTxProxy_Conn_Exec(t *testing.T) {
	testName := "TestTxProxy_Conn_Exec"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if sqlc.GetDbFlavor() == FlavorCosmosDb {
				t.SkipNow()
			}
			if conn, err := sqlc.ConnProxy(nil); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			} else {
				defer conn.Close()
				_testTxProxy_ConnBeginTx_Exec(testName, dbtype, sqlc, conn, func(msg string) { t.Fatalf(msg) })
				conn.Close()
			}
			if conn, err := sqlc.GetDBProxy().ConnProxy(context.TODO()); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			} else {
				defer conn.Close()
				_testTxProxy_ConnBeginTx_Exec(testName, dbtype, sqlc, conn, func(msg string) { t.Fatalf(msg) })
				conn.Close()
			}
		})
	}
}

func _testTxProxy_ExecContext(testName, dbtype string, sqlc *SqlConnect, tx *TxProxy, f _testFailedWithMsgFunc) {
	type testInfo struct {
		cmd, query string
		params     []interface{}
		metricCat  string
	}
	testInfoList := []testInfo{
		{cmd: "SELECT", query: `select * from ${table}`, params: []interface{}{}, metricCat: MetricsCatOther},
		{cmd: "INSERT", query: `insert into ${table} (${colId}) VALUES (${valId})`, params: []interface{}{}, metricCat: MetricsCatDML},
		{cmd: "UPDATE", query: `update ${table} set ${setClause} where ${whereClauseId}`, params: []interface{}{}, metricCat: MetricsCatDML},
		{cmd: "DELETE", query: `delete from ${table} where ${whereClauseId}`, params: []interface{}{}, metricCat: MetricsCatDML},
	}
	if err := sqlInitTable(sqlc, testSqlTableName, false); err != nil {
		f(fmt.Sprintf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err))
	}
	for _, ti := range testInfoList {
		ti.query = strings.ReplaceAll(ti.query, `${table}`, testSqlTableName)
		ti.query = strings.ReplaceAll(ti.query, `${colId}`, sqlTableColNames[1])
		if sqlc.GetDbFlavor() == FlavorCosmosDb {
			if ti.cmd == "SELECT" {
				continue
			}
			ti.query = strings.ReplaceAll(ti.query, `${valId}`, `"\"id\""`)
			ti.query = strings.ReplaceAll(ti.query, `${setClause}`, sqlTableColNames[2]+`="\"value\""`)
			ti.query = strings.ReplaceAll(ti.query, `${whereClauseId}`, "id=id")
			ti.params = append(ti.params, "id")
		} else {
			ti.query = strings.ReplaceAll(ti.query, `${valId}`, `'id'`)
			ti.query = strings.ReplaceAll(ti.query, `${setClause}`, sqlTableColNames[2]+"='value'")
			ti.query = strings.ReplaceAll(ti.query, `${whereClauseId}`, sqlTableColNames[1]+"='id'")
		}
		if _, err := tx.ExecContext(context.TODO(), ti.query, ti.params...); err != nil {
			f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
		}
		_sqlcVerifyLastCommand(f, testName, sqlc, ti.cmd, MetricsCatAll, ti.metricCat)
	}
}

func _testTxProxy_DBBegin_ExecContext(testName, dbtype string, sqlc *SqlConnect, db *DBProxy, f _testFailedWithMsgFunc) {
	if tx, err := db.BeginProxy(); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_ExecContext(testName, dbtype, sqlc, tx, f)
	}
}

func _testTxProxy_DBBeginTx_ExecContext(testName, dbtype string, sqlc *SqlConnect, db *DBProxy, f _testFailedWithMsgFunc) {
	if tx, err := db.BeginTxProxy(context.TODO(), nil); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_ExecContext(testName, dbtype, sqlc, tx, f)
	}
}

func _testTxProxy_ConnBeginTx_ExecContext(testName, dbtype string, sqlc *SqlConnect, conn *ConnProxy, f _testFailedWithMsgFunc) {
	if tx, err := conn.BeginTxProxy(context.TODO(), nil); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_ExecContext(testName, dbtype, sqlc, tx, f)
	}
}

func TestTxProxy_DB_ExecContext(t *testing.T) {
	testName := "TestTxProxy_DB_ExecContext"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if sqlc.GetDbFlavor() == FlavorCosmosDb {
				t.SkipNow()
			}
			_testTxProxy_DBBegin_ExecContext(testName, dbtype, sqlc, sqlc.GetDBProxy(), func(msg string) { t.Fatalf(msg) })
			_testTxProxy_DBBeginTx_ExecContext(testName, dbtype, sqlc, sqlc.GetDBProxy(), func(msg string) { t.Fatalf(msg) })
		})
	}
}

func TestTxProxy_Conn_ExecContext(t *testing.T) {
	testName := "TestTxProxy_Conn_ExecContext"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if sqlc.GetDbFlavor() == FlavorCosmosDb {
				t.SkipNow()
			}
			if conn, err := sqlc.ConnProxy(nil); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			} else {
				defer conn.Close()
				_testTxProxy_ConnBeginTx_ExecContext(testName, dbtype, sqlc, conn, func(msg string) { t.Fatalf(msg) })
				conn.Close()
			}
			if conn, err := sqlc.GetDBProxy().ConnProxy(context.TODO()); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			} else {
				defer conn.Close()
				_testTxProxy_ConnBeginTx_ExecContext(testName, dbtype, sqlc, conn, func(msg string) { t.Fatalf(msg) })
				conn.Close()
			}
		})
	}
}

func _testTxProxy_Query(testName, dbtype string, sqlc *SqlConnect, tx *TxProxy, f _testFailedWithMsgFunc) {
	type testInfo struct {
		cmd, query string
		params     []interface{}
		metricCat  string
	}
	testInfoList := []testInfo{
		{cmd: "SELECT", query: `select * from ${table}`, params: []interface{}{}, metricCat: MetricsCatDQL},
	}
	if err := sqlInitTable(sqlc, testSqlTableName, false); err != nil {
		f(fmt.Sprintf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err))
	}
	for _, ti := range testInfoList {
		ti.query = strings.ReplaceAll(ti.query, `${table}`, testSqlTableName)
		if sqlc.GetDbFlavor() == FlavorCosmosDb {
			if ti.cmd != "SELECT" {
				continue
			}
			ti.query += " WITH cross_partition=true"
		}
		if _, err := tx.Query(ti.query, ti.params...); err != nil {
			f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
		}
		_sqlcVerifyLastCommand(f, testName, sqlc, ti.cmd, MetricsCatAll, ti.metricCat)
	}
}

func _testTxProxy_DBBegin_Query(testName, dbtype string, sqlc *SqlConnect, db *DBProxy, f _testFailedWithMsgFunc) {
	if tx, err := db.BeginProxy(); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_Query(testName, dbtype, sqlc, tx, f)
	}
}

func _testTxProxy_DBBeginTx_Query(testName, dbtype string, sqlc *SqlConnect, db *DBProxy, f _testFailedWithMsgFunc) {
	if tx, err := db.BeginTxProxy(context.TODO(), nil); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_Query(testName, dbtype, sqlc, tx, f)
	}
}

func _testTxProxy_ConnBeginTx_Query(testName, dbtype string, sqlc *SqlConnect, conn *ConnProxy, f _testFailedWithMsgFunc) {
	if tx, err := conn.BeginTxProxy(context.TODO(), nil); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_Query(testName, dbtype, sqlc, tx, f)
	}
}

func TestTxProxy_DB_Query(t *testing.T) {
	testName := "TestTxProxy_DB_Query"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if sqlc.GetDbFlavor() == FlavorCosmosDb {
				t.SkipNow()
			}
			_testTxProxy_DBBegin_Query(testName, dbtype, sqlc, sqlc.GetDBProxy(), func(msg string) { t.Fatalf(msg) })
			_testTxProxy_DBBeginTx_Query(testName, dbtype, sqlc, sqlc.GetDBProxy(), func(msg string) { t.Fatalf(msg) })
		})
	}
}

func TestTxProxy_Conn_Query(t *testing.T) {
	testName := "TestTxProxy_Conn_Query"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if sqlc.GetDbFlavor() == FlavorCosmosDb {
				t.SkipNow()
			}
			if conn, err := sqlc.ConnProxy(nil); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			} else {
				defer conn.Close()
				_testTxProxy_ConnBeginTx_Query(testName, dbtype, sqlc, conn, func(msg string) { t.Fatalf(msg) })
				conn.Close()
			}
			if conn, err := sqlc.GetDBProxy().ConnProxy(context.TODO()); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			} else {
				defer conn.Close()
				_testTxProxy_ConnBeginTx_Query(testName, dbtype, sqlc, conn, func(msg string) { t.Fatalf(msg) })
				conn.Close()
			}
		})
	}
}

func _testTxProxy_QueryContext(testName, dbtype string, sqlc *SqlConnect, tx *TxProxy, f _testFailedWithMsgFunc) {
	type testInfo struct {
		cmd, query string
		params     []interface{}
		metricCat  string
	}
	testInfoList := []testInfo{
		{cmd: "SELECT", query: `select * from ${table}`, params: []interface{}{}, metricCat: MetricsCatDQL},
	}
	if err := sqlInitTable(sqlc, testSqlTableName, false); err != nil {
		f(fmt.Sprintf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err))
	}
	for _, ti := range testInfoList {
		ti.query = strings.ReplaceAll(ti.query, `${table}`, testSqlTableName)
		if sqlc.GetDbFlavor() == FlavorCosmosDb {
			if ti.cmd != "SELECT" {
				continue
			}
			ti.query += " WITH cross_partition=true"
		}
		if _, err := tx.QueryContext(context.TODO(), ti.query, ti.params...); err != nil {
			f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
		}
		_sqlcVerifyLastCommand(f, testName, sqlc, ti.cmd, MetricsCatAll, ti.metricCat)
	}
}

func _testTxProxy_DBBegin_QueryContext(testName, dbtype string, sqlc *SqlConnect, db *DBProxy, f _testFailedWithMsgFunc) {
	if tx, err := db.BeginProxy(); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_QueryContext(testName, dbtype, sqlc, tx, f)
	}
}

func _testTxProxy_DBBeginTx_QueryContext(testName, dbtype string, sqlc *SqlConnect, db *DBProxy, f _testFailedWithMsgFunc) {
	if tx, err := db.BeginTxProxy(context.TODO(), nil); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_QueryContext(testName, dbtype, sqlc, tx, f)
	}
}

func _testTxProxy_ConnBeginTx_QueryContext(testName, dbtype string, sqlc *SqlConnect, conn *ConnProxy, f _testFailedWithMsgFunc) {
	if tx, err := conn.BeginTxProxy(context.TODO(), nil); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_QueryContext(testName, dbtype, sqlc, tx, f)
	}
}

func TestTxProxy_DB_QueryContext(t *testing.T) {
	testName := "TestTxProxy_DB_QueryContext"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if sqlc.GetDbFlavor() == FlavorCosmosDb {
				t.SkipNow()
			}
			_testTxProxy_DBBegin_QueryContext(testName, dbtype, sqlc, sqlc.GetDBProxy(), func(msg string) { t.Fatalf(msg) })
			_testTxProxy_DBBeginTx_QueryContext(testName, dbtype, sqlc, sqlc.GetDBProxy(), func(msg string) { t.Fatalf(msg) })
		})
	}
}

func TestTxProxy_Conn_QueryContext(t *testing.T) {
	testName := "TestTxProxy_Conn_QueryContext"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if sqlc.GetDbFlavor() == FlavorCosmosDb {
				t.SkipNow()
			}
			if conn, err := sqlc.ConnProxy(nil); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			} else {
				defer conn.Close()
				_testTxProxy_ConnBeginTx_QueryContext(testName, dbtype, sqlc, conn, func(msg string) { t.Fatalf(msg) })
				conn.Close()
			}
			if conn, err := sqlc.GetDBProxy().ConnProxy(context.TODO()); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			} else {
				defer conn.Close()
				_testTxProxy_ConnBeginTx_QueryContext(testName, dbtype, sqlc, conn, func(msg string) { t.Fatalf(msg) })
				conn.Close()
			}
		})
	}
}

func _testTxProxy_QueryRow(testName, dbtype string, sqlc *SqlConnect, tx *TxProxy, f _testFailedWithMsgFunc) {
	type testInfo struct {
		cmd, query string
		params     []interface{}
		metricCat  string
	}
	testInfoList := []testInfo{
		{cmd: "SELECT", query: `select * from ${table}`, params: []interface{}{}, metricCat: MetricsCatDQL},
	}
	if err := sqlInitTable(sqlc, testSqlTableName, false); err != nil {
		f(fmt.Sprintf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err))
	}
	for _, ti := range testInfoList {
		ti.query = strings.ReplaceAll(ti.query, `${table}`, testSqlTableName)
		if sqlc.GetDbFlavor() == FlavorCosmosDb {
			if ti.cmd != "SELECT" {
				continue
			}
			ti.query += " WITH cross_partition=true"
		}
		if row := tx.QueryRow(ti.query, ti.params...); row.Err() != nil {
			f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, row.Err()))
		}
		_sqlcVerifyLastCommand(f, testName, sqlc, ti.cmd, MetricsCatAll, ti.metricCat)
	}
}

func _testTxProxy_DBBegin_QueryRow(testName, dbtype string, sqlc *SqlConnect, db *DBProxy, f _testFailedWithMsgFunc) {
	if tx, err := db.BeginProxy(); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_QueryRow(testName, dbtype, sqlc, tx, f)
	}
}

func _testTxProxy_DBBeginTx_QueryRow(testName, dbtype string, sqlc *SqlConnect, db *DBProxy, f _testFailedWithMsgFunc) {
	if tx, err := db.BeginTxProxy(context.TODO(), nil); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_QueryRow(testName, dbtype, sqlc, tx, f)
	}
}

func _testTxProxy_ConnBeginTx_QueryRow(testName, dbtype string, sqlc *SqlConnect, conn *ConnProxy, f _testFailedWithMsgFunc) {
	if tx, err := conn.BeginTxProxy(context.TODO(), nil); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_QueryRow(testName, dbtype, sqlc, tx, f)
	}
}

func TestTxProxy_DB_QueryRow(t *testing.T) {
	testName := "TestTxProxy_DB_QueryRow"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if sqlc.GetDbFlavor() == FlavorCosmosDb {
				t.SkipNow()
			}
			_testTxProxy_DBBegin_QueryRow(testName, dbtype, sqlc, sqlc.GetDBProxy(), func(msg string) { t.Fatalf(msg) })
			_testTxProxy_DBBeginTx_QueryRow(testName, dbtype, sqlc, sqlc.GetDBProxy(), func(msg string) { t.Fatalf(msg) })
		})
	}
}

func TestTxProxy_Conn_QueryRow(t *testing.T) {
	testName := "TestTxProxy_Conn_QueryRow"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if sqlc.GetDbFlavor() == FlavorCosmosDb {
				t.SkipNow()
			}
			if conn, err := sqlc.ConnProxy(nil); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			} else {
				defer conn.Close()
				_testTxProxy_ConnBeginTx_QueryRow(testName, dbtype, sqlc, conn, func(msg string) { t.Fatalf(msg) })
				conn.Close()
			}
			if conn, err := sqlc.GetDBProxy().ConnProxy(context.TODO()); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			} else {
				defer conn.Close()
				_testTxProxy_ConnBeginTx_QueryRow(testName, dbtype, sqlc, conn, func(msg string) { t.Fatalf(msg) })
				conn.Close()
			}
		})
	}
}

func _testTxProxy_QueryRowContext(testName, dbtype string, sqlc *SqlConnect, tx *TxProxy, f _testFailedWithMsgFunc) {
	type testInfo struct {
		cmd, query string
		params     []interface{}
		metricCat  string
	}
	testInfoList := []testInfo{
		{cmd: "SELECT", query: `select * from ${table}`, params: []interface{}{}, metricCat: MetricsCatDQL},
	}
	if err := sqlInitTable(sqlc, testSqlTableName, false); err != nil {
		f(fmt.Sprintf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err))
	}
	for _, ti := range testInfoList {
		ti.query = strings.ReplaceAll(ti.query, `${table}`, testSqlTableName)
		if sqlc.GetDbFlavor() == FlavorCosmosDb {
			if ti.cmd != "SELECT" {
				continue
			}
			ti.query += " WITH cross_partition=true"
		}
		if row := tx.QueryRowContext(context.TODO(), ti.query, ti.params...); row.Err() != nil {
			f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, row.Err()))
		}
		_sqlcVerifyLastCommand(f, testName, sqlc, ti.cmd, MetricsCatAll, ti.metricCat)
	}
}

func _testTxProxy_DBBegin_QueryRowContext(testName, dbtype string, sqlc *SqlConnect, db *DBProxy, f _testFailedWithMsgFunc) {
	if tx, err := db.BeginProxy(); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_QueryRowContext(testName, dbtype, sqlc, tx, f)
	}
}

func _testTxProxy_DBBeginTx_QueryRowContext(testName, dbtype string, sqlc *SqlConnect, db *DBProxy, f _testFailedWithMsgFunc) {
	if tx, err := db.BeginTxProxy(context.TODO(), nil); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_QueryRowContext(testName, dbtype, sqlc, tx, f)
	}
}

func _testTxProxy_ConnBeginTx_QueryRowContext(testName, dbtype string, sqlc *SqlConnect, conn *ConnProxy, f _testFailedWithMsgFunc) {
	if tx, err := conn.BeginTxProxy(context.TODO(), nil); err != nil {
		f(fmt.Sprintf("%s failed: %s", testName+"/"+dbtype, err))
	} else {
		defer tx.Commit()
		_testTxProxy_QueryRowContext(testName, dbtype, sqlc, tx, f)
	}
}

func TestTxProxy_DB_QueryRowContext(t *testing.T) {
	testName := "TestTxProxy_DB_QueryRowContext"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if sqlc.GetDbFlavor() == FlavorCosmosDb {
				t.SkipNow()
			}
			_testTxProxy_DBBegin_QueryRowContext(testName, dbtype, sqlc, sqlc.GetDBProxy(), func(msg string) { t.Fatalf(msg) })
			_testTxProxy_DBBeginTx_QueryRowContext(testName, dbtype, sqlc, sqlc.GetDBProxy(), func(msg string) { t.Fatalf(msg) })
		})
	}
}

func TestTxProxy_Conn_QueryRowContext(t *testing.T) {
	testName := "TestTxProxy_Conn_QueryRowContext"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if sqlc.GetDbFlavor() == FlavorCosmosDb {
				t.SkipNow()
			}
			if conn, err := sqlc.ConnProxy(nil); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			} else {
				defer conn.Close()
				_testTxProxy_ConnBeginTx_QueryRowContext(testName, dbtype, sqlc, conn, func(msg string) { t.Fatalf(msg) })
				conn.Close()
			}
			if conn, err := sqlc.GetDBProxy().ConnProxy(context.TODO()); err != nil {
				t.Fatalf("%s failed: %s", testName+"/"+dbtype, err)
			} else {
				defer conn.Close()
				_testTxProxy_ConnBeginTx_QueryRowContext(testName, dbtype, sqlc, conn, func(msg string) { t.Fatalf(msg) })
				conn.Close()
			}
		})
	}
}
