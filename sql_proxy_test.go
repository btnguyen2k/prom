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
