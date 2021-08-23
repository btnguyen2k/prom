package prom

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"
)

var sqlColNamesTestDataTypeMoney = []string{"id",
	"data_money2", "data_money4", "data_money6", "data_money8"}

func _testSqlDataTypeMoney(t *testing.T, name string, sqlc *SqlConnect, colTypes []string) {
	tblName := "tbl_test"
	rand.Seed(time.Now().UnixNano())

	colNameList := sqlColNamesTestDataTypeMoney

	// init
	sqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE %s", tblName))
	if sqlc.flavor == FlavorCosmosDb {
		if _, err := sqlc.GetDB().Exec(fmt.Sprintf("CREATE COLLECTION %s WITH pk=/%s", tblName, colNameList[0])); err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
	} else {
		sql := fmt.Sprintf("CREATE TABLE %s (", tblName)
		for i := range colNameList {
			sql += colNameList[i] + " " + colTypes[i] + ","
		}
		sql += fmt.Sprintf("PRIMARY KEY(%s))", colNameList[0])
		if _, err := sqlc.GetDB().Exec(sql); err != nil {
			t.Fatalf("%s failed: %s\n%s", name, err, sql)
		}
	}

	type Row struct {
		id         string
		dataMoney2 float64
		dataMoney4 float64
		dataMoney6 float64
		dataMoney8 float64
	}
	rowArr := make([]Row, 0)
	numRows := 100

	// insert some rows
	sql := fmt.Sprintf("INSERT INTO %s (", tblName)
	sql += strings.Join(colNameList, ",")
	sql += ") VALUES ("
	sql += _generatePlaceholders(len(colNameList), sqlc) + ")"
	for i := 1; i <= numRows; i++ {
		vMoneySmall := float64(rand.Intn(65536)) + rand.Float64()
		vMoneyLarge := float64(rand.Int31()) + rand.Float64()

		row := Row{
			id:         fmt.Sprintf("%03d", i),
			dataMoney2: math.Round(vMoneySmall*1e2) / 1e2,
			dataMoney4: math.Round(vMoneySmall*1e4) / 1e4,
			dataMoney6: math.Round(vMoneyLarge*1e6) / 1e6,
			dataMoney8: math.Round(vMoneyLarge*1e8) / 1e8,
		}
		rowArr = append(rowArr, row)
		params := []interface{}{row.id, row.dataMoney2, row.dataMoney4, row.dataMoney6, row.dataMoney8}
		if sqlc.flavor == FlavorCosmosDb {
			params = append(params, row.id)
		}
		_, err := sqlc.GetDB().Exec(sql, params...)
		if err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
	}

	// query some rows
	sql = "SELECT * FROM %s ORDER BY id"
	if sqlc.flavor == FlavorCosmosDb {
		sql = "SELECT * FROM %s t ORDER BY t.id WITH cross_partition=true"
	}
	sql = fmt.Sprintf(sql, tblName)
	dbRows, err := sqlc.GetDB().Query(sql)
	if err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	defer dbRows.Close()
	rows := make([]map[string]interface{}, 0)
	err = sqlc.FetchRowsCallback(dbRows, func(row map[string]interface{}, err error) bool {
		rows = append(rows, row)
		return true
	})
	if err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}

	for i, row := range rows {
		for k, v := range row {
			// transform to lower-cases
			row[strings.ToLower(k)] = v
		}
		expected := rowArr[i]
		{
			f := "id"
			e := expected.id
			v, ok := row[f].(string)
			if !ok || v != e {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e, row[f])
			}
		}
		{
			e := expected.dataMoney2
			f := colNameList[1]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.2f", e), fmt.Sprintf("%.2f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v/%.10f(%T) but received %#v/%.10f(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, e, vstr, v, row[f], err)
			}
		}
		{
			e := expected.dataMoney4
			f := colNameList[2]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.4f", e), fmt.Sprintf("%.4f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v/%.10f(%T) but received %#v/%.10f(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, e, vstr, v, row[f], err)
			}
		}
		{
			e := expected.dataMoney6
			f := colNameList[3]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.6f", e), fmt.Sprintf("%.6f", v); err != nil || vstr != estr {
				fmt.Printf("\tDEBUG: Row %#v(%.10f) / Expected %#v(%.10f)\n", e, e, row[f], row[f])
				t.Fatalf("%s failed: [%s] expected %#v/%.10f(%T) but received %#v/%.10f(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, e, vstr, v, row[f], err)
			}
		}
		{
			e := expected.dataMoney8
			f := colNameList[4]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.8f", e), fmt.Sprintf("%.8f", v); err != nil || vstr != estr {
				fmt.Printf("\tDEBUG: Row %#v / Expected %#v\n", row[f], e)
				t.Fatalf("%s failed: [%s] expected %#v/%.10f(%T) but received %#v/%.10f(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, e, vstr, v, row[f], err)
			}
		}
	}
}

func TestSql_DataTypeMoney_Cosmos(t *testing.T) {
	name := "TestSql_DataTypeReal_Cosmos"
	urlMap := sqlGetUrlFromEnv()
	info, ok := urlMap["cosmos"]
	if !ok {
		info, ok = urlMap["cosmosdb"]
		if !ok {
			t.Skipf("%s skipped", name)
		}
	}
	sqlc, err := newSqlConnectCosmosdb(info.driver, info.url, timezoneSql, -1, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	} else if sqlc == nil {
		t.Fatalf("%s failed: nil", name)
	}

	_testSqlDataTypeMoney(t, name, sqlc, nil)
}

func TestSql_DataTypeMoney_Mssql(t *testing.T) {
	name := "TestSql_DataTypeMoney_Mssql"
	dbtype := "mssql"
	urlMap := sqlGetUrlFromEnv()
	info, ok := urlMap[dbtype]
	if !ok {
		t.Skipf("%s skipped", name)
	}
	sqlc, err := newSqlConnectMssql(info.driver, info.url, timezoneSql, -1, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	} else if sqlc == nil {
		t.Fatalf("%s failed: nil", name)
	}

	// MSSQL has no data type NUMBER
	// SMALLMONEY & MONEY have 4 decimal point digits
	sqlColTypes := []string{"NVARCHAR(8)",
		"DEC(36,2)", "MONEY", "DECIMAL(36,6)", "NUMERIC(36,8)"}
	_testSqlDataTypeMoney(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeMoney_Mysql(t *testing.T) {
	name := "TestSql_DataTypeMoney_Mysql"
	dbtype := "mysql"
	urlMap := sqlGetUrlFromEnv()
	info, ok := urlMap[dbtype]
	if !ok {
		t.Skipf("%s skipped", name)
	}
	sqlc, err := newSqlConnectMysql(info.driver, info.url, timezoneSql, -1, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	} else if sqlc == nil {
		t.Fatalf("%s failed: nil", name)
	}

	// MySQL has not data type NUMBER
	sqlColTypes := []string{"VARCHAR(8)",
		"DECIMAL(24,2)", "NUMERIC(28,4)", "DEC(32,6)", "NUMERIC(36,8)"}
	_testSqlDataTypeMoney(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeMoney_Oracle(t *testing.T) {
	name := "TestSql_DataTypeMoney_Oracle"
	dbtype := "oracle"
	urlMap := sqlGetUrlFromEnv()
	info, ok := urlMap[dbtype]
	if !ok {
		t.Skipf("%s skipped", name)
	}
	sqlc, err := newSqlConnectOracle(info.driver, info.url, timezoneSql, -1, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	} else if sqlc == nil {
		t.Fatalf("%s failed: nil", name)
	}

	sqlColTypes := []string{"NVARCHAR2(8)",
		"NUMERIC(24,2)", "DECIMAL(28,4)", "DEC(32,6)", "NUMERIC(36,8)"}
	_testSqlDataTypeMoney(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeMoney_Pgsql(t *testing.T) {
	name := "TestSql_DataTypeMoney_Pgsql"
	dbtype := "pgsql"
	urlMap := sqlGetUrlFromEnv()
	info, ok := urlMap[dbtype]
	if !ok {
		t.Skipf("%s skipped", name)
	}
	sqlc, err := newSqlConnectPgsql(info.driver, info.url, timezoneSql, -1, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	} else if sqlc == nil {
		t.Fatalf("%s failed: nil", name)
	}

	sqlColTypes := []string{"VARCHAR(8)",
		"MONEY", "NUMERIC(28,4)", "DECIMAL(32,6)", "DEC(36,8)"}
	_testSqlDataTypeMoney(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeMoney_Sqlite(t *testing.T) {
	name := "TestSql_DataTypeMoney_Sqlite"
	dbtype := "sqlite"
	urlMap := sqlGetUrlFromEnv()
	info, ok := urlMap[dbtype]
	if !ok {
		t.Skipf("%s skipped", name)
	}
	sqlc, err := newSqlConnectSqlite(info.driver, info.url, timezoneSql, -1, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	} else if sqlc == nil {
		t.Fatalf("%s failed: nil", name)
	}

	sqlColTypes := []string{"VARCHAR(8)",
		"DECIMAL(24,2)", "NUMERIC(28,4)", "DEC(32,6)", "NUMERIC(36,8)"}
	_testSqlDataTypeMoney(t, name, sqlc, sqlColTypes)
}
