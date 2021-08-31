package prom

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"
)

func _toFloatIfReal(v interface{}) (float64, error) {
	if v == nil {
		return 0, errors.New("input is nil")
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Float32, reflect.Float64:
		return rv.Float(), nil
	}
	fmt.Printf("\tDEBUG - %#v(%T)\n", v, v)
	return 0, errors.New("input is not real number")
}

func _toFloatIfNumber(v interface{}) (float64, error) {
	if v == nil {
		return 0, errors.New("input is nil")
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return rv.Float(), nil
	}
	return 0, errors.New("input is not valid number")
}

var sqlColNamesTestDataTypeReal = []string{"id",
	"data_float", "data_double", "data_real",
	"data_decimal", "data_number", "data_numeric",
	"data_float32", "data_float64",
	"data_float4", "data_float8"}

func _testSqlDataTypeReal(t *testing.T, name string, sqlc *SqlConnect, colTypes []string) {
	tblName := "tbl_test"
	rand.Seed(time.Now().UnixNano())

	colNameList := sqlColNamesTestDataTypeReal

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
		id          string
		dataFloat   float64
		dataDouble  float64
		dataReal    float64
		dataDecimal float64
		dataNumber  float64
		dataNumeric float64
		dataFloat32 float32
		dataFloat64 float64
		dataFloat4  float32
		dataFloat8  float64
	}
	rowArr := make([]Row, 0)
	numRows := 100

	// insert some rows
	sql := fmt.Sprintf("INSERT INTO %s (", tblName)
	sql += strings.Join(colNameList, ",")
	sql += ") VALUES ("
	sql += _generatePlaceholders(len(colNameList), sqlc) + ")"
	for i := 1; i <= numRows; i++ {
		vReal := rand.Float64()
		row := Row{
			id:          fmt.Sprintf("%03d", i),
			dataFloat:   math.Round(vReal*1e6) / 1e6,
			dataDouble:  math.Round(vReal*1e6) / 1e6,
			dataReal:    math.Round(vReal*1e6) / 1e6,
			dataDecimal: math.Round(vReal*1e6) / 1e6,
			dataNumber:  math.Round(vReal*1e6) / 1e6,
			dataNumeric: math.Round(vReal*1e6) / 1e6,
			dataFloat32: float32(math.Round(vReal*1e4) / 1e4),
			dataFloat64: math.Round(vReal*1e8) / 1e8,
			dataFloat4:  float32(math.Round(vReal*1e4) / 1e4),
			dataFloat8:  math.Round(vReal*1e8) / 1e8,
		}
		rowArr = append(rowArr, row)
		params := []interface{}{row.id, row.dataFloat, row.dataDouble, row.dataReal,
			row.dataDecimal, row.dataNumeric, row.dataNumeric,
			row.dataFloat32, row.dataFloat64, row.dataFloat4, row.dataFloat8}
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
			e := expected.dataFloat
			f := colNameList[1]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.f", e), fmt.Sprintf("%.f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
			}
		}
		{
			e := expected.dataDouble
			f := colNameList[2]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.f", e), fmt.Sprintf("%.f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
			}
		}
		{
			e := expected.dataReal
			f := colNameList[3]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.f", e), fmt.Sprintf("%.f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
			}
		}
		{
			e := expected.dataDecimal
			f := colNameList[4]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.f", e), fmt.Sprintf("%.f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
			}
		}
		{
			e := expected.dataNumber
			f := colNameList[5]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.f", e), fmt.Sprintf("%.f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
			}
		}
		{
			e := expected.dataNumeric
			f := colNameList[6]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.f", e), fmt.Sprintf("%.f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
			}
		}
		{
			e := expected.dataFloat32
			f := colNameList[7]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.f", e), fmt.Sprintf("%.f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
			}
		}
		{
			e := expected.dataFloat64
			f := colNameList[8]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.f", e), fmt.Sprintf("%.f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
			}
		}
		{
			e := expected.dataFloat4
			f := colNameList[9]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.f", e), fmt.Sprintf("%.f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
			}
		}
		{
			e := expected.dataFloat8
			f := colNameList[10]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.f", e), fmt.Sprintf("%.f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
			}
		}
	}
}

func TestSql_DataTypeReal_Cosmos(t *testing.T) {
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

	_testSqlDataTypeReal(t, name, sqlc, nil)
}

func TestSql_DataTypeReal_Mssql(t *testing.T) {
	name := "TestSql_DataTypeReal_Mssql"
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
	sqlColTypes := []string{"NVARCHAR(8)",
		"FLOAT(32)", "DOUBLE PRECISION", "REAL",
		"DECIMAL(38,6)", "DEC(38,6)", "NUMERIC(38,6)",
		"FLOAT(16)", "FLOAT(32)",
		"FLOAT(20)", "FLOAT(40)"}
	_testSqlDataTypeReal(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeReal_Mysql(t *testing.T) {
	name := "TestSql_DataTypeReal_Mysql"
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
		"FLOAT(32)", "DOUBLE(38,6)", "REAL",
		"DECIMAL(38,6)", "DEC(38,6)", "NUMERIC(38,6)",
		"FLOAT(16,4)", "DOUBLE(32,8)",
		"FLOAT(20,4)", "DOUBLE(40,8)"}
	_testSqlDataTypeReal(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeReal_Oracle(t *testing.T) {
	name := "TestSql_DataTypeReal_Oracle"
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
		"FLOAT", "DOUBLE PRECISION", "REAL",
		"DECIMAL(38,6)", "NUMBER(38,6)", "NUMERIC(38,6)",
		"BINARY_FLOAT", "BINARY_DOUBLE",
		"BINARY_FLOAT", "BINARY_DOUBLE"}
	_testSqlDataTypeReal(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeReal_Pgsql(t *testing.T) {
	name := "TestSql_DataTypeReal_Pgsql"
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

	// PostgreSQL has no data type NUMBER
	sqlColTypes := []string{"VARCHAR(8)",
		"FLOAT", "DOUBLE PRECISION", "REAL",
		"DECIMAL(38,6)", "NUMERIC(38,6)", "NUMERIC(38,6)",
		"FLOAT4", "FLOAT8",
		"FLOAT4", "FLOAT8"}
	_testSqlDataTypeReal(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeReal_Sqlite(t *testing.T) {
	name := "TestSql_DataTypeReal_Sqlite"
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
		"FLOAT", "DOUBLE", "REAL",
		"DECIMAL(38,6)", "NUMBER(38,6)", "NUMERIC(38,6)",
		"FLOAT", "DOUBLE PRECISION",
		"FLOAT4", "FLOAT8"}
	_testSqlDataTypeReal(t, name, sqlc, sqlColTypes)
}

func _testSqlDataTypeRealZero(t *testing.T, name string, sqlc *SqlConnect, colTypes []string) {
	tblName := "tbl_test"
	rand.Seed(time.Now().UnixNano())

	colNameList := sqlColNamesTestDataTypeReal

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
		id          string
		dataFloat   float64
		dataDouble  float64
		dataReal    float64
		dataDecimal float64
		dataNumber  float64
		dataNumeric float64
		dataFloat32 float32
		dataFloat64 float64
		dataFloat4  float32
		dataFloat8  float64
	}
	rowArr := make([]Row, 0)
	numRows := 100

	// insert some rows
	sql := fmt.Sprintf("INSERT INTO %s (", tblName)
	sql += strings.Join(colNameList, ",")
	sql += ") VALUES ("
	sql += _generatePlaceholders(len(colNameList), sqlc) + ")"
	for i := 1; i <= numRows; i++ {
		vReal := float64(rand.Intn(12345))
		row := Row{
			id:          fmt.Sprintf("%03d", i),
			dataFloat:   math.Round(vReal*1e6) / 1e6,
			dataDouble:  math.Round(vReal*1e6) / 1e6,
			dataReal:    math.Round(vReal*1e6) / 1e6,
			dataDecimal: math.Round(vReal*1e6) / 1e6,
			dataNumber:  math.Round(vReal*1e6) / 1e6,
			dataNumeric: math.Round(vReal*1e6) / 1e6,
			dataFloat32: float32(math.Round(vReal*1e4) / 1e4),
			dataFloat64: math.Round(vReal*1e8) / 1e8,
			dataFloat4:  float32(math.Round(vReal*1e4) / 1e4),
			dataFloat8:  math.Round(vReal*1e8) / 1e8,
		}
		rowArr = append(rowArr, row)
		params := []interface{}{row.id, row.dataFloat, row.dataDouble, row.dataReal,
			row.dataDecimal, row.dataNumeric, row.dataNumeric,
			row.dataFloat32, row.dataFloat64, row.dataFloat4, row.dataFloat8}
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
			e := expected.dataFloat
			f := colNameList[1]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.f", e), fmt.Sprintf("%.f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
			}
		}
		{
			e := expected.dataDouble
			f := colNameList[2]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.f", e), fmt.Sprintf("%.f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
			}
		}
		{
			e := expected.dataReal
			f := colNameList[3]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.f", e), fmt.Sprintf("%.f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
			}
		}
		{
			e := expected.dataDecimal
			f := colNameList[4]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.f", e), fmt.Sprintf("%.f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
			}
		}
		{
			e := expected.dataNumber
			f := colNameList[5]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.f", e), fmt.Sprintf("%.f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
			}
		}
		{
			e := expected.dataNumeric
			f := colNameList[6]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.f", e), fmt.Sprintf("%.f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
			}
		}
		{
			e := expected.dataFloat32
			f := colNameList[7]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.f", e), fmt.Sprintf("%.f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
			}
		}
		{
			e := expected.dataFloat64
			f := colNameList[8]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.f", e), fmt.Sprintf("%.f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
			}
		}
		{
			e := expected.dataFloat4
			f := colNameList[9]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.f", e), fmt.Sprintf("%.f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
			}
		}
		{
			e := expected.dataFloat8
			f := colNameList[10]
			v, err := _toFloatIfReal(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.f", e), fmt.Sprintf("%.f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
			}
		}
	}
}

func TestSql_DataTypeRealZero_Cosmos(t *testing.T) {
	name := "TestSql_DataTypeRealZero_Cosmos"
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

	_testSqlDataTypeRealZero(t, name, sqlc, nil)
}

func TestSql_DataTypeRealZero_Mssql(t *testing.T) {
	name := "TestSql_DataTypeRealZero_Mssql"
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
	sqlColTypes := []string{"NVARCHAR(8)",
		"FLOAT(32)", "DOUBLE PRECISION", "REAL",
		"DECIMAL(38,6)", "DEC(38,6)", "NUMERIC(38,6)",
		"FLOAT(16)", "FLOAT(32)",
		"FLOAT(20)", "FLOAT(40)"}
	_testSqlDataTypeRealZero(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeRealZero_Mysql(t *testing.T) {
	name := "TestSql_DataTypeRealZero_Mysql"
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
		"FLOAT(32)", "DOUBLE(38,6)", "REAL",
		"DECIMAL(38,6)", "DEC(38,6)", "NUMERIC(38,6)",
		"FLOAT(16,4)", "DOUBLE(32,8)",
		"FLOAT(20,4)", "DOUBLE(40,8)"}
	_testSqlDataTypeRealZero(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeRealZero_Oracle(t *testing.T) {
	name := "TestSql_DataTypeRealZero_Oracle"
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
		"FLOAT", "DOUBLE PRECISION", "REAL",
		"DECIMAL(38,6)", "NUMBER(38,6)", "NUMERIC(38,6)",
		"BINARY_FLOAT", "BINARY_DOUBLE",
		"BINARY_FLOAT", "BINARY_DOUBLE"}
	_testSqlDataTypeRealZero(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeRealZero_Pgsql(t *testing.T) {
	name := "TestSql_DataTypeRealZero_Pgsql"
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

	// PostgreSQL has no data type NUMBER
	sqlColTypes := []string{"VARCHAR(8)",
		"FLOAT", "DOUBLE PRECISION", "REAL",
		"DECIMAL(38,6)", "NUMERIC(38,6)", "NUMERIC(38,6)",
		"FLOAT4", "FLOAT8",
		"FLOAT4", "FLOAT8"}
	_testSqlDataTypeRealZero(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeRealZero_Sqlite(t *testing.T) {
	name := "TestSql_DataTypeRealZero_Sqlite"
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
		"FLOAT", "DOUBLE", "REAL",
		"DECIMAL(38,6)", "NUMBER(38,6)", "NUMERIC(38,6)",
		"FLOAT", "DOUBLE PRECISION",
		"FLOAT4", "FLOAT8"}
	_testSqlDataTypeRealZero(t, name, sqlc, sqlColTypes)
}
