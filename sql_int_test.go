package prom

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"
)

func _toIntIfInteger(v interface{}) (int64, error) {
	if v == nil {
		return 0, errors.New("input is nil")
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(rv.Uint()), nil
	}
	return 0, errors.New("input is not integer")
}

func _toIntIfNumber(v interface{}) (int64, error) {
	if v == nil {
		return 0, errors.New("input is nil")
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(rv.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return int64(rv.Float()), nil
	}
	return 0, errors.New("input is not valid number")
}

var sqlColNamesTestDataTypeInt = []string{"id",
	"data_int", "data_integer", "data_decimal", "data_number", "data_numeric",
	"data_tinyint", "data_smallint", "data_mediumint", "data_bigint",
	"data_int1", "data_int2", "data_int4", "data_int8"}

func _testSqlDataTypeInt(t *testing.T, name string, sqlc *SqlConnect, colTypes []string) {
	tblName := "tbl_test"
	rand.Seed(time.Now().UnixNano())

	colNameList := sqlColNamesTestDataTypeInt

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
		id            string
		dataInt       int
		dataInteger   int
		dataDecimal   int
		dataNumber    int
		dataNumeric   int
		dataTinyInt   int8
		dataSmallInt  int16
		dataMediumInt int32
		dataBigInt    int64
		dataInt1      int8
		dataInt2      int16
		dataInt4      int32
		dataInt8      int64
	}
	rowArr := make([]Row, 0)
	numRows := 100

	// insert some rows
	sql := fmt.Sprintf("INSERT INTO %s (", tblName)
	sql += strings.Join(colNameList, ",")
	sql += ") VALUES ("
	sql += _generatePlaceholders(len(colNameList), sqlc) + ")"
	for i := 1; i <= numRows; i++ {
		vInt := rand.Int63()
		if sqlc.flavor == FlavorCosmosDb {
			vInt >>= 63 - 48
		}
		row := Row{
			id:            fmt.Sprintf("%03d", i),
			dataInt:       int(vInt%(2^32)) + 1,
			dataInteger:   int(vInt%(2^32)) + 2,
			dataDecimal:   int(vInt%(2^32)) + 3,
			dataNumber:    int(vInt%(2^32)) + 4,
			dataNumeric:   int(vInt%(2^32)) + 5,
			dataTinyInt:   int8(vInt%(2^8)) + 6,
			dataSmallInt:  int16(vInt%(2^16)) + 7,
			dataMediumInt: int32(vInt%(2^24)) + 8,
			dataBigInt:    vInt - 1,
			dataInt1:      int8(vInt%(2^8)) + 9,
			dataInt2:      int16(vInt%(2^16)) + 10,
			dataInt4:      int32(vInt%(2^24)) + 11,
			dataInt8:      vInt - 2,
		}
		rowArr = append(rowArr, row)
		params := []interface{}{row.id, row.dataInt, row.dataInteger, row.dataDecimal, row.dataNumber, row.dataNumeric,
			row.dataTinyInt, row.dataSmallInt, row.dataMediumInt, row.dataBigInt,
			row.dataInt1, row.dataInt2, row.dataInt4, row.dataInt8}
		if sqlc.flavor == FlavorCosmosDb {
			params = append(params, row.id)
		}
		_, err := sqlc.GetDB().Exec(sql, params...)
		if err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
	}

	// query some rows
	id := rand.Intn(numRows) + 1
	placeholder := _generatePlaceholders(1, sqlc)
	sql = "SELECT * FROM %s WHERE id>=%s ORDER BY id"
	if sqlc.flavor == FlavorCosmosDb {
		sql = "SELECT * FROM %s t WHERE t.id>=%s WITH cross_partition=true"
	}
	sql = fmt.Sprintf(sql, tblName, placeholder)
	params := []interface{}{fmt.Sprintf("%03d", id)}
	dbRows, err := sqlc.GetDB().Query(sql, params...)
	if err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	defer dbRows.Close()
	rows := make([]map[string]interface{}, 0)
	err = sqlc.FetchRowsCallback(dbRows, func(row map[string]interface{}, err error) bool {
		rows = append(rows, row)
		return true
	})
	// rows, err := sqlc.FetchRows(dbRows)
	if err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	row := rows[0]
	for k, v := range row {
		// transform to lower-cases
		row[strings.ToLower(k)] = v
	}
	expected := rowArr[id-1]

	// fmt.Printf("\tDEBUG: %#v\n", row)
	{
		f := "id"
		e := expected.id
		v, ok := row[f].(string)
		if !ok || v != e {
			t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e, row[f])
		}
	}
	{
		e := int64(expected.dataInt)
		f := colNameList[1]
		v, err := _toIntIfInteger(row[f])
		if sqlc.flavor == FlavorCosmosDb {
			v, err = _toIntIfNumber(row[f])
		}
		if err != nil || v != e {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := int64(expected.dataInteger)
		f := colNameList[2]
		v, err := _toIntIfInteger(row[f])
		if sqlc.flavor == FlavorCosmosDb {
			v, err = _toIntIfNumber(row[f])
		}
		if err != nil || v != e {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := int64(expected.dataDecimal)
		f := colNameList[3]
		v, err := _toIntIfInteger(row[f])
		if sqlc.flavor == FlavorCosmosDb {
			v, err = _toIntIfNumber(row[f])
		}
		if err != nil || v != e {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := int64(expected.dataNumber)
		f := colNameList[4]
		v, err := _toIntIfInteger(row[f])
		if sqlc.flavor == FlavorCosmosDb {
			v, err = _toIntIfNumber(row[f])
		}
		if err != nil || v != e {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := int64(expected.dataNumeric)
		f := colNameList[5]
		v, err := _toIntIfInteger(row[f])
		if sqlc.flavor == FlavorCosmosDb {
			v, err = _toIntIfNumber(row[f])
		}
		if err != nil || v != e {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := int64(expected.dataTinyInt)
		f := colNameList[6]
		v, err := _toIntIfInteger(row[f])
		if sqlc.flavor == FlavorCosmosDb {
			v, err = _toIntIfNumber(row[f])
		}
		if err != nil || v != e {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := int64(expected.dataSmallInt)
		f := colNameList[7]
		v, err := _toIntIfInteger(row[f])
		if sqlc.flavor == FlavorCosmosDb {
			v, err = _toIntIfNumber(row[f])
		}
		if err != nil || v != e {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := int64(expected.dataMediumInt)
		f := colNameList[8]
		v, err := _toIntIfInteger(row[f])
		if sqlc.flavor == FlavorCosmosDb {
			v, err = _toIntIfNumber(row[f])
		}
		if err != nil || v != e {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := int64(expected.dataBigInt)
		f := colNameList[9]
		v, err := _toIntIfInteger(row[f])
		if sqlc.flavor == FlavorCosmosDb {
			v, err = _toIntIfNumber(row[f])
		}
		if err != nil || v != e {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := int64(expected.dataInt1)
		f := colNameList[10]
		v, err := _toIntIfInteger(row[f])
		if sqlc.flavor == FlavorCosmosDb {
			v, err = _toIntIfNumber(row[f])
		}
		if err != nil || v != e {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := int64(expected.dataInt2)
		f := colNameList[11]
		v, err := _toIntIfInteger(row[f])
		if sqlc.flavor == FlavorCosmosDb {
			v, err = _toIntIfNumber(row[f])
		}
		if err != nil || v != e {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := int64(expected.dataInt4)
		f := colNameList[12]
		v, err := _toIntIfInteger(row[f])
		if sqlc.flavor == FlavorCosmosDb {
			v, err = _toIntIfNumber(row[f])
		}
		if err != nil || v != e {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := int64(expected.dataInt8)
		f := colNameList[13]
		v, err := _toIntIfInteger(row[f])
		if sqlc.flavor == FlavorCosmosDb {
			v, err = _toIntIfNumber(row[f])
		}
		if err != nil || v != e {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
}

func TestSql_DataTypeInt_Cosmos(t *testing.T) {
	name := "TestSql_DataTypeInt_Cosmos"
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

	_testSqlDataTypeInt(t, name, sqlc, nil)
}

func TestSql_DataTypeInt_Mssql(t *testing.T) {
	name := "TestSql_DataTypeInt_Mssql"
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
	// DECIMAL(*,0) or NUMERIC(*,0) can be used as integer
	sqlColTypes := []string{"NVARCHAR(8)",
		"INT", "INTEGER", "DECIMAL(32,0)", "DECIMAL(36,0)", "NUMERIC(38,0)",
		"TINYINT", "SMALLINT", "INT", "BIGINT",
		"TINYINT", "SMALLINT", "INTEGER", "BIGINT"}
	_testSqlDataTypeInt(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeInt_Mysql(t *testing.T) {
	name := "TestSql_DataTypeInt_Mysql"
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
	// DECIMAL(*,0) or NUMERIC(*,0) can be used as integer, but FLOAT(*,0) or DOUBLE(*,0) cannot
	sqlColTypes := []string{"NVARCHAR(8)",
		"INT", "INTEGER", "DECIMAL(32,0)", "NUMERIC(36,0)", "NUMERIC(40,0)",
		"TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT",
		"TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT"}
	_testSqlDataTypeInt(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeInt_Oracle(t *testing.T) {
	name := "TestSql_DataTypeInt_Oracle"
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

	// NUMERIC(*,0) or NUMBER(*,0) OR DECIMAL(*,0) can be used as integer
	sqlColTypes := []string{"NVARCHAR2(8)",
		"INT", "INTEGER", "NUMERIC(38,0)", "NUMBER(38,0)", "DECIMAL(38,0)",
		"NUMERIC(3,0)", "SMALLINT", "DECIMAL(19,0)", "DEC(38,0)",
		"DEC(4,0)", "NUMBER(8,0)", "DECIMAL(16,0)", "NUMERIC(32,0)"}
	_testSqlDataTypeInt(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeInt_Pgsql(t *testing.T) {
	name := "TestSql_DataTypeInt_Pgsql"
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
	// NUMERIC(*,0) or NUMBER(*,0) OR DECIMAL(*,0) cannot be used as integer
	sqlColTypes := []string{"VARCHAR(8)",
		"INT", "INTEGER", "INT", "INTEGER", "INT",
		"INT", "SMALLINT", "INTEGER", "BIGINT",
		"INT", "INT2", "INT4", "INT8"}
	_testSqlDataTypeInt(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeInt_Sqlite(t *testing.T) {
	name := "TestSql_DataTypeInt_Sqlite"
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

	// doNOT use DECIMAL(*,0), NUMBER(*,0) or NUMERIC(*,0) as integer
	sqlColTypes := []string{"NVARCHAR(8)",
		// "INT", "INTEGER", "DECIMAL(32,0)", "NUMBER(32,0)", "NUMERIC(32,0)",
		"INT", "INTEGER", "INTEGER", "INT", "INT",
		"TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT",
		"INT1", "INT2", "INT4", "INT8"}
	_testSqlDataTypeInt(t, name, sqlc, sqlColTypes)
}

func TestSql_DataType_PgsqlSerial(t *testing.T) {
	name := "TestSql_DataType_PgsqlSerial"
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

	colNameList := []string{"id", "data_smallserial", "data_serial", "dataa_bigserial"}
	colTypes := []string{"VARCHAR(8)", "SMALLSERIAL", "SERIAL", "BIGSERIAL"}
	tblName := "tbl_test"
	rand.Seed(time.Now().UnixNano())

	// init
	sqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE %s", tblName))
	sql := fmt.Sprintf("CREATE TABLE %s (", tblName)
	for i := range colNameList {
		sql += colNameList[i] + " " + colTypes[i] + ","
	}
	sql += fmt.Sprintf("PRIMARY KEY(%s))", colNameList[0])
	if _, err := sqlc.GetDB().Exec(sql); err != nil {
		t.Fatalf("%s failed: %s\n%s", name, err, sql)
	}

	type Row struct {
		id              string
		dataSmallSerial int
		dataSerial      int
		dataBigSerial   int
	}
	rowArr := make([]Row, 0)
	numRows := 100

	// insert some rows
	sql = fmt.Sprintf("INSERT INTO %s (", tblName)
	sql += strings.Join(colNameList, ",")
	sql += ") VALUES ("
	sql += _generatePlaceholders(len(colNameList), sqlc) + ")"
	for i := 1; i <= numRows; i++ {
		vInt := rand.Int63()
		row := Row{
			id:              fmt.Sprintf("%03d", i),
			dataSmallSerial: int(vInt%(2^16)) + 1,
			dataSerial:      int(vInt%(2^24)) + 2,
			dataBigSerial:   int(vInt%(2^32)) + 3,
		}
		rowArr = append(rowArr, row)
		params := []interface{}{row.id, row.dataSmallSerial, row.dataSerial, row.dataBigSerial}
		_, err := sqlc.GetDB().Exec(sql, params...)
		if err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
	}

	// query some rows
	id := rand.Intn(numRows) + 1
	placeholder := _generatePlaceholders(1, sqlc)
	sql = "SELECT * FROM %s WHERE id>=%s ORDER BY id"
	sql = fmt.Sprintf(sql, tblName, placeholder)
	params := []interface{}{fmt.Sprintf("%03d", id)}
	dbRows, err := sqlc.GetDB().Query(sql, params...)
	if err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	defer dbRows.Close()
	rows, err := sqlc.FetchRows(dbRows)
	if err != nil {
		t.Fatalf("%s failed: %s", name, err)
	}
	row := rows[0]
	for k, v := range row {
		// transform to lower-cases
		row[strings.ToLower(k)] = v
	}
	expected := rowArr[id-1]

	{
		f := "id"
		e := expected.id
		v, ok := row[f].(string)
		if !ok || v != e {
			t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e, row[f])
		}
	}
	{
		e := int64(expected.dataSmallSerial)
		f := colNameList[1]
		v, err := _toIntIfInteger(row[f])
		if err != nil || v != e {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := int64(expected.dataSerial)
		f := colNameList[2]
		v, err := _toIntIfInteger(row[f])
		if err != nil || v != e {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := int64(expected.dataBigSerial)
		f := colNameList[3]
		v, err := _toIntIfInteger(row[f])
		if err != nil || v != e {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
}
