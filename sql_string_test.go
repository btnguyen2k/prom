package prom

import (
	"encoding/base64"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"
)

var sqlColNamesTestDataTypeString = []string{"id",
	"data_char", "data_varchar", "data_binchar", "data_text",
	"data_uchar", "data_uvchar", "data_utext",
	"data_clob", "data_uclob", "data_blob"}

func _testSqlDataTypeString(t *testing.T, name string, sqlc *SqlConnect, colTypes []string) {
	tblName := "tbl_test"
	rand.Seed(time.Now().UnixNano())

	colNameList := sqlColNamesTestDataTypeString

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
		dataChar    string
		dataVchar   string
		dataBinchar []byte
		dataText    string
		dataUchar   string
		dataUvchar  string
		dataUtext   string
		dataClob    string
		dataUclob   string
		dataBlob    []byte
	}
	rowArr := make([]Row, 0)
	numRows := 100

	// insert some rows
	sql := fmt.Sprintf("INSERT INTO %s (", tblName)
	sql += strings.Join(colNameList, ",")
	sql += ") VALUES ("
	sql += _generatePlaceholders(len(colNameList), sqlc) + ")"
	for i := 1; i <= numRows; i++ {
		id := fmt.Sprintf("%03d", i)
		row := Row{
			id:          id,
			dataChar:    "CHAR " + id,
			dataVchar:   "VCHAR " + id,
			dataBinchar: []byte("BINCHAR " + id),
			dataText:    strings.Repeat("This is supposed to be a long text ", i*2),
			dataUchar:   "Chào buổi sáng, доброе утро, ສະ​ບາຍ​ດີ​ຕອນ​ເຊົ້າ, สวัสดีตอนเช้า",
			dataUvchar:  "Chào buổi sáng, доброе утро, ສະ​ບາຍ​ດີ​ຕອນ​ເຊົ້າ, สวัสดีตอนเช้า",
			dataUtext:   strings.Repeat("Chào buổi sáng, đây sẽ là một đoạn văn bản dài. доброе утро, ສະ​ບາຍ​ດີ​ຕອນ​ເຊົ້າ, สวัสดีตอนเช้า ", i*2),
			dataClob:    strings.Repeat("This is supposed to be a long text ", i*10),
			dataUclob:   strings.Repeat("Chào buổi sáng, đây sẽ là một đoạn văn bản dài. доброе утро, ສະ​ບາຍ​ດີ​ຕອນ​ເຊົ້າ, สวัสดีตอนเช้า ", i*10),
			dataBlob:    []byte(strings.Repeat("This is supposed to be a long text ", i*10)),
		}
		rowArr = append(rowArr, row)
		params := []interface{}{row.id, row.dataChar,
			row.dataVchar, row.dataBinchar, row.dataText, row.dataUchar, row.dataUvchar, row.dataUtext,
			row.dataClob, row.dataUclob, row.dataBlob}
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
		e := expected.dataChar
		f := colNameList[1]
		v, ok := row[f].(string)
		if !ok || strings.TrimSpace(v) != strings.TrimSpace(e) {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := expected.dataVchar
		f := colNameList[2]
		v, ok := row[f].(string)
		if !ok || strings.TrimSpace(v) != strings.TrimSpace(e) {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := expected.dataBinchar
		f := colNameList[3]
		v, ok := row[f].([]byte)
		if sqlc.flavor == FlavorCosmosDb {
			var t string
			t, ok = row[f].(string)
			if ok {
				var err error
				v, err = base64.StdEncoding.DecodeString(t)
				ok = err == nil
			}
		}
		if !ok || !reflect.DeepEqual(v, e) {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := expected.dataText
		f := colNameList[4]
		v, ok := row[f].(string)
		if !ok || strings.TrimSpace(v) != strings.TrimSpace(e) {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := expected.dataUchar
		f := colNameList[5]
		v, ok := row[f].(string)
		if !ok || strings.TrimSpace(v) != strings.TrimSpace(e) {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := expected.dataUvchar
		f := colNameList[6]
		v, ok := row[f].(string)
		if !ok || strings.TrimSpace(v) != strings.TrimSpace(e) {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := expected.dataUtext
		f := colNameList[7]
		v, ok := row[f].(string)
		if !ok || strings.TrimSpace(v) != strings.TrimSpace(e) {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := expected.dataClob
		f := colNameList[8]
		v, ok := row[f].(string)
		if !ok || strings.TrimSpace(v) != strings.TrimSpace(e) {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := expected.dataUclob
		f := colNameList[9]
		v, ok := row[f].(string)
		if !ok || strings.TrimSpace(v) != strings.TrimSpace(e) {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
	{
		e := expected.dataBlob
		f := colNameList[10]
		v, ok := row[f].([]byte)
		if sqlc.flavor == FlavorCosmosDb {
			var t string
			t, ok = row[f].(string)
			if ok {
				var err error
				v, err = base64.StdEncoding.DecodeString(t)
				ok = err == nil
			}
		}
		if !ok || !reflect.DeepEqual(v, e) {
			t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
		}
	}
}

func TestSql_DataTypeString_Cosmos(t *testing.T) {
	name := "TestSql_DataTypeString_Cosmos"
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

	_testSqlDataTypeString(t, name, sqlc, nil)
}

func TestSql_DataTypeString_Mssql(t *testing.T) {
	name := "TestSql_DataTypeString_Mssql"
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

	sqlColTypes := []string{"NVARCHAR(8)",
		"CHAR(255)", "VARCHAR(255)", "VARBINARY(255)", "TEXT",
		"NCHAR(255)", "NVARCHAR(255)", "NTEXT",
		"TEXT", "NTEXT", "IMAGE"}
	_testSqlDataTypeString(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeString_Mysql(t *testing.T) {
	name := "TestSql_DataTypeString_Mysql"
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

	sqlColTypes := []string{"VARCHAR(8)",
		"CHAR(255)", "VARCHAR(255)", "VARBINARY(255)", "TEXT",
		"CHAR(255)", "VARCHAR(255)", "TEXT",
		"MEDIUMTEXT", "LONGTEXT", "BLOB"}
	_testSqlDataTypeString(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeString_Oracle(t *testing.T) {
	name := "TestSql_DataTypeString_Oracle"
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
		"CHAR(255)", "VARCHAR2(255)", "RAW(255)", "CLOB",
		"NCHAR(255)", "NVARCHAR2(255)", "NCLOB",
		"CLOB", "NCLOB", "BLOB"}
	_testSqlDataTypeString(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeString_Pgsql(t *testing.T) {
	name := "TestSql_DataTypeString_Pgsql"
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
		"CHAR(255)", "VARCHAR(255)", "BYTEA", "TEXT",
		"CHAR(255)", "VARCHAR(255)", "TEXT",
		"TEXT", "TEXT", "BYTEA"}
	_testSqlDataTypeString(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeString_Sqlite(t *testing.T) {
	name := "TestSql_DataTypeString_Sqlite"
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
		"CHAR(255)", "VARCHAR(255)", "BLOB", "TEXT",
		"NCHAR(255)", "NVARCHAR(255)", "TEXT",
		"CLOB", "TEXT", "BLOB"}
	_testSqlDataTypeString(t, name, sqlc, sqlColTypes)
}
