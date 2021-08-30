package prom

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"
)

func _startOfDay(t time.Time) time.Time {
	arr := []byte(t.Format(time.RFC3339))
	arr[11], arr[12], arr[14], arr[15], arr[17], arr[18] = '0', '0', '0', '0', '0', '0'
	t, _ = time.ParseInLocation(time.RFC3339, string(arr), t.Location())
	return t
}

func _changeLoc(t time.Time, loc *time.Location) time.Time {
	if loc == nil {
		loc = time.UTC
	}
	format := "2006-01-02T15:04:05.999999999"
	_t, _ := time.ParseInLocation(format, t.Format(format), loc)
	return _t
}

var sqlColNamesTestDataTypeDatetime = []string{"id", "data_date", "data_time", "data_datetime", "data_datetimez", "data_duration"}

func _testSqlDataTypeDatetime(t *testing.T, name string, sqlc *SqlConnect, colTypes []string) {
	tblName := "tbl_test"
	rand.Seed(time.Now().UnixNano())

	colNameList := sqlColNamesTestDataTypeDatetime

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
		dataDate      time.Time
		dataTime      time.Time
		dataDatetime  time.Time
		dataDatetimez time.Time
		dataDuration  time.Duration
	}
	rowArr := make([]Row, 0)
	numRows := 100

	LOC, _ := time.LoadLocation(timezoneSql)
	LOC2, _ := time.LoadLocation(timezoneSql2)

	// insert some rows
	sql := fmt.Sprintf("INSERT INTO %s (", tblName)
	sql += strings.Join(colNameList, ",")
	sql += ") VALUES ("
	sql += _generatePlaceholders(len(colNameList), sqlc) + ")"
	for i := 1; i <= numRows; i++ {
		vDatetime, _ := time.ParseInLocation("2006-01-02T15:04:05", "2021-02-28T23:24:25", time.UTC)
		row := Row{
			id:            fmt.Sprintf("%03d", i),
			dataDate:      _startOfDay(vDatetime),          // no timezone support
			dataTime:      _changeLoc(vDatetime, sqlc.loc), // no timezone support
			dataDatetime:  _changeLoc(vDatetime, sqlc.loc), // no timezone support
			dataDatetimez: vDatetime,
			dataDuration:  time.Duration(rand.Int63n(1024)) * time.Second,
		}
		rowArr = append(rowArr, row)
		params := []interface{}{row.id, row.dataDate, row.dataTime, row.dataDatetime, row.dataDatetimez, row.dataDuration}
		if sqlc.flavor == FlavorCosmosDb {
			params = append(params, row.id)
		}
		_, err := sqlc.GetDB().Exec(sql, params...)
		if err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
		// fmt.Printf("Original: %s - %s - %s / D: %s / T: %s / DT: %s / DTz: %s\n",
		// 	vDatetime.Format(time.RFC3339), vDatetime.In(time.UTC).Format(time.RFC3339), vDatetime.In(sqlc.loc).Format(time.RFC3339),
		// 	row.dataDate.Format(time.RFC3339),
		// 	row.dataTime.Format(time.RFC3339),
		// 	row.dataDatetime.Format(time.RFC3339),
		// 	row.dataDatetimez.Format(time.RFC3339))
		// break
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
			layout := "2006-01-02"
			e := expected.dataDate
			f := colNameList[1]
			v, ok := row[f].(time.Time)
			if sqlc.flavor == FlavorCosmosDb {
				t, err := time.ParseInLocation(time.RFC3339, row[f].(string), LOC)
				v = t
				ok = err == nil
			}
			if eloc, vloc := sqlc.loc, v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
				t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", name, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
			}
			v = _changeLoc(v, e.Location())
			if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
				t.Fatalf("%s failed: [%s]\nexpected %#v/%#v/%#v(%T)\nbut received %#v/%#v/%#v(%T) (Ok: %#v)", name,
					row["id"].(string)+"/"+f, estr, e.Format(time.RFC3339), e.In(LOC2).Format(time.RFC3339), e,
					vstr, v.Format(time.RFC3339), v.In(LOC2).Format(time.RFC3339), row[f], ok)
			}
		}
		{
			layout := "15:04:05"
			e := expected.dataTime
			f := colNameList[2]
			v, ok := row[f].(time.Time)
			if sqlc.flavor == FlavorCosmosDb {
				t, err := time.ParseInLocation(time.RFC3339, row[f].(string), LOC)
				v = t
				ok = err == nil
			}
			if eloc, vloc := sqlc.loc, v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
				t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", name, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
			}
			if sqlc.flavor != FlavorOracle && sqlc.flavor != FlavorSqlite {
				v = _changeLoc(v, e.Location())
			}
			if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
				t.Fatalf("%s failed: [%s]\nexpected %#v/%#v/%#v(%T)\nbut received %#v/%#v/%#v(%T) (Ok: %#v)", name,
					row["id"].(string)+"/"+f, estr, e.Format(time.RFC3339), e.In(LOC2).Format(time.RFC3339), e,
					vstr, v.Format(time.RFC3339), v.In(LOC2).Format(time.RFC3339), row[f], ok)
			}
		}
		{
			layout := time.RFC3339
			e := expected.dataDatetime
			f := colNameList[3]
			v, ok := row[f].(time.Time)
			if sqlc.flavor == FlavorCosmosDb {
				t, err := time.ParseInLocation(time.RFC3339, row[f].(string), LOC)
				v = t
				ok = err == nil
			}
			if eloc, vloc := sqlc.loc, v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
				t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", name, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
			}
			if sqlc.flavor != FlavorOracle && sqlc.flavor != FlavorSqlite {
				v = _changeLoc(v, e.Location())
			}
			if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
				t.Fatalf("%s failed: [%s]\nexpected %#v/%#v/%#v(%T)\nbut received %#v/%#v/%#v(%T) (Ok: %#v)", name,
					row["id"].(string)+"/"+f, estr, e.Format(time.RFC3339), e.In(LOC2).Format(time.RFC3339), e,
					vstr, v.Format(time.RFC3339), v.In(LOC2).Format(time.RFC3339), row[f], ok)
			}
		}
		{
			layout := time.RFC3339
			e := expected.dataDatetimez
			f := colNameList[4]
			v, ok := row[f].(time.Time)
			if sqlc.flavor == FlavorCosmosDb {
				t, err := time.ParseInLocation(time.RFC3339, row[f].(string), LOC)
				v = t
				ok = err == nil
			}
			if eloc, vloc := sqlc.loc, v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
				t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", name, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
			}
			if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
				t.Fatalf("%s failed: [%s]\nexpected %#v/%#v/%#v(%T)\nreceived %#v/%#v/%#v(%T) (Ok: %#v)",
					name, row["id"].(string)+"/"+f,
					estr, e.Format(time.RFC3339), e.In(LOC2).Format(time.RFC3339), e,
					vstr, v.Format(time.RFC3339), v.In(LOC2).Format(time.RFC3339), row[f],
					ok)
			}
		}
		{
			e := expected.dataDuration
			f := colNameList[5]
			v, err := _toIntIfInteger(row[f])
			if sqlc.flavor == FlavorCosmosDb {
				v, err = _toIntIfNumber(row[f])
			}
			if err != nil || v != int64(e) {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
			}
		}
	}
}

func TestSql_DataTypeDatetime_Cosmos(t *testing.T) {
	name := "TestSql_DataTypeDatetime_Cosmos"
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

	_testSqlDataTypeDatetime(t, name, sqlc, nil)
}

func TestSql_DataTypeDatetime_Mssql(t *testing.T) {
	name := "TestSql_DataTypeDatetime_Mssql"
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

	sqlColTypes := []string{"NVARCHAR(8)", "DATE", "TIME", "DATETIME2", "DATETIMEOFFSET", "BIGINT"}
	_testSqlDataTypeDatetime(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeDatetime_Mysql(t *testing.T) {
	name := "TestSql_DataTypeDatetime_Mysql"
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

	sqlColTypes := []string{"VARCHAR(8)", "DATE", "TIME", "DATETIME", "TIMESTAMP", "BIGINT"}
	_testSqlDataTypeDatetime(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeDatetime_Oracle(t *testing.T) {
	name := "TestSql_DataTypeDatetime_Oracle"
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

	sqlColTypes := []string{"NVARCHAR2(8)", "DATE", "DATE", "DATE", "TIMESTAMP(0) WITH TIME ZONE", "INTERVAL DAY TO SECOND"}
	_testSqlDataTypeDatetime(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeDatetime_Pgsql(t *testing.T) {
	name := "TestSql_DataTypeDatetime_Pgsql"
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

	sqlColTypes := []string{"VARCHAR(8)", "DATE", "TIME(0)", "TIMESTAMP(0)", "TIMESTAMP(0) WITH TIME ZONE", "BIGINT"}
	_testSqlDataTypeDatetime(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeDatetime_Sqlite(t *testing.T) {
	name := "TestSql_DataTypeDatetime_Sqlite"
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

	sqlColTypes := []string{"VARCHAR(8)", "DATE", "TIME", "DATETIME", "DATETIME", "BIGINT"}
	_testSqlDataTypeDatetime(t, name, sqlc, sqlColTypes)
}

/*----------------------------------------------------------------------*/

var sqlColNamesTestDataTypeNull = []string{"id",
	"data_int", "data_float", "data_string", "data_money",
	"data_date", "data_time", "data_datetime", "data_duration"}

func _testSqlDataTypeNull(t *testing.T, name string, sqlc *SqlConnect, colTypes []string) {
	tblName := "tbl_test"
	rand.Seed(time.Now().UnixNano())

	colNameList := sqlColNamesTestDataTypeNull

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
		id           string
		dataInt      *int64
		dataFloat    *float64
		dataString   *string
		dataMoney    *float64
		dataDate     *time.Time
		dataTime     *time.Time
		dataDatetime *time.Time
		dataDuration *time.Duration
	}
	rowArr := make([]Row, 0)
	numRows := 100

	LOC, _ := time.LoadLocation(timezoneSql)
	LOC2, _ := time.LoadLocation(timezoneSql2)

	// insert some rows
	sql := fmt.Sprintf("INSERT INTO %s (", tblName)
	sql += strings.Join(colNameList, ",")
	sql += ") VALUES ("
	sql += _generatePlaceholders(len(colNameList), sqlc) + ")"
	for i := 1; i <= numRows; i++ {
		vInt := rand.Int63n(1024)
		vFloat := math.Round(rand.Float64()*1e3) / 1e3
		vString := strconv.Itoa(rand.Intn(1024))
		vMoney := math.Round(rand.Float64()*1e2) / 1e2
		vDatetime, _ := time.ParseInLocation("2006-01-02T15:04:05", "2021-02-28T23:24:25", time.UTC)
		vDuration := time.Duration(rand.Int63n(1024)) * time.Second
		row := Row{id: fmt.Sprintf("%03d", i)}
		if i%2 == 0 {
			row.dataInt = &vInt
			row.dataFloat = &vFloat
		}
		if i%3 == 0 {
			row.dataString = &vString
		}
		if i%4 == 0 {
			row.dataMoney = &vMoney
		}
		if i%5 == 0 {
			_vDate := _startOfDay(vDatetime)              // assume no timezone support
			_vTime := _changeLoc(vDatetime, sqlc.loc)     // assume no timezone support
			_vDatetime := _changeLoc(vDatetime, sqlc.loc) // assume no timezone support
			row.dataDate = &_vDate
			row.dataTime = &_vTime
			row.dataDatetime = &_vDatetime
		}
		if i%6 == 0 {
			row.dataDuration = &vDuration
		}
		rowArr = append(rowArr, row)
		params := []interface{}{row.id, row.dataInt, row.dataFloat, row.dataString, row.dataMoney, row.dataDate, row.dataTime, row.dataDatetime, row.dataDuration}
		if sqlc.flavor == FlavorCosmosDb {
			params = append(params, row.id)
		}
		_, err := sqlc.GetDB().Exec(sql, params...)
		if err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
		// if i == 5 {
		// 	fmt.Printf("Original: %s - %s - %s / D: %s / T: %s / DT: %s\n",
		// 		vDatetime.Format(time.RFC3339), vDatetime.In(time.UTC).Format(time.RFC3339), vDatetime.In(sqlc.loc).Format(time.RFC3339),
		// 		row.dataDate.Format(time.RFC3339),
		// 		row.dataTime.Format(time.RFC3339),
		// 		row.dataDatetime.Format(time.RFC3339))
		// 	break
		// }
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
			f := colNameList[1]
			if (i+1)%2 == 0 {
				e := expected.dataInt
				v, err := _toIntIfInteger(row[f])
				if sqlc.flavor == FlavorCosmosDb {
					v, err = _toIntIfNumber(row[f])
				}
				if err != nil || v != *e {
					t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
				}
			} else if row[f] != (*int64)(nil) && (sqlc.flavor == FlavorCosmosDb && row[f] != nil) {
				t.Fatalf("%s failed: [%s] expected nil but received %#v(%T)", name, row["id"].(string)+"/"+f, row[f], row[f])
			}
		}
		{
			f := colNameList[2]
			if (i+1)%2 == 0 {
				e := expected.dataFloat
				v, err := _toFloatIfReal(row[f])
				if sqlc.flavor == FlavorCosmosDb {
					v, err = _toFloatIfNumber(row[f])
				}
				if estr, vstr := fmt.Sprintf("%.3f", *e), fmt.Sprintf("%.3f", v); err != nil || vstr != estr {
					t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
				}
			} else if row[f] != (*float64)(nil) && (sqlc.flavor == FlavorCosmosDb && row[f] != nil) {
				t.Fatalf("%s failed: [%s] expected nil but received %#v(%T)", name, row["id"].(string)+"/"+f, row[f], row[f])
			}
		}
		{
			f := colNameList[3]
			if (i+1)%3 == 0 {
				e := expected.dataString
				v, ok := row[f].(string)
				if !ok || strings.TrimSpace(v) != strings.TrimSpace(*e) {
					t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
				}
			} else if row[f] != (*string)(nil) && (sqlc.flavor == FlavorCosmosDb && row[f] != nil) {
				t.Fatalf("%s failed: [%s] expected nil but received %#v(%T)", name, row["id"].(string)+"/"+f, row[f], row[f])
			}
		}
		{
			f := colNameList[4]
			if (i+1)%4 == 0 {
				e := expected.dataMoney
				v, err := _toFloatIfReal(row[f])
				if sqlc.flavor == FlavorCosmosDb {
					v, err = _toFloatIfNumber(row[f])
				}
				if estr, vstr := fmt.Sprintf("%.2f", *e), fmt.Sprintf("%.2f", v); err != nil || vstr != estr {
					t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
				}
			} else if row[f] != (*float64)(nil) && (sqlc.flavor == FlavorCosmosDb && row[f] != nil) {
				t.Fatalf("%s failed: [%s] expected nil but received %#v(%T)", name, row["id"].(string)+"/"+f, row[f], row[f])
			}
		}
		{
			f := colNameList[5]
			if (i+1)%5 == 0 {
				layout := "2006-01-02"
				e := expected.dataDate
				v, ok := row[f].(time.Time)
				if sqlc.flavor == FlavorCosmosDb {
					t, err := time.ParseInLocation(time.RFC3339, row[f].(string), LOC)
					v = t
					ok = err == nil
				}
				if eloc, vloc := sqlc.loc, v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
					t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", name, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
				}
				v = _changeLoc(v, e.Location())
				if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
					t.Fatalf("%s failed: [%s]\nexpected %#v/%#v(%T)\nbut received %#v/%#v(%T) (Ok: %#v)", name,
						row["id"].(string)+"/"+f, estr, e.Format(time.RFC3339), e,
						vstr, v.Format(time.RFC3339), row[f], ok)
				}
			} else if row[f] != (*time.Time)(nil) && ((sqlc.flavor == FlavorCosmosDb) && row[f] != nil) {
				t.Fatalf("%s failed: [%s] expected nil but received %#v(%T)", name, row["id"].(string)+"/"+f, row[f], row[f])
			}
		}
		{
			f := colNameList[6]
			if (i+1)%5 == 0 {
				layout := "15:04:05"
				e := expected.dataTime
				v, ok := row[f].(time.Time)
				if sqlc.flavor == FlavorCosmosDb {
					t, err := time.ParseInLocation(time.RFC3339, row[f].(string), LOC)
					v = t
					ok = err == nil
				}
				if eloc, vloc := sqlc.loc, v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
					t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", name, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
				}
				if sqlc.flavor != FlavorOracle && sqlc.flavor != FlavorSqlite {
					v = _changeLoc(v, e.Location())
				}
				if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
					fmt.Printf("Expected: %s / %s (UTC) / %s (%s) / %s (%s)\n", e.Location(), e.In(time.UTC), e.In(LOC), LOC, e.In(LOC2), LOC2)
					t.Fatalf("%s failed: [%s]\nexpected %#v/%#v/%#v(%T)\nbut received %#v/%#v/%#v(%T) (Ok: %#v)", name,
						"row:"+row["id"].(string)+"/field:"+f, estr, e.Format(time.RFC3339), e.In(LOC2).Format(time.RFC3339), e,
						vstr, v.Format(time.RFC3339), v.In(LOC2).Format(time.RFC3339), row[f], ok)
				}
			} else if row[f] != (*time.Time)(nil) && ((sqlc.flavor == FlavorCosmosDb) && row[f] != nil) {
				t.Fatalf("%s failed: [%s] expected nil but received %#v(%T)", name, row["id"].(string)+"/"+f, row[f], row[f])
			}
		}
		{
			f := colNameList[7]
			if (i+1)%5 == 0 {
				layout := time.RFC3339
				e := expected.dataDatetime
				v, ok := row[f].(time.Time)
				if sqlc.flavor == FlavorCosmosDb {
					t, err := time.ParseInLocation(time.RFC3339, row[f].(string), LOC)
					v = t
					ok = err == nil
				}
				if eloc, vloc := sqlc.loc, v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
					t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", name, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
				}
				if sqlc.flavor != FlavorOracle && sqlc.flavor != FlavorSqlite {
					v = _changeLoc(v, e.Location())
				}
				if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
					t.Fatalf("%s failed: [%s]\nexpected %#v/%#v(%T)\nbut received %#v/%#v(%T) (Ok: %#v)", name,
						row["id"].(string)+"/"+f, estr, e.Format(time.RFC3339), e,
						vstr, v.Format(time.RFC3339), row[f], ok)
				}
			} else if row[f] != (*time.Time)(nil) && ((sqlc.flavor == FlavorCosmosDb) && row[f] != nil) {
				t.Fatalf("%s failed: [%s] expected nil but received %#v(%T)", name, row["id"].(string)+"/"+f, row[f], row[f])
			}
		}
		{
			f := colNameList[8]
			if (i+1)%6 == 0 {
				e := expected.dataDuration
				v, err := _toIntIfInteger(row[f])
				if sqlc.flavor == FlavorCosmosDb {
					v, err = _toIntIfNumber(row[f])
				}
				if err != nil || v != int64(*e) {
					t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
				}
			} else if row[f] != (*int64)(nil) && ((sqlc.flavor == FlavorCosmosDb) && row[f] != nil) {
				t.Fatalf("%s failed: [%s] expected nil but received %#v(%T)", name, row["id"].(string)+"/"+f, row[f], row[f])
			}
		}
	}
}

func TestSql_DataTypeNull_Cosmos(t *testing.T) {
	name := "TestSql_DataTypeNull_Cosmos"
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

	_testSqlDataTypeNull(t, name, sqlc, nil)
}

func TestSql_DataTypeNull_Mssql(t *testing.T) {
	name := "TestSql_DataTypeNull_Mssql"
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
		"INT", "DOUBLE PRECISION", "NVARCHAR(64)", "MONEY", "DATE", "TIME", "DATETIME2", "BIGINT"}
	_testSqlDataTypeNull(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeNull_Mysql(t *testing.T) {
	name := "TestSql_DataTypeNull_Mysql"
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
		"INT", "DOUBLE", "VARCHAR(64)", "DECIMAL(36,2)", "DATE", "TIME", "DATETIME", "BIGINT"}
	_testSqlDataTypeNull(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeNull_Oracle(t *testing.T) {
	name := "TestSql_DataTypeNull_Oracle"
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
		"INT", "DOUBLE PRECISION", "NVARCHAR2(64)", "NUMERIC(36,2)", "DATE", "TIMESTAMP(0)", "TIMESTAMP(0)", "INTERVAL DAY TO SECOND"}
	_testSqlDataTypeNull(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeNull_Pgsql(t *testing.T) {
	name := "TestSql_DataTypeNull_Pgsql"
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
		"INT", "DOUBLE PRECISION", "VARCHAR(64)", "NUMERIC(36,2)", "DATE", "TIME(0)", "TIMESTAMP(0)", "BIGINT"}
	_testSqlDataTypeNull(t, name, sqlc, sqlColTypes)
}

func TestSql_DataTypeNull_Sqlite(t *testing.T) {
	name := "TestSql_DataTypeNull_Sqlite"
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
		"INT", "DOUBLE", "VARCHAR(64)", "DECIMAL(36,2)", "DATE", "TIME", "DATETIME", "BIGINT"}
	_testSqlDataTypeNull(t, name, sqlc, sqlColTypes)
}

/*----------------------------------------------------------------------*/

var sqlColNamesTestTimezone = []string{"id", "data_date", "data_time", "data_datetime", "data_datetimez"}

func _testSqlTimezone(t *testing.T, name string, sqlc, sqlc2 *SqlConnect, colTypes []string) {
	tblName := "tbl_test"
	rand.Seed(time.Now().UnixNano())

	colNameList := sqlColNamesTestTimezone

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
		dataDate      time.Time
		dataTime      time.Time
		dataDatetime  time.Time
		dataDatetimez time.Time
	}
	rowArr := make([]Row, 0)
	numRows := 100

	LOC := sqlc.loc
	LOC2 := sqlc2.loc

	// insert some rows
	sql := fmt.Sprintf("INSERT INTO %s (", tblName)
	sql += strings.Join(colNameList, ",")
	sql += ") VALUES ("
	sql += _generatePlaceholders(len(colNameList), sqlc) + ")"
	for i := 1; i <= numRows; i++ {
		vDatetime, _ := time.ParseInLocation("2006-01-02T15:04:05", "2021-02-28T23:24:25", LOC)
		// vDatetime = vDatetime.Add(time.Duration(rand.Intn(1024)) * time.Minute)
		row := Row{
			id:            fmt.Sprintf("%03d", i),
			dataDate:      _startOfDay(vDatetime),          // no timezone support
			dataTime:      _changeLoc(vDatetime, time.UTC), // no timezone support --> move to UTC
			dataDatetime:  _changeLoc(vDatetime, time.UTC), // no timezone support --> convert to UTC
			dataDatetimez: vDatetime,
		}
		if sqlc.flavor == FlavorMySql {
			// special care for MySQL (not sure if it's behavior of MySQL server or go-sql-driver/mysql)
			row.dataTime = row.dataTime.In(sqlc.loc)
			row.dataDatetime = row.dataDatetime.In(sqlc.loc)
		}
		rowArr = append(rowArr, row)
		params := []interface{}{row.id, row.dataDate, row.dataTime, row.dataDatetime, row.dataDatetimez}
		if sqlc.flavor == FlavorCosmosDb {
			params = append(params, row.id)
		}
		_, err := sqlc.GetDB().Exec(sql, params...)
		if err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
		// fmt.Printf("Original: %s - %s / %s / %s / %s / %s\n",
		// 	vDatetime.Format(time.RFC3339), vDatetime.In(time.UTC).Format(time.RFC3339),
		// 	row.dataDate.Format(time.RFC3339),
		// 	row.dataTime.Format(time.RFC3339),
		// 	row.dataDatetime.Format(time.RFC3339),
		// 	row.dataDatetimez.Format(time.RFC3339))
		// break
	}

	// query some rows
	sql = "SELECT * FROM %s ORDER BY id"
	if sqlc.flavor == FlavorCosmosDb {
		sql = "SELECT * FROM %s t ORDER BY t.id WITH cross_partition=true"
	}
	sql = fmt.Sprintf(sql, tblName)

	for idx, conn := range []*SqlConnect{sqlc, sqlc2} {
		dbRows, err := conn.GetDB().Query(sql)
		if err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
		rows := make([]map[string]interface{}, 0)
		err = conn.FetchRowsCallback(dbRows, func(row map[string]interface{}, err error) bool {
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
				layout := "2006-01-02"
				e := expected.dataDate
				f := colNameList[1]
				v, ok := row[f].(time.Time)
				if conn.flavor == FlavorCosmosDb {
					t, err := time.ParseInLocation(time.RFC3339, row[f].(string), conn.loc)
					v = t
					ok = err == nil
				}
				v = _changeLoc(v, e.Location())
				if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
					t.Fatalf("%s failed: [%s]\nexpected %#v/%#v/%#v(%T)\nbut received %#v/%#v/%#v(%T) (Ok: %#v)", name,
						"idx:"+strconv.Itoa(idx)+"/row:"+row["id"].(string)+"/field:"+f, estr, e.Format(time.RFC3339), e.In(LOC2).Format(time.RFC3339), e,
						vstr, v.Format(time.RFC3339), v.In(LOC2).Format(time.RFC3339), row[f], ok)
				}
			}
			{
				layout := "15:04:05"
				e := expected.dataTime
				f := colNameList[2]
				v, ok := row[f].(time.Time)
				if conn.flavor == FlavorCosmosDb {
					t, err := time.ParseInLocation(time.RFC3339, row[f].(string), LOC)
					v = t
					ok = err == nil
				}
				if conn.flavor != FlavorOracle && conn.flavor != FlavorSqlite {
					v = _changeLoc(v, e.Location())
				}
				if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
					t.Fatalf("%s failed: [%s]\nexpected %#v/%#v/%#v(%T)\nbut received %#v/%#v/%#v(%T) (Ok: %#v)", name,
						"idx:"+strconv.Itoa(idx)+"/row:"+row["id"].(string)+"/field:"+f, estr, e.Format(time.RFC3339), e.In(LOC2).Format(time.RFC3339), e,
						vstr, v.Format(time.RFC3339), v.In(LOC2).Format(time.RFC3339), row[f], ok)
				}
			}
			{
				layout := time.RFC3339
				e := expected.dataDatetime
				f := colNameList[3]
				v, ok := row[f].(time.Time)
				if conn.flavor == FlavorCosmosDb {
					t, err := time.ParseInLocation(time.RFC3339, row[f].(string), LOC)
					v = t
					ok = err == nil
				}
				if conn.flavor != FlavorOracle && conn.flavor != FlavorSqlite {
					v = _changeLoc(v, e.Location())
				}
				if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
					t.Fatalf("%s failed: [%s]\nexpected %#v/%#v/%#v(%T)\nbut received %#v/%#v/%#v(%T) (Ok: %#v)", name,
						"idx:"+strconv.Itoa(idx)+"/row:"+row["id"].(string)+"/field:"+f, estr, e.Format(time.RFC3339), e.In(LOC2).Format(time.RFC3339), e,
						vstr, v.Format(time.RFC3339), v.In(LOC2).Format(time.RFC3339), row[f], ok)
				}
			}
			{
				layout := time.RFC3339
				e := expected.dataDatetimez
				f := colNameList[4]
				v, ok := row[f].(time.Time)
				if conn.flavor == FlavorCosmosDb {
					t, err := time.ParseInLocation(time.RFC3339, row[f].(string), LOC)
					v = t
					ok = err == nil
				}
				if conn.flavor == FlavorMySql {
					// currently, the Go driver treats parseTime=false for "TIME" column
					v = _changeLoc(v, e.Location())
				}
				if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
					t.Fatalf("%s failed: [%s]\nexpected %#v/%#v/%#v(%T)\nbut received %#v/%#v/%#v(%T) (Ok: %#v)", name,
						"idx:"+strconv.Itoa(idx)+"/row:"+row["id"].(string)+"/field:"+f, estr, e.Format(time.RFC3339), e.In(LOC2).Format(time.RFC3339), e,
						vstr, v.Format(time.RFC3339), v.In(LOC2).Format(time.RFC3339), row[f], ok)
				}
			}
		}
	}
}

func TestSql_Timezone_Cosmos(t *testing.T) {
	name := "TestSql_Timezone_Cosmos"
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
	sqlc2, err := newSqlConnectCosmosdb(info.driver, info.url, timezoneSql2, -1, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	} else if sqlc2 == nil {
		t.Fatalf("%s failed: nil", name)
	}

	_testSqlTimezone(t, name, sqlc, sqlc2, nil)
}

func TestSql_Timezone_Mssql(t *testing.T) {
	name := "TestSql_Timezone_Mssql"
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
	sqlc2, err := newSqlConnectMssql(info.driver, info.url, timezoneSql2, -1, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	} else if sqlc2 == nil {
		t.Fatalf("%s failed: nil", name)
	}

	sqlColTypes := []string{"NVARCHAR(8)", "DATE", "TIME", "DATETIME2", "DATETIMEOFFSET"}
	_testSqlTimezone(t, name, sqlc, sqlc2, sqlColTypes)
}

func TestSql_Timezone_Mysql(t *testing.T) {
	name := "TestSql_Timezone_Mysql"
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
	sqlc2, err := newSqlConnectMysql(info.driver, info.url, timezoneSql2, -1, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	} else if sqlc2 == nil {
		t.Fatalf("%s failed: nil", name)
	}

	sqlColTypes := []string{"VARCHAR(8)", "DATE", "TIME", "DATETIME", "TIMESTAMP"}
	_testSqlTimezone(t, name, sqlc, sqlc2, sqlColTypes)
}

func TestSql_Timezone_Oracle(t *testing.T) {
	name := "TestSql_Timezone_Oracle"
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
	sqlc2, err := newSqlConnectOracle(info.driver, info.url, timezoneSql2, -1, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	} else if sqlc2 == nil {
		t.Fatalf("%s failed: nil", name)
	}

	sqlColTypes := []string{"NVARCHAR2(8)", "DATE", "TIMESTAMP(0)", "TIMESTAMP(0)", "TIMESTAMP(0) WITH TIME ZONE"}
	_testSqlTimezone(t, name, sqlc, sqlc2, sqlColTypes)
}

func TestSql_Timezone_Pgsql(t *testing.T) {
	name := "TestSql_Timezone_Pgsql"
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
	sqlc2, err := newSqlConnectPgsql(info.driver, info.url, timezoneSql2, -1, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	} else if sqlc2 == nil {
		t.Fatalf("%s failed: nil", name)
	}

	sqlColTypes := []string{"VARCHAR(8)", "DATE", "TIME(0)", "TIMESTAMP(0)", "TIMESTAMP(0) WITH TIME ZONE"}
	_testSqlTimezone(t, name, sqlc, sqlc2, sqlColTypes)
}

func TestSql_Timezone_Sqlite(t *testing.T) {
	name := "TestSql_Timezone_Sqlite"
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
	sqlc2, err := newSqlConnectSqlite(info.driver, info.url, timezoneSql2, -1, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	} else if sqlc2 == nil {
		t.Fatalf("%s failed: nil", name)
	}

	sqlColTypes := []string{"VARCHAR(8)", "DATE", "TIME", "DATETIME", "DATETIME"}
	_testSqlTimezone(t, name, sqlc, sqlc2, sqlColTypes)
}
