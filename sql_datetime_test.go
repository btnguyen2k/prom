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

func TestSql_DataTypeDatetime(t *testing.T) {
	testName := "TestSql_DataTypeDatetime"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	rand.Seed(time.Now().UnixNano())
	tblName := "test_datetime"
	colNameList := sqlColNamesTestDataTypeDatetime
	colTypesMap := map[DbFlavor][]string{
		FlavorCosmosDb: nil,
		FlavorMsSql:    {"NVARCHAR(8)", "DATE", "TIME", "DATETIME2", "DATETIMEOFFSET", "BIGINT"},
		FlavorMySql:    {"VARCHAR(8)", "DATE", "TIME", "DATETIME", "TIMESTAMP", "BIGINT"},
		FlavorOracle:   {"NVARCHAR2(8)", "DATE", "DATE", "DATE", "TIMESTAMP(0) WITH TIME ZONE", "INTERVAL DAY TO SECOND"},
		FlavorPgSql:    {"VARCHAR(8)", "DATE", "TIME(0)", "TIMESTAMP(0)", "TIMESTAMP(0) WITH TIME ZONE", "BIGINT"},
		FlavorSqlite:   {"VARCHAR(8)", "DATE", "TIME", "DATETIME", "DATETIME", "BIGINT"},
	}
	type Row struct {
		id            string
		dataDate      time.Time
		dataTime      time.Time
		dataDatetime  time.Time
		dataDatetimez time.Time
		dataDuration  time.Duration
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		colTypes := colTypesMap[sqlc.GetDbFlavor()]
		t.Run(dbtype, func(t *testing.T) {
			// init table
			sqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE %s", tblName))
			if sqlc.flavor == FlavorCosmosDb {
				stm := fmt.Sprintf("CREATE COLLECTION %s WITH pk=/%s", tblName, colNameList[0])
				if _, err := sqlc.GetDB().Exec(stm); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				}
			} else {
				sql := fmt.Sprintf("CREATE TABLE %s (", tblName)
				for i := range colNameList {
					sql += colNameList[i] + " " + colTypes[i] + ","
				}
				sql += fmt.Sprintf("PRIMARY KEY(%s))", colNameList[0])
				if _, err := sqlc.GetDB().Exec(sql); err != nil {
					t.Fatalf("%s failed: %s\n%s", testName, err, sql)
				}
			}

			rowArr := make([]Row, 0)
			numRows := 100

			// LOC, _ := time.LoadLocation(timezoneSql)
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
					dataDate:      _changeLoc(_startOfDay(vDatetime), sqlc.loc), // no timezone support
					dataTime:      _changeLoc(vDatetime, sqlc.loc),              // no timezone support
					dataDatetime:  _changeLoc(vDatetime, sqlc.loc),              // no timezone support
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
					t.Fatalf("%s failed: %s", testName, err)
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
				t.Fatalf("%s failed: %s", testName, err)
			}
			defer dbRows.Close()
			rows := make([]map[string]interface{}, 0)
			err = sqlc.FetchRowsCallback(dbRows, func(row map[string]interface{}, err error) bool {
				rows = append(rows, row)
				return true
			})
			if err != nil {
				t.Fatalf("%s failed: %s", testName, err)
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
						t.Fatalf("%s failed: [%s] expected %#v but received %#v", testName, f, e, row[f])
					}
				}
				{
					layout := "2006-01-02"
					e := expected.dataDate
					f := colNameList[1]
					v, ok := row[f].(time.Time)
					if sqlc.flavor == FlavorCosmosDb {
						t, err := time.ParseInLocation(time.RFC3339, row[f].(string), sqlc.loc)
						v = _changeLoc(t, sqlc.loc)
						ok = err == nil
					}
					if eloc, vloc := sqlc.loc, v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
						t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", testName, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
					}
					v = _changeLoc(v, e.Location())
					if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
						t.Fatalf("%s failed: [%s]\nexpected %#v/%#v/%#v(%T)\nbut received %#v/%#v/%#v(%T) (Ok: %#v)", testName,
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
						t, err := time.ParseInLocation(time.RFC3339, row[f].(string), sqlc.loc)
						v = _changeLoc(t, sqlc.loc)
						ok = err == nil
					}
					if eloc, vloc := sqlc.loc, v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
						t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", testName, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
					}
					if sqlc.flavor != FlavorOracle && sqlc.flavor != FlavorSqlite {
						v = _changeLoc(v, e.Location())
					}
					if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
						t.Fatalf("%s failed: [%s]\nexpected %#v/%#v/%#v(%T)\nbut received %#v/%#v/%#v(%T) (Ok: %#v)", testName,
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
						t, err := time.ParseInLocation(time.RFC3339, row[f].(string), sqlc.loc)
						v = _changeLoc(t, sqlc.loc)
						ok = err == nil
					}
					if eloc, vloc := sqlc.loc, v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
						t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", testName, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
					}
					if sqlc.flavor != FlavorOracle && sqlc.flavor != FlavorSqlite {
						v = _changeLoc(v, e.Location())
					}
					if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
						t.Fatalf("%s failed: [%s]\nexpected %#v/%#v/%#v(%T)\nbut received %#v/%#v/%#v(%T) (Ok: %#v)", testName,
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
						t, err := time.ParseInLocation(time.RFC3339, row[f].(string), sqlc.loc)
						v = t.In(sqlc.loc)
						ok = err == nil
					}
					if eloc, vloc := sqlc.loc, v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
						t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", testName, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
					}
					if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
						t.Fatalf("%s failed: [%s]\nexpected %#v/%#v/%#v(%T)\nreceived %#v/%#v/%#v(%T) (Ok: %#v)",
							testName, row["id"].(string)+"/"+f,
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
						t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
					}
				}
			}
		})
	}
}

var sqlColNamesTestDataTypeNull = []string{"id",
	"data_int", "data_float", "data_string", "data_money",
	"data_date", "data_time", "data_datetime", "data_duration"}

func TestSql_DataTypeNull(t *testing.T) {
	testName := "TestSql_DataTypeNull"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	rand.Seed(time.Now().UnixNano())
	tblName := "test_null"
	colNameList := sqlColNamesTestDataTypeNull
	colTypesMap := map[DbFlavor][]string{
		FlavorCosmosDb: nil,
		FlavorMsSql:    {"NVARCHAR(8)", "INT", "DOUBLE PRECISION", "NVARCHAR(64)", "MONEY", "DATE", "TIME", "DATETIME2", "BIGINT"},
		FlavorMySql:    {"VARCHAR(8)", "INT", "DOUBLE", "VARCHAR(64)", "DECIMAL(36,2)", "DATE", "TIME", "DATETIME", "BIGINT"},
		FlavorOracle:   {"NVARCHAR2(8)", "INT", "DOUBLE PRECISION", "NVARCHAR2(64)", "NUMERIC(36,2)", "DATE", "TIMESTAMP(0)", "TIMESTAMP(0)", "INTERVAL DAY TO SECOND"},
		FlavorPgSql:    {"VARCHAR(8)", "INT", "DOUBLE PRECISION", "VARCHAR(64)", "NUMERIC(36,2)", "DATE", "TIME(0)", "TIMESTAMP(0)", "BIGINT"},
		FlavorSqlite:   {"VARCHAR(8)", "INT", "DOUBLE", "VARCHAR(64)", "DECIMAL(36,2)", "DATE", "TIME", "DATETIME", "BIGINT"},
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
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		colTypes := colTypesMap[sqlc.GetDbFlavor()]
		t.Run(dbtype, func(t *testing.T) {
			// init table
			sqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE %s", tblName))
			if sqlc.flavor == FlavorCosmosDb {
				stm := fmt.Sprintf("CREATE COLLECTION %s WITH pk=/%s", tblName, colNameList[0])
				if _, err := sqlc.GetDB().Exec(stm); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				}
			} else {
				sql := fmt.Sprintf("CREATE TABLE %s (", tblName)
				for i := range colNameList {
					sql += colNameList[i] + " " + colTypes[i] + ","
				}
				sql += fmt.Sprintf("PRIMARY KEY(%s))", colNameList[0])
				if _, err := sqlc.GetDB().Exec(sql); err != nil {
					t.Fatalf("%s failed: %s\n%s", testName, err, sql)
				}
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
					_vDate := _changeLoc(_startOfDay(vDatetime), sqlc.loc) // assume no timezone support
					_vTime := _changeLoc(vDatetime, sqlc.loc)              // assume no timezone support
					_vDatetime := _changeLoc(vDatetime, sqlc.loc)          // assume no timezone support
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
					t.Fatalf("%s failed: %s", testName, err)
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
				t.Fatalf("%s failed: %s", testName, err)
			}
			defer dbRows.Close()
			rows := make([]map[string]interface{}, 0)
			err = sqlc.FetchRowsCallback(dbRows, func(row map[string]interface{}, err error) bool {
				rows = append(rows, row)
				return true
			})
			if err != nil {
				t.Fatalf("%s failed: %s", testName, err)
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
						t.Fatalf("%s failed: [%s] expected %#v but received %#v", testName, f, e, row[f])
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
							t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
						}
					} else if row[f] != (*int64)(nil) && (sqlc.flavor == FlavorCosmosDb && row[f] != nil) {
						t.Fatalf("%s failed: [%s] expected nil but received %#v(%T)", testName, row["id"].(string)+"/"+f, row[f], row[f])
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
							t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
						}
					} else if row[f] != (*float64)(nil) && (sqlc.flavor == FlavorCosmosDb && row[f] != nil) {
						t.Fatalf("%s failed: [%s] expected nil but received %#v(%T)", testName, row["id"].(string)+"/"+f, row[f], row[f])
					}
				}
				{
					f := colNameList[3]
					if (i+1)%3 == 0 {
						e := expected.dataString
						v, ok := row[f].(string)
						if !ok || strings.TrimSpace(v) != strings.TrimSpace(*e) {
							t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
						}
					} else if row[f] != (*string)(nil) && (sqlc.flavor == FlavorCosmosDb && row[f] != nil) {
						t.Fatalf("%s failed: [%s] expected nil but received %#v(%T)", testName, row["id"].(string)+"/"+f, row[f], row[f])
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
							t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
						}
					} else if row[f] != (*float64)(nil) && (sqlc.flavor == FlavorCosmosDb && row[f] != nil) {
						t.Fatalf("%s failed: [%s] expected nil but received %#v(%T)", testName, row["id"].(string)+"/"+f, row[f], row[f])
					}
				}
				{
					f := colNameList[5]
					if (i+1)%5 == 0 {
						layout := "2006-01-02"
						e := expected.dataDate
						v, ok := row[f].(time.Time)
						if sqlc.flavor == FlavorCosmosDb {
							t, err := time.ParseInLocation(time.RFC3339, row[f].(string), sqlc.loc)
							v = _changeLoc(t, sqlc.loc)
							ok = err == nil
						}
						if eloc, vloc := sqlc.loc, v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
							t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", testName, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
						}
						v = _changeLoc(v, e.Location())
						if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
							t.Fatalf("%s failed: [%s]\nexpected %#v/%#v(%T)\nbut received %#v/%#v(%T) (Ok: %#v)", testName,
								row["id"].(string)+"/"+f, estr, e.Format(time.RFC3339), e,
								vstr, v.Format(time.RFC3339), row[f], ok)
						}
					} else if row[f] != (*time.Time)(nil) && ((sqlc.flavor == FlavorCosmosDb) && row[f] != nil) {
						t.Fatalf("%s failed: [%s] expected nil but received %#v(%T)", testName, row["id"].(string)+"/"+f, row[f], row[f])
					}
				}
				{
					f := colNameList[6]
					if (i+1)%5 == 0 {
						layout := "15:04:05"
						e := expected.dataTime
						v, ok := row[f].(time.Time)
						if sqlc.flavor == FlavorCosmosDb {
							t, err := time.ParseInLocation(time.RFC3339, row[f].(string), sqlc.loc)
							v = _changeLoc(t, sqlc.loc)
							ok = err == nil
						}
						if eloc, vloc := sqlc.loc, v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
							t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", testName, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
						}
						if sqlc.flavor != FlavorOracle && sqlc.flavor != FlavorSqlite {
							v = _changeLoc(v, e.Location())
						}
						if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
							fmt.Printf("Expected: %s / %s (UTC) / %s (%s) / %s (%s)\n", e.Location(), e.In(time.UTC), e.In(LOC), LOC, e.In(LOC2), LOC2)
							t.Fatalf("%s failed: [%s]\nexpected %#v/%#v/%#v(%T)\nbut received %#v/%#v/%#v(%T) (Ok: %#v)", testName,
								"row:"+row["id"].(string)+"/field:"+f, estr, e.Format(time.RFC3339), e.In(LOC2).Format(time.RFC3339), e,
								vstr, v.Format(time.RFC3339), v.In(LOC2).Format(time.RFC3339), row[f], ok)
						}
					} else if row[f] != (*time.Time)(nil) && ((sqlc.flavor == FlavorCosmosDb) && row[f] != nil) {
						t.Fatalf("%s failed: [%s] expected nil but received %#v(%T)", testName, row["id"].(string)+"/"+f, row[f], row[f])
					}
				}
				{
					f := colNameList[7]
					if (i+1)%5 == 0 {
						layout := time.RFC3339
						e := expected.dataDatetime
						v, ok := row[f].(time.Time)
						if sqlc.flavor == FlavorCosmosDb {
							t, err := time.ParseInLocation(time.RFC3339, row[f].(string), sqlc.loc)
							v = _changeLoc(t, sqlc.loc)
							ok = err == nil
						}
						if eloc, vloc := sqlc.loc, v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
							t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", testName, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
						}
						if sqlc.flavor != FlavorOracle && sqlc.flavor != FlavorSqlite {
							v = _changeLoc(v, e.Location())
						}
						if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
							t.Fatalf("%s failed: [%s]\nexpected %#v/%#v(%T)\nbut received %#v/%#v(%T) (Ok: %#v)", testName,
								row["id"].(string)+"/"+f, estr, e.Format(time.RFC3339), e,
								vstr, v.Format(time.RFC3339), row[f], ok)
						}
					} else if row[f] != (*time.Time)(nil) && ((sqlc.flavor == FlavorCosmosDb) && row[f] != nil) {
						t.Fatalf("%s failed: [%s] expected nil but received %#v(%T)", testName, row["id"].(string)+"/"+f, row[f], row[f])
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
							t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
						}
					} else if row[f] != (*int64)(nil) && ((sqlc.flavor == FlavorCosmosDb) && row[f] != nil) {
						t.Fatalf("%s failed: [%s] expected nil but received %#v(%T)", testName, row["id"].(string)+"/"+f, row[f], row[f])
					}
				}
			}
		})
	}
}

var sqlColNamesTestTimezone = []string{"id", "data_date", "data_time", "data_datetime", "data_datetimez"}

/*----------------------------------------------------------------------*/

func TestSql_Timezone(t *testing.T) {
	testName := "TestSql_Timezone"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	rand.Seed(time.Now().UnixNano())
	tblName := "test_timezone"
	colNameList := sqlColNamesTestTimezone
	colTypesMap := map[DbFlavor][]string{
		FlavorCosmosDb: nil,
		FlavorMsSql:    {"NVARCHAR(8)", "DATE", "TIME", "DATETIME2", "DATETIMEOFFSET"},
		FlavorMySql:    {"VARCHAR(8)", "DATE", "TIME", "DATETIME", "TIMESTAMP"},
		FlavorOracle:   {"NVARCHAR2(8)", "DATE", "TIMESTAMP(0)", "TIMESTAMP(0)", "TIMESTAMP(0) WITH TIME ZONE"},
		FlavorPgSql:    {"VARCHAR(8)", "DATE", "TIME(0)", "TIMESTAMP(0)", "TIMESTAMP(0) WITH TIME ZONE"},
		FlavorSqlite:   {"VARCHAR(8)", "DATE", "TIME", "DATETIME", "DATETIME"},
	}
	type Row struct {
		id            string
		dataDate      time.Time
		dataTime      time.Time
		dataDatetime  time.Time
		dataDatetimez time.Time
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		sqlc2 := sqlc2List[index]
		colTypes := colTypesMap[sqlc.GetDbFlavor()]
		t.Run(dbtype, func(t *testing.T) {
			// init table
			sqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE %s", tblName))
			if sqlc.flavor == FlavorCosmosDb {
				if _, err := sqlc.GetDB().Exec(fmt.Sprintf("CREATE COLLECTION %s WITH pk=/%s", tblName, colNameList[0])); err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				}
			} else {
				sql := fmt.Sprintf("CREATE TABLE %s (", tblName)
				for i := range colNameList {
					sql += colNameList[i] + " " + colTypes[i] + ","
				}
				sql += fmt.Sprintf("PRIMARY KEY(%s))", colNameList[0])
				if _, err := sqlc.GetDB().Exec(sql); err != nil {
					t.Fatalf("%s failed: %s\n%s", testName, err, sql)
				}
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
				row := Row{
					id:            fmt.Sprintf("%03d", i),
					dataDate:      _changeLoc(_startOfDay(vDatetime), sqlc.loc), // no timezone support
					dataTime:      _changeLoc(vDatetime, sqlc.loc),              // no timezone support
					dataDatetime:  _changeLoc(vDatetime, sqlc.loc),              // no timezone support
					dataDatetimez: vDatetime,
				}
				rowArr = append(rowArr, row)
				params := []interface{}{row.id, row.dataDate, row.dataTime, row.dataDatetime, row.dataDatetimez}
				if sqlc.flavor == FlavorCosmosDb {
					params = append(params, row.id)
				}
				_, err := sqlc.GetDB().Exec(sql, params...)
				if err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				}
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
					t.Fatalf("%s failed: %s", testName, err)
				}
				rows := make([]map[string]interface{}, 0)
				err = conn.FetchRowsCallback(dbRows, func(row map[string]interface{}, err error) bool {
					rows = append(rows, row)
					return true
				})
				if err != nil {
					t.Fatalf("%s failed: %s", testName, err)
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
							t.Fatalf("%s failed: [%s] expected %#v but received %#v", testName, f, e, row[f])
						}
					}
					{
						layout := "2006-01-02"
						e := expected.dataDate
						f := colNameList[1]
						v, ok := row[f].(time.Time)
						if conn.flavor == FlavorCosmosDb {
							t, err := time.ParseInLocation(time.RFC3339, row[f].(string), sqlc.loc)
							v = _changeLoc(t, sqlc.loc)
							ok = err == nil
						}
						v = _changeLoc(v, e.Location())
						if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
							t.Fatalf("%s failed: [%s]\nexpected %#v/%#v/%#v(%T)\nbut received %#v/%#v/%#v(%T) (Ok: %#v)", testName,
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
							t, err := time.ParseInLocation(time.RFC3339, row[f].(string), sqlc.loc)
							v = _changeLoc(t, sqlc.loc)
							ok = err == nil
						}
						if conn.flavor != FlavorOracle && conn.flavor != FlavorSqlite {
							v = _changeLoc(v, e.Location())
						}
						if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
							t.Fatalf("%s failed: [%s]\nexpected %#v/%#v/%#v(%T)\nbut received %#v/%#v/%#v(%T) (Ok: %#v)", testName,
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
							t, err := time.ParseInLocation(time.RFC3339, row[f].(string), sqlc.loc)
							v = _changeLoc(t, sqlc.loc)
							ok = err == nil
						}
						if conn.flavor != FlavorOracle && conn.flavor != FlavorSqlite {
							v = _changeLoc(v, e.Location())
						}
						if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
							t.Fatalf("%s failed: [%s]\nexpected %#v/%#v/%#v(%T)\nbut received %#v/%#v/%#v(%T) (Ok: %#v)", testName,
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
							t, err := time.ParseInLocation(time.RFC3339, row[f].(string), sqlc.loc)
							v = t.In(sqlc.loc)
							ok = err == nil
						}
						if conn.flavor == FlavorMySql {
							// currently, the Go driver treats parseTime=false for "TIME" column
							v = _changeLoc(v, e.Location())
						}
						if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
							t.Fatalf("%s failed: [%s]\nexpected %#v/%#v/%#v(%T)\nbut received %#v/%#v/%#v(%T) (Ok: %#v)", testName,
								"idx:"+strconv.Itoa(idx)+"/row:"+row["id"].(string)+"/field:"+f, estr, e.Format(time.RFC3339), e.In(LOC2).Format(time.RFC3339), e,
								vstr, v.Format(time.RFC3339), v.In(LOC2).Format(time.RFC3339), row[f], ok)
						}
					}
				}
			}
		})
	}
}
