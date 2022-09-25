package sql

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

func TestSql_DataTypeReal(t *testing.T) {
	testName := "TestSql_DataTypeReal"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	rand.Seed(time.Now().UnixNano())
	tblName := "test_real"
	colNameList := sqlColNamesTestDataTypeReal
	colTypesMap := map[DbFlavor][]string{
		FlavorCosmosDb: nil,
		FlavorMsSql: {"NVARCHAR(8)",
			"FLOAT(32)", "DOUBLE PRECISION", "REAL",
			"DECIMAL(38,6)", "DEC(38,6)", "NUMERIC(38,6)",
			"FLOAT(16)", "FLOAT(32)",
			"FLOAT(20)", "FLOAT(40)"},
		FlavorMySql: {"VARCHAR(8)",
			"FLOAT(32)", "DOUBLE(38,6)", "REAL",
			"DECIMAL(38,6)", "DEC(38,6)", "NUMERIC(38,6)",
			"FLOAT(16,4)", "DOUBLE(32,8)",
			"FLOAT(20,4)", "DOUBLE(40,8)"},
		FlavorOracle: {"NVARCHAR2(8)",
			"FLOAT", "DOUBLE PRECISION", "REAL",
			"DECIMAL(38,6)", "NUMBER(38,6)", "NUMERIC(38,6)",
			"BINARY_FLOAT", "BINARY_DOUBLE",
			"BINARY_FLOAT", "BINARY_DOUBLE"},
		FlavorPgSql: {"VARCHAR(8)",
			"FLOAT", "DOUBLE PRECISION", "REAL",
			"DECIMAL(38,6)", "NUMERIC(38,6)", "NUMERIC(38,6)",
			"FLOAT4", "FLOAT8",
			"FLOAT4", "FLOAT8"},
		FlavorSqlite: {"VARCHAR(8)",
			"FLOAT", "DOUBLE", "REAL",
			"DECIMAL(38,6)", "NUMBER(38,6)", "NUMERIC(38,6)",
			"FLOAT", "DOUBLE PRECISION",
			"FLOAT4", "FLOAT8"},
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
					e := expected.dataFloat
					f := colNameList[1]
					v, err := _toFloatIfReal(row[f])
					if sqlc.flavor == FlavorCosmosDb {
						v, err = _toFloatIfNumber(row[f])
					}
					if estr, vstr := fmt.Sprintf("%.f", e), fmt.Sprintf("%.f", v); err != nil || vstr != estr {
						t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
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
						t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
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
						t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
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
						t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
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
						t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
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
						t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
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
						t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
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
						t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
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
						t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
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
						t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
					}
				}
			}
		})
	}
}

func TestSql_DataTypeRealZero(t *testing.T) {
	testName := "TestSql_DataTypeRealZero"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	rand.Seed(time.Now().UnixNano())
	tblName := "test_realzero"
	colNameList := sqlColNamesTestDataTypeReal
	colTypesMap := map[DbFlavor][]string{
		FlavorCosmosDb: nil,
		FlavorMsSql: {"NVARCHAR(8)",
			"FLOAT(32)", "DOUBLE PRECISION", "REAL",
			"DECIMAL(38,6)", "DEC(38,6)", "NUMERIC(38,6)",
			"FLOAT(16)", "FLOAT(32)",
			"FLOAT(20)", "FLOAT(40)"},
		FlavorMySql: {"VARCHAR(8)",
			"FLOAT(32)", "DOUBLE(38,6)", "REAL",
			"DECIMAL(38,6)", "DEC(38,6)", "NUMERIC(38,6)",
			"FLOAT(16,4)", "DOUBLE(32,8)",
			"FLOAT(20,4)", "DOUBLE(40,8)"},
		FlavorOracle: {"NVARCHAR2(8)",
			"FLOAT", "DOUBLE PRECISION", "REAL",
			"DECIMAL(38,6)", "NUMBER(38,6)", "NUMERIC(38,6)",
			"BINARY_FLOAT", "BINARY_DOUBLE",
			"BINARY_FLOAT", "BINARY_DOUBLE"},
		FlavorPgSql: {"VARCHAR(8)",
			"FLOAT", "DOUBLE PRECISION", "REAL",
			"DECIMAL(38,6)", "NUMERIC(38,6)", "NUMERIC(38,6)",
			"FLOAT4", "FLOAT8",
			"FLOAT4", "FLOAT8"},
		FlavorSqlite: {"VARCHAR(8)",
			"FLOAT", "DOUBLE", "REAL",
			"DECIMAL(38,6)", "NUMBER(38,6)", "NUMERIC(38,6)",
			"FLOAT", "DOUBLE PRECISION",
			"FLOAT4", "FLOAT8"},
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
					e := expected.dataFloat
					f := colNameList[1]
					v, err := _toFloatIfReal(row[f])
					if sqlc.flavor == FlavorCosmosDb {
						v, err = _toFloatIfNumber(row[f])
					}
					if estr, vstr := fmt.Sprintf("%.f", e), fmt.Sprintf("%.f", v); err != nil || vstr != estr {
						t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
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
						t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
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
						t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
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
						t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
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
						t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
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
						t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
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
						t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
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
						t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
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
						t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
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
						t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
					}
				}
			}
		})
	}
}
