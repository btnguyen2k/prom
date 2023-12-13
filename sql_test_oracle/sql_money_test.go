package sql_test

import (
	"fmt"
	promsql "github.com/btnguyen2k/prom/sql"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"
)

var sqlColNamesTestDataTypeMoney = []string{"id",
	"data_money2", "data_money4", "data_money6", "data_money8"}

func TestSql_DataTypeMoney(t *testing.T) {
	testName := "TestSql_DataTypeMoney"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	rand.Seed(time.Now().UnixNano())
	tblName := "test_money"
	colNameList := sqlColNamesTestDataTypeMoney
	colTypesMap := map[promsql.DbFlavor][]string{
		promsql.FlavorCosmosDb: nil,
		promsql.FlavorMsSql:    {"NVARCHAR(8)", "DEC(36,2)", "MONEY", "DECIMAL(36,6)", "NUMERIC(36,8)"},
		promsql.FlavorMySql:    {"VARCHAR(8)", "DECIMAL(24,2)", "NUMERIC(28,4)", "DEC(32,6)", "NUMERIC(36,8)"},
		promsql.FlavorOracle:   {"NVARCHAR2(8)", "NUMERIC(24,2)", "DECIMAL(28,4)", "DEC(32,6)", "NUMERIC(36,8)"},
		promsql.FlavorPgSql:    {"VARCHAR(8)", "MONEY", "NUMERIC(28,4)", "DECIMAL(32,6)", "DEC(36,8)"},
		promsql.FlavorSqlite:   {"VARCHAR(8)", "DECIMAL(24,2)", "NUMERIC(28,4)", "DEC(32,6)", "NUMERIC(36,8)"},
	}
	type Row struct {
		id         string
		dataMoney2 float64
		dataMoney4 float64
		dataMoney6 float64
		dataMoney8 float64
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		colTypes := colTypesMap[sqlc.GetDbFlavor()]
		t.Run(dbtype, func(t *testing.T) {
			// init table
			sqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE %s", tblName))
			if sqlc.GetDbFlavor() == promsql.FlavorCosmosDb {
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
				if sqlc.GetDbFlavor() == promsql.FlavorCosmosDb {
					params = append(params, row.id)
				}
				_, err := sqlc.GetDB().Exec(sql, params...)
				if err != nil {
					t.Fatalf("%s failed: %s", testName, err)
				}
			}

			// query some rows
			sql = "SELECT * FROM %s ORDER BY id"
			if sqlc.GetDbFlavor() == promsql.FlavorCosmosDb {
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
					e := expected.dataMoney2
					f := colNameList[1]
					v, err := _toFloatIfReal(row[f])
					if sqlc.GetDbFlavor() == promsql.FlavorCosmosDb {
						v, err = _toFloatIfNumber(row[f])
					}
					if estr, vstr := fmt.Sprintf("%.2f", e), fmt.Sprintf("%.2f", v); err != nil || vstr != estr {
						t.Fatalf("%s failed: [%s] expected %#v/%.10f(%T) but received %#v/%.10f(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, e, vstr, v, row[f], err)
					}
				}
				{
					e := expected.dataMoney4
					f := colNameList[2]
					v, err := _toFloatIfReal(row[f])
					if sqlc.GetDbFlavor() == promsql.FlavorCosmosDb {
						v, err = _toFloatIfNumber(row[f])
					}
					if estr, vstr := fmt.Sprintf("%.4f", e), fmt.Sprintf("%.4f", v); err != nil || vstr != estr {
						t.Fatalf("%s failed: [%s] expected %#v/%.10f(%T) but received %#v/%.10f(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, e, vstr, v, row[f], err)
					}
				}
				{
					e := expected.dataMoney6
					f := colNameList[3]
					v, err := _toFloatIfReal(row[f])
					if sqlc.GetDbFlavor() == promsql.FlavorCosmosDb {
						v, err = _toFloatIfNumber(row[f])
					}
					if estr, vstr := fmt.Sprintf("%.6f", e), fmt.Sprintf("%.6f", v); err != nil || vstr != estr {
						fmt.Printf("\tDEBUG: Row %#v(%.10f) / Expected %#v(%.10f)\n", e, e, row[f], row[f])
						t.Fatalf("%s failed: [%s] expected %#v/%.10f(%T) but received %#v/%.10f(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, e, vstr, v, row[f], err)
					}
				}
				{
					e := expected.dataMoney8
					f := colNameList[4]
					v, err := _toFloatIfReal(row[f])
					if sqlc.GetDbFlavor() == promsql.FlavorCosmosDb {
						v, err = _toFloatIfNumber(row[f])
					}
					if estr, vstr := fmt.Sprintf("%.8f", e), fmt.Sprintf("%.8f", v); err != nil || vstr != estr {
						fmt.Printf("\tDEBUG: Row %#v / Expected %#v\n", row[f], e)
						t.Fatalf("%s failed: [%s] expected %#v/%.10f(%T) but received %#v/%.10f(%T) (error: %s)", testName, row["id"].(string)+"/"+f, estr, e, e, vstr, v, row[f], err)
					}
				}
			}
		})
	}
}
