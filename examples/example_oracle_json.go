package main

import (
	"encoding/json"
	"fmt"
	"github.com/btnguyen2k/prom"
	_ "gopkg.in/goracle.v2"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// construct an 'prom.SqlConnect' instance
func createSqlConnectOracleJson() *prom.SqlConnect {
	driver := "goracle"
	dsn := "test/test@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=localhost)(PORT=1521)))(CONNECT_DATA=(SID=ORCLCDB)))"
	sqlConnect, err := prom.NewSqlConnectWithFlavor(driver, dsn, 10000, nil, prom.FlavorOracle)
	if sqlConnect == nil || err != nil {
		if err != nil {
			fmt.Println("Error:", err)
		}
		if sqlConnect == nil {
			panic("error creating [prom.SqlConnect] instance")
		}
	}
	return sqlConnect
}

var colsOracleJson = []string{"id", "data_map", "data_list"}

func printRowOracleJson(row map[string]interface{}) {
	id := row["ID"]
	fmt.Printf("\t\tRow [%v]\n", id)
	for _, n := range colsOracleJson {
		v := row[strings.ToUpper(n)]
		if reflect.TypeOf(v).String() == "[]uint8" {
			fmt.Println("\t\t\t", n, "[", reflect.TypeOf(v), "] = ", string(v.([]byte)))
		} else {
			fmt.Println("\t\t\t", n, "[", reflect.TypeOf(v), "] = ", v)
		}
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	SEP := "======================================================================"
	sqlConnect := createSqlConnectOracleJson()
	defer sqlConnect.Close()

	{
		fmt.Println("-== Database & Ping info ==-")

		// get the database object and send ping command
		db := sqlConnect.GetDB()
		fmt.Println("\tCurrent database:", db)
		fmt.Println("\tIs connected    :", sqlConnect.IsConnected())
		err := sqlConnect.Ping(nil)
		if err != nil {
			fmt.Println("\tPing error      :", err)
		} else {
			fmt.Println("\tPing ok")
		}

		fmt.Println(SEP)
	}

	{
		fmt.Println("-== Drop/Create Table ==-")

		// setting up
		sql := "DROP TABLE tbl_demojson"
		_, err := sqlConnect.GetDB().Exec(sql)
		if err != nil {
			fmt.Printf("\tError while executing query [%s]: %s\n", sql, err)
		}
		fmt.Println("\tDropped table [tbl_demojson]")

		types := []string{"INT", "CLOB", "CLOB"}

		sql = "CREATE TABLE tbl_demojson ("
		for i := range colsOracleJson {
			sql += colsOracleJson[i] + " " + types[i] + ","
		}
		sql += "PRIMARY KEY(id))"
		fmt.Println("\tQuery:" + sql)

		_, err = sqlConnect.GetDB().Exec(sql)
		if err != nil {
			fmt.Printf("\tError while executing query: %s\n", err)
		} else {
			fmt.Println("\tCreated table [tbl_demojson]")
		}

		fmt.Println(SEP)
	}

	{
		fmt.Println("-== Insert Rows to Table ==-")
		loc, _ := time.LoadLocation("Asia/Ho_Chi_Minh")

		// insert some rows
		sql := "INSERT INTO tbl_demojson ("
		sql += strings.Join(colsOracleJson, ",")
		sql += ") VALUES ("
		for k := range colsOracleJson {
			sql += ":" + strconv.Itoa(k+1) + ","
		}
		sql = sql[0 : len(sql)-1]
		sql += ")"

		n := 100
		fmt.Printf("\tInserting %d rows to table [tbl_demojson]\n", n)
		for i := 1; i <= n; i++ {
			t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
			id := i
			username := t.String()
			email := strconv.Itoa(int(rand.Int31n(int32(n)))) + "@" + strconv.Itoa(int(rand.Int31n(9999))) + ".com"
			dataInt := rand.Int31()
			dataBool := strconv.Itoa(int(dataInt % 2))
			dataFloat := rand.Float64()
			dataDatetime := t
			dataMap := map[string]interface{}{"username": username, "email": email, "int": dataInt, "bool": dataBool, "float": dataFloat, "datetime": dataDatetime}
			dataList := []interface{}{username, email, dataInt, dataBool, dataFloat, dataDatetime}
			val1, err := json.Marshal(dataMap)
			if err != nil {
				fmt.Println("\t\tError:", err)
			}
			val2, err := json.Marshal(dataList)
			if err != nil {
				fmt.Println("\t\tError:", err)
			}
			_, err = sqlConnect.GetDB().Exec(sql, id, string(val1), string(val2))
			if err != nil {
				fmt.Println("\t\tError:", err)
			}
		}

		fmt.Println(SEP)
	}

	{
		fmt.Println("-== Query Single Row from Table ==-")

		// query single row
		sql := "SELECT * FROM tbl_demojson WHERE id=:1"

		id := rand.Intn(100) + 1
		fmt.Printf("\tFetching row id %d from table [tbl_demojson]\n", id)
		dbRow := sqlConnect.GetDB().QueryRow(sql, id)
		data, err := sqlConnect.FetchRow(dbRow, len(colsOracleJson))
		if err != nil {
			fmt.Printf("\tError fetching row %d from table [tbl_demojson]: %e\n", id, err)
		} else if data == nil {
			fmt.Println("\t\tRow not found")
		} else {
			for _, v := range data {
				switch v.(type) {
				case []byte:
					fmt.Println("\t\t", reflect.TypeOf(v), string(v.([]byte)))
				default:
					fmt.Println("\t\t", reflect.TypeOf(v), v)
				}
			}
		}

		id = 999
		fmt.Printf("\tFetching row id %d from table [tbl_demojson]\n", id)
		dbRow = sqlConnect.GetDB().QueryRow(sql, id)
		data, err = sqlConnect.FetchRow(dbRow, len(colsOracleJson))
		if err != nil {
			fmt.Printf("\tError fetching row %d from table [tbl_demojson]: %e\n", id, err)
		} else if data == nil {
			fmt.Println("\t\tNo row matches query")
		} else {
			for _, v := range data {
				switch v.(type) {
				case []byte:
					fmt.Println("\t\t", reflect.TypeOf(v), string(v.([]byte)))
				default:
					fmt.Println("\t\t", reflect.TypeOf(v), v)
				}
			}
		}

		fmt.Println(SEP)
	}

	{
		fmt.Println("-== Query Multiple Rows from Table ==-")

		sql := "SELECT * FROM tbl_demojson WHERE id>=:1 ORDER BY id OFFSET 0 ROWS FETCH NEXT 4 ROWS ONLY"

		id := rand.Intn(100) + 1
		fmt.Printf("\tFetching rows starting at %d from table [tbl_demojson]\n", id)
		dbRows1, err := sqlConnect.GetDB().Query(sql, id)
		defer dbRows1.Close()
		if err != nil {
			fmt.Printf("\tE\trror while executing query: %e\n", err)
		} else {
			rows, err := sqlConnect.FetchRows(dbRows1)
			if err != nil {
				fmt.Printf("\t\tError while fetching rows from table [tbl_demojson]: %e\n", err)
			} else if len(rows) > 0 {
				for _, r := range rows {
					printRowOracleJson(r)
				}
			} else {
				fmt.Println("\t\tNo row matches query")
			}
		}

		id = 999
		fmt.Printf("\tFetching rows starting at %d from table [tbl_demojson]\n", id)
		dbRows2, err := sqlConnect.GetDB().Query(sql, id)
		defer dbRows2.Close()
		if err != nil {
			fmt.Printf("\t\tError while executing query: %e\n", err)
		} else {
			rows, err := sqlConnect.FetchRows(dbRows2)
			if err != nil {
				fmt.Printf("\t\tError while fetching rows from table [tbl_demojson]: %e\n", err)
			} else if len(rows) > 0 {
				for _, r := range rows {
					printRowOracleJson(r)
				}
			} else {
				fmt.Println("\t\tNo row matches query")
			}
		}

		fmt.Println(SEP)
	}

	{
		fmt.Println("-== Query Multiple Rows from Table (using callback) ==-")

		// query multiple rows with callback function
		sql := "SELECT * FROM tbl_demojson WHERE id>=:1 ORDER BY id OFFSET 0 ROWS FETCH NEXT 4 ROWS ONLY"
		callback := func(row map[string]interface{}, err error) bool {
			if err != nil {
				fmt.Printf("\t\tError while fetching rows from table [tbl_demojson]: %e\n", err)
			} else {
				printRowOracleJson(row)
			}
			return true
		}

		id := rand.Intn(100) + 1
		fmt.Printf("\tFetching rows starting at %d from table [tbl_demojson]\n", id)
		dbRows1, err := sqlConnect.GetDB().Query(sql, id)
		defer dbRows1.Close()
		if err != nil {
			fmt.Printf("\t\tError while executing query: %e\n", err)
		} else {
			sqlConnect.FetchRowsCallback(dbRows1, callback)
		}

		id = 999
		fmt.Printf("\tFetching rows starting at %d from table [tbl_demojson]\n", id)
		dbRows2, err := sqlConnect.GetDB().Query(sql, id)
		defer dbRows2.Close()
		if err != nil {
			fmt.Printf("\t\tError while executing query: %e\n", err)
		} else {
			sqlConnect.FetchRowsCallback(dbRows2, callback)
		}

		fmt.Println(SEP)
	}
}
