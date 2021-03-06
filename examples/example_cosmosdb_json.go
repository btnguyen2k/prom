package main

import (
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	_ "github.com/btnguyen2k/gocosmos"

	"github.com/btnguyen2k/prom"
)

// construct an 'prom.SqlConnect' instance
func createSqlConnectCosmosJson() *prom.SqlConnect {
	driver := "gocosmos"
	dsn := "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==;Db=prom"
	if os.Getenv("COSMOSDB_URL") != "" {
		dsn = strings.ReplaceAll(os.Getenv("COSMOSDB_URL"), `"`, "")
	}
	sqlConnect, err := prom.NewSqlConnectWithFlavor(driver, dsn, 10000, nil, prom.FlavorCosmosDb)
	if sqlConnect == nil || err != nil {
		if err != nil {
			fmt.Println("Error:", err)
		}
		if sqlConnect == nil {
			panic("error creating [prom.SqlConnect] instance")
		}
	}
	sqlConnect.GetDB().Exec("CREATE DATABASE IF NOT EXISTS prom")
	return sqlConnect
}

var colsCosmosJson = []string{"id", "data_map", "data_list"}

func printRowCosmosJson(row map[string]interface{}) {
	id := row["id"]
	fmt.Printf("\t\tRow [%v]\n", id)
	for _, n := range colsCosmosJson {
		v := row[n]
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
	sqlConnect := createSqlConnectCosmosJson()
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
		sql := "DROP TABLE IF EXISTS tbl_demojson"
		_, err := sqlConnect.GetDB().Exec(sql)
		if err != nil {
			fmt.Printf("\tError while executing query [%s]: %s\n", sql, err)
		} else {
			fmt.Println("\tDropped table [tbl_demojson]")

			sql := "CREATE COLLECTION tbl_demojson WITH pk=/id WITH maxru=10000"
			fmt.Println("\tQuery:" + sql)

			_, err := sqlConnect.GetDB().Exec(sql)
			if err != nil {
				fmt.Printf("\tError while executing query: %s\n", err)
			} else {
				fmt.Println("\tCreated table [tbl_demojson]")
			}
		}

		fmt.Println(SEP)
	}

	{
		fmt.Println("-== Insert Rows to Table ==-")
		loc, _ := time.LoadLocation("Asia/Ho_Chi_Minh")

		// insert some rows
		sql := "INSERT INTO tbl_demojson ("
		sql += strings.Join(colsCosmosJson, ",")
		sql += ") VALUES ("
		for k := range colsCosmosJson {
			sql += "$" + strconv.Itoa(k+1) + ","
		}
		sql = sql[0 : len(sql)-1]
		sql += ")"

		n := 100
		fmt.Printf("\tInserting %d rows to table [tbl_demojson]\n", n)
		for i := 1; i <= n; i++ {
			t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
			id := fmt.Sprintf("%03d", i)
			username := t.String()
			email := strconv.Itoa(int(rand.Int31n(int32(n)))) + "@" + strconv.Itoa(int(rand.Int31n(9999))) + ".com"
			dataInt := rand.Int31()
			dataBool := strconv.Itoa(int(dataInt % 2))
			dataFloat := rand.Float64()
			dataDatetime := t
			dataMap := map[string]interface{}{"username": username, "email": email, "int": dataInt, "bool": dataBool, "float": dataFloat, "datetime": dataDatetime}
			dataList := []interface{}{username, email, dataInt, dataBool, dataFloat, dataDatetime}
			_, err := sqlConnect.GetDB().Exec(sql, id, dataMap, dataList, id)
			if err != nil {
				fmt.Println("\t\tError:", err)
			}
		}

		fmt.Println(SEP)
	}

	{
		fmt.Println("-== Query Single Row from Table ==-")

		// query single row
		colList := "t." + colsCosmosJson[0]
		for i := 1; i < len(colsCosmosJson); i++ {
			colList += ",t." + colsCosmosJson[i]
		}
		sql := "SELECT CROSS PARTITION %s FROM tbl_demojson t WHERE t.id=$1"
		sql = fmt.Sprintf(sql, colList)
		id := fmt.Sprintf("%03d", rand.Intn(100)+1)
		fmt.Printf("\tFetching row id %d from table [tbl_demojson]\n", id)
		dbRow := sqlConnect.GetDB().QueryRow(sql, id)
		data, err := sqlConnect.FetchRow(dbRow, len(colsCosmosJson))
		if err != nil {
			fmt.Printf("\tError fetching row %d from table [tbl_demojson]: %s\n", id, err)
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

		id = "999"
		fmt.Printf("\tFetching row id %d from table [tbl_demojson]\n", id)
		dbRow = sqlConnect.GetDB().QueryRow(sql, id)
		data, err = sqlConnect.FetchRow(dbRow, len(colsCosmosJson))
		if err != nil {
			fmt.Printf("\tError fetching row %d from table [tbl_demojson]: %s\n", id, err)
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

		// query multiple rows
		colList := "t." + colsCosmosJson[0]
		for i := 1; i < len(colsCosmosJson); i++ {
			colList += ",t." + colsCosmosJson[i]
		}
		sql := "SELECT CROSS PARTITION %s FROM tbl_demojson t WHERE t.id>$1"
		sql = fmt.Sprintf(sql, colList)
		id := fmt.Sprintf("%03d", rand.Intn(100)+1)
		fmt.Printf("\tFetching rows starting at %d from table [tbl_demojson]\n", id)
		dbRows1, err := sqlConnect.GetDB().Query(sql, id)
		defer dbRows1.Close()
		if err != nil {
			fmt.Printf("\tE\trror while executing query: %s\n", err)
		} else {
			rows, err := sqlConnect.FetchRows(dbRows1)
			if err != nil {
				fmt.Printf("\t\tError while fetching rows from table [tbl_demojson]: %s\n", err)
			} else if len(rows) > 0 {
				for i, r := range rows {
					printRowCosmosJson(r)
					if i >= 3 {
						break
					}
				}
			} else {
				fmt.Println("\t\tNo row matches query")
			}
		}

		id = "999"
		fmt.Printf("\tFetching rows starting at %d from table [tbl_demojson]\n", id)
		dbRows2, err := sqlConnect.GetDB().Query(sql, id)
		defer dbRows2.Close()
		if err != nil {
			fmt.Printf("\t\tError while executing query: %s\n", err)
		} else {
			rows, err := sqlConnect.FetchRows(dbRows2)
			if err != nil {
				fmt.Printf("\t\tError while fetching rows from table [tbl_demojson]: %s\n", err)
			} else if len(rows) > 0 {
				for _, r := range rows {
					printRowCosmosJson(r)
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
		colList := "t." + colsCosmosJson[0]
		for i := 1; i < len(colsCosmosJson); i++ {
			colList += ",t." + colsCosmosJson[i]
		}
		sql := "SELECT CROSS PARTITION %s FROM tbl_demojson t WHERE t.id>$1"
		sql = fmt.Sprintf(sql, colList)
		id := fmt.Sprintf("%03d", rand.Intn(100)+1)
		count := 0
		callback := func(row map[string]interface{}, err error) bool {
			if err != nil {
				fmt.Printf("\t\tError while fetching rows from table [tbl_demojson]: %s\n", err)
			} else {
				printRowCosmosJson(row)
			}
			count++
			if count > 3 {
				return false
			}
			return true
		}

		fmt.Printf("\tFetching rows starting at %d from table [tbl_demojson]\n", id)
		dbRows1, err := sqlConnect.GetDB().Query(sql, id)
		defer dbRows1.Close()
		if err != nil {
			fmt.Printf("\t\tError while executing query: %s\n", err)
		} else {
			sqlConnect.FetchRowsCallback(dbRows1, callback)
		}

		id = "999"
		fmt.Printf("\tFetching rows starting at %d from table [tbl_demojson]\n", id)
		dbRows2, err := sqlConnect.GetDB().Query(sql, id)
		defer dbRows2.Close()
		if err != nil {
			fmt.Printf("\t\tError while executing query: %s\n", err)
		} else {
			sqlConnect.FetchRowsCallback(dbRows2, callback)
		}

		fmt.Println(SEP)
	}
}
