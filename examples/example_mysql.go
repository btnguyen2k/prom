package main

import (
	"fmt"
	"github.com/btnguyen2k/prom"
	_ "github.com/go-sql-driver/mysql"
	"math/rand"
	"reflect"
	"time"
)

// construct an 'prom.SqlConnect' instance
func createSqlConnectForMySql() *prom.SqlConnect {
	driver := "mysql"
	dsn := "test:test@tcp(localhost:3306)/test"
	sqlConnect, err := prom.NewSqlConnect(driver, dsn, 10000, nil)
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

func main() {
	rand.Seed(time.Now().UnixNano())

	sqlConnect := createSqlConnectForMySql()
	defer sqlConnect.Close()

	{
		// get the database object and send ping command
		db := sqlConnect.GetDB()
		fmt.Println("Current database:", db)
		fmt.Println("Is connected    :", sqlConnect.IsConnected())
		err := sqlConnect.Ping(nil)
		if err != nil {
			fmt.Println("Ping error      :", err)
		} else {
			fmt.Println("Ping ok")
		}

		fmt.Println("==================================================")
	}

	{
		// setting up
		sql := "DROP TABLE IF EXISTS tbl_demo"
		_, err := sqlConnect.GetDB().Exec(sql)
		if err != nil {
			fmt.Printf("Error while executing query [%s]: %e\n", sql, err)
		} else {
			fmt.Println("Dropped table [tbl_demo]")

			sql := "CREATE TABLE tbl_demo (id INT, name TEXT, PRIMARY KEY(id))"
			_, err := sqlConnect.GetDB().Exec(sql)
			if err != nil {
				fmt.Printf("Error while executing query [%s]: %e\n", sql, err)
			} else {
				fmt.Println("Created table [tbl_demo]")
			}
		}

		fmt.Println("==================================================")
	}

	{
		// insert some rows
		sql := "INSERT INTO tbl_demo (id, name) VALUES (?, ?)"
		n := 100
		fmt.Printf("Inserting %d rows to table [tbl_demo]\n", n)
		for i := 1; i <= n; i++ {
			id := i
			name := time.Unix(int64(rand.Int31()), rand.Int63())
			sqlConnect.GetDB().Exec(sql, id, name.String())
		}

		fmt.Println("==================================================")
	}

	{
		// query single row
		sql := "SELECT name,id FROM tbl_demo WHERE id=?"

		id := rand.Intn(100) + 1
		fmt.Printf("Fetching row id %d from table [tbl_demo]\n", id)
		dbRow := sqlConnect.GetDB().QueryRow(sql, id)
		data, err := sqlConnect.FetchRow(dbRow, 2)
		if err != nil {
			fmt.Printf("Error fetching row %d from table [tbl_demo]: %e\n", id, err)
		} else if data == nil {
			fmt.Println("\tRow not found")
		} else {
			vId := data[1].(int64)
			vName := string(data[0].([]byte))
			fmt.Printf("\tRow: id[%v / %v],name[%v / %v]\n", vId, reflect.TypeOf(vId), vName, reflect.TypeOf(vName))
		}

		id = 999
		fmt.Printf("Fetching row id %d from table [tbl_demo]\n", id)
		dbRow = sqlConnect.GetDB().QueryRow(sql, id)
		data, err = sqlConnect.FetchRow(dbRow, 2)
		if err != nil {
			fmt.Printf("Error fetching row %d from table [tbl_demo]: %e\n", id, err)
		} else if data == nil {
			fmt.Println("\tNo row matches query")
		} else {
			vId := data[1].(int64)
			vName := string(data[0].([]byte))
			fmt.Printf("\tRow: id[%v / %v],name[%v / %v]\n", vId, reflect.TypeOf(vId), vName, reflect.TypeOf(vName))
		}

		fmt.Println("==================================================")
	}

	{
		// query multiple rows
		sql := "SELECT name AS 'MyName',id AS 'my-id' FROM tbl_demo WHERE id>=? LIMIT 4"

		id := rand.Intn(100) + 1
		fmt.Printf("Fetching rows starting at %d from table [tbl_demo]\n", id)
		dbRows1, err := sqlConnect.GetDB().Query(sql, id)
		defer dbRows1.Close()
		if err != nil {
			fmt.Printf("\tError while executing query: %e\n", err)
		} else {
			rows, err := sqlConnect.FetchRows(dbRows1)
			if err != nil {
				fmt.Printf("\tError while fetching rows from table [tbl_demo]: %e\n", err)
			} else if len(rows) > 0 {
				fmt.Println("\tRows:", rows)
			} else {
				fmt.Println("\tNo row matches query")
			}
		}

		id = 999
		fmt.Printf("Fetching rows starting at %d from table [tbl_demo]\n", id)
		dbRows2, err := sqlConnect.GetDB().Query(sql, id)
		defer dbRows2.Close()
		if err != nil {
			fmt.Printf("\tError while executing query: %e\n", err)
		} else {
			rows, err := sqlConnect.FetchRows(dbRows2)
			if err != nil {
				fmt.Printf("\tError while fetching rows from table [tbl_demo]: %e\n", err)
			} else if len(rows) > 0 {
				fmt.Println("\tRows:", rows)
			} else {
				fmt.Println("\tNo row matches query")
			}
		}

		fmt.Println("==================================================")
	}

	{
		// query multiple rows with callback function
		sql := "SELECT id,name FROM tbl_demo WHERE id>=? LIMIT 4"

		id := rand.Intn(100) + 1
		fmt.Printf("Fetching rows starting at %d from table [tbl_demo]\n", id)
		dbRows1, err := sqlConnect.GetDB().Query(sql, id)
		defer dbRows1.Close()
		if err != nil {
			fmt.Printf("\tError while executing query: %e\n", err)
		} else {
			sqlConnect.FetchRowsCallback(dbRows1, func(row map[string]interface{}, err error) bool {
				if err != nil {
					fmt.Printf("\tError while fetching rows from table [tbl_demo]: %e\n", err)
				} else {
					vId := row["id"]
					vName := string(row["name"].([]byte))
					fmt.Printf("\tRow: id[%v / %v],name[%v / %v]\n", vId, reflect.TypeOf(vId), vName, reflect.TypeOf(vName))
				}
				return true
			})
		}

		id = 999
		fmt.Printf("Fetching rows starting at %d from table [tbl_demo]\n", id)
		dbRows2, err := sqlConnect.GetDB().Query(sql, id)
		defer dbRows2.Close()
		if err != nil {
			fmt.Printf("\tError while executing query: %e\n", err)
		} else {
			sqlConnect.FetchRowsCallback(dbRows2, func(row map[string]interface{}, err error) bool {
				if err != nil {
					fmt.Printf("\tError while fetching rows from table [tbl_demo]: %e\n", err)
				} else {
					vId := row["id"]
					vName := string(row["name"].([]byte))
					fmt.Printf("\tRow: id[%v / %v],name[%v / %v]\n", vId, reflect.TypeOf(vId), vName, reflect.TypeOf(vName))
				}
				return true
			})
		}

		fmt.Println("==================================================")
	}
}
