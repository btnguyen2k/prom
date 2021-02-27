package prom

import (
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/btnguyen2k/gocosmos"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/godror/godror"
	_ "github.com/jackc/pgx/v4/stdlib"
	_ "github.com/mattn/go-sqlite3"
)

func newSqlConnectSqlite(driver, url, timezone string, timeoutMs int, poolOptions *SqlPoolOptions) (*SqlConnect, error) {
	os.Remove(url)
	sqlc, err := NewSqlConnectWithFlavor(driver, url, timeoutMs, poolOptions, FlavorSqlite)
	if err == nil && sqlc != nil {
		loc, _ := time.LoadLocation(timezone)
		sqlc.SetLocation(loc)
	}
	return sqlc, err
}

func newSqlConnectMssql(driver, url, timezone string, timeoutMs int, poolOptions *SqlPoolOptions) (*SqlConnect, error) {
	sqlc, err := NewSqlConnectWithFlavor(driver, url, timeoutMs, poolOptions, FlavorMsSql)
	if err == nil && sqlc != nil {
		loc, _ := time.LoadLocation(timezone)
		sqlc.SetLocation(loc)
	}
	return sqlc, err
}

func newSqlConnectMysql(driver, url, timezone string, timeoutMs int, poolOptions *SqlPoolOptions) (*SqlConnect, error) {
	urlTimezone := strings.ReplaceAll(timezone, "/", "%2f")
	url = strings.ReplaceAll(url, "${loc}", urlTimezone)
	url = strings.ReplaceAll(url, "${tz}", urlTimezone)
	url = strings.ReplaceAll(url, "${timezone}", urlTimezone)
	sqlc, err := NewSqlConnectWithFlavor(driver, url, timeoutMs, poolOptions, FlavorMySql)
	if err == nil && sqlc != nil {
		loc, _ := time.LoadLocation(timezone)
		sqlc.SetLocation(loc)
	}
	return sqlc, err
}

func newSqlConnectOracle(driver, url, timezone string, timeoutMs int, poolOptions *SqlPoolOptions) (*SqlConnect, error) {
	sqlc, err := NewSqlConnectWithFlavor(driver, url, timeoutMs, poolOptions, FlavorOracle)
	if err == nil && sqlc != nil {
		loc, _ := time.LoadLocation(timezone)
		sqlc.SetLocation(loc)
	}
	return sqlc, err
}

func newSqlConnectPgsql(driver, url, timezone string, timeoutMs int, poolOptions *SqlPoolOptions) (*SqlConnect, error) {
	sqlc, err := NewSqlConnectWithFlavor(driver, url, timeoutMs, poolOptions, FlavorPgSql)
	if err == nil && sqlc != nil {
		loc, _ := time.LoadLocation(timezone)
		sqlc.SetLocation(loc)
	}
	return sqlc, err
}

func newSqlConnectCosmosdb(driver, url, timezone string, timeoutMs int, poolOptions *SqlPoolOptions) (*SqlConnect, error) {
	url += ";Db=prom"
	sqlc, err := NewSqlConnectWithFlavor(driver, url, timeoutMs, poolOptions, FlavorCosmosDb)
	if err == nil && sqlc != nil {
		loc, _ := time.LoadLocation(timezone)
		sqlc.SetLocation(loc)
	}
	sqlc.GetDB().Exec("CREATE DATABASE prom WITH maxru=10000")
	return sqlc, err
}

const (
	timezoneSql = "Asia/Kabul"
)

func TestNewSqlConnect(t *testing.T) {
	name := "TestNewSqlConnect"
	driver := "mysql"
	dsn := "test:test@tcp(localhost:3306)/test?charset=utf8mb4,utf8&parseTime=false&loc="
	dsn += strings.ReplaceAll(timezoneSql, "/", "%2f")
	sqlc, err := NewSqlConnect(driver, dsn, 10000, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	}
	if sqlc == nil {
		t.Fatalf("%s failed: nil", name)
	}
}

func TestSqlConnect_GetInfo(t *testing.T) {
	name := "TestSqlConnect_GetInfo"

	type testInfo struct {
		driver, dsn string
		dbFlavor    DbFlavor
	}
	testDataMap := map[string]testInfo{
		"sqlite": {driver: "sqlite3", dsn: "./temp/temp.db", dbFlavor: FlavorSqlite},
		"mssql":  {driver: "sqlserver", dsn: "sqlserver://sa:secret@localhost:1433?database=tempdb", dbFlavor: FlavorMsSql},
		"mysql":  {driver: "mysql", dsn: "test:test@tcp(localhost:3306)/test?charset=utf8mb4,utf8&parseTime=false", dbFlavor: FlavorMySql},
		"oracle": {driver: "godror", dsn: "test/test@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=localhost)(PORT=1521)))(CONNECT_DATA=(SID=c)))", dbFlavor: FlavorOracle},
		"pgsql":  {driver: "pgx", dsn: "postgres://test:test@localhost:5432/test?sslmode=disable&client_encoding=UTF-8&application_name=prom", dbFlavor: FlavorPgSql},
		"cosmos": {driver: "gocosmos", dsn: "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==;Db=prom", dbFlavor: FlavorCosmosDb},
	}
	for k, info := range testDataMap {
		var sqlc *SqlConnect
		var err error
		switch k {
		case "sqlite", "sqlite3":
			sqlc, err = newSqlConnectSqlite(info.driver, info.dsn, timezoneSql, -1, nil)
		case "mssql":
			sqlc, err = newSqlConnectMssql(info.driver, info.dsn, timezoneSql, -1, nil)
		case "mysql":
			sqlc, err = newSqlConnectMysql(info.driver, info.dsn, timezoneSql, -1, nil)
		case "oracle":
			sqlc, err = newSqlConnectOracle(info.driver, info.dsn, timezoneSql, -1, nil)
		case "pgsql", "postgresql":
			sqlc, err = newSqlConnectPgsql(info.driver, info.dsn, timezoneSql, -1, nil)
		case "cosmos", "cosmosdb":
			sqlc, err = newSqlConnectCosmosdb(info.driver, info.dsn, timezoneSql, -1, nil)
		default:
			t.Fatalf("%s failed: unknown database type [%s]", name, k)
		}
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name, err)
		} else if sqlc == nil {
			t.Fatalf("%s failed: nil", name)
		}
		if sqlc.GetLocation() == nil {
			t.Fatalf("%s failed: GetLocation returns nil", name+"/"+k)
		}
		if sqlc.GetLocation().String() != timezoneSql {
			t.Fatalf("%s failed: expected timezone %#v but received %#v", name+"/"+k, timezoneSql, sqlc.GetLocation().String())
		}
		if sqlc.GetDbFlavor() != info.dbFlavor {
			t.Fatalf("%s failed: expected dbflavor %#v but received %#v", name+"/"+k, info.dbFlavor, sqlc.GetDbFlavor())
		}
		sqlc.SetDbFlavor(FlavorDefault)
		if sqlc.GetDbFlavor() != FlavorDefault {
			t.Fatalf("%s failed: expected dbflavor %#v but received %#v", name+"/"+k, FlavorDefault, sqlc.GetDbFlavor())
		}
	}
}

const (
	envSqliteDriver = "SQLITE_DRIVER"
	envSqliteUrl    = "SQLITE_URL"
	envMssqlDriver  = "MSSQL_DRIVER"
	envMssqlUrl     = "MSSQL_URL"
	envMysqlDriver  = "MYSQL_DRIVER"
	envMysqlUrl     = "MYSQL_URL"
	envOracleDriver = "ORACLE_DRIVER"
	envOracleUrl    = "ORACLE_URL"
	envPgsqlDriver  = "PGSQL_DRIVER"
	envPgsqlUrl     = "PGSQL_URL"
	envCosmosDriver = "COSMOSDB_DRIVER"
	envCosmosUrl    = "COSMOSDB_URL"
)

type sqlDriverAndUrl struct {
	driver, url string
}

func newSqlDriverAndUrl(driver, url string) sqlDriverAndUrl {
	return sqlDriverAndUrl{driver: strings.Trim(driver, `"`), url: strings.Trim(url, `"`)}
}

func sqlGetUrlFromEnv() map[string]sqlDriverAndUrl {
	urlMap := make(map[string]sqlDriverAndUrl)
	if os.Getenv(envSqliteDriver) != "" && os.Getenv(envSqliteUrl) != "" {
		urlMap["sqlite"] = newSqlDriverAndUrl(os.Getenv(envSqliteDriver), os.Getenv(envSqliteUrl))
	}
	if os.Getenv(envMssqlDriver) != "" && os.Getenv(envMssqlUrl) != "" {
		urlMap["mssql"] = newSqlDriverAndUrl(os.Getenv(envMssqlDriver), os.Getenv(envMssqlUrl))
	}
	if os.Getenv(envMysqlDriver) != "" && os.Getenv(envMysqlUrl) != "" {
		urlMap["mysql"] = newSqlDriverAndUrl(os.Getenv(envMysqlDriver), os.Getenv(envMysqlUrl))
	}
	if os.Getenv(envOracleDriver) != "" && os.Getenv(envOracleUrl) != "" {
		urlMap["oracle"] = newSqlDriverAndUrl(os.Getenv(envOracleDriver), os.Getenv(envOracleUrl))
	}
	if os.Getenv(envPgsqlDriver) != "" && os.Getenv(envPgsqlUrl) != "" {
		urlMap["pgsql"] = newSqlDriverAndUrl(os.Getenv(envPgsqlDriver), os.Getenv(envPgsqlUrl))
	}
	if os.Getenv(envCosmosDriver) != "" && os.Getenv(envCosmosUrl) != "" {
		urlMap["cosmosdb"] = newSqlDriverAndUrl(os.Getenv(envCosmosDriver), os.Getenv(envCosmosUrl))
	}
	return urlMap
}

func TestSqlConnect_Connection(t *testing.T) {
	name := "TestSqlConnect_Connection"
	urlMap := sqlGetUrlFromEnv()
	if len(urlMap) == 0 {
		t.Skipf("%s skipped", name)
	}
	for k, info := range urlMap {
		var sqlc *SqlConnect
		var err error
		switch k {
		case "sqlite", "sqlite3":
			sqlc, err = newSqlConnectSqlite(info.driver, info.url, timezoneSql, 10000, nil)
		case "mssql":
			sqlc, err = newSqlConnectMssql(info.driver, info.url, timezoneSql, 10000, nil)
		case "mysql":
			sqlc, err = newSqlConnectMysql(info.driver, info.url, timezoneSql, 10000, nil)
		case "oracle":
			sqlc, err = newSqlConnectOracle(info.driver, info.url, timezoneSql, 10000, nil)
		case "pgsql", "postgresql":
			sqlc, err = newSqlConnectPgsql(info.driver, info.url, timezoneSql, 10000, nil)
		case "cosmos", "cosmosdb":
			sqlc, err = newSqlConnectCosmosdb(info.driver, info.url, timezoneSql, 10000, nil)
		default:
			t.Fatalf("%s failed: unknown database type [%s]", name, k)
		}
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/"+k, err)
		} else if sqlc == nil {
			t.Fatalf("%s failed: nil", name+"/"+k)
		}
		if sqlc.GetDB() == nil {
			t.Fatalf("%s failed: GetDB returns nil", name+"/"+k)
		}
		if err = sqlc.Ping(nil); err != nil {
			t.Fatalf("%s failed: %s", name+"/Ping/"+k, err)
		}
		if !sqlc.IsConnected() {
			t.Fatalf("%s failed: not connected", name+"/"+k)
		}
		conn, err := sqlc.Conn(nil)
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/Conn/"+k, err)
		} else if conn.Close() != nil {
			t.Fatalf("%s failed: error [%s]", name+"/Conn.Close/"+k, err)
		}
		if err = sqlc.Close(); err != nil {
			t.Fatalf("%s failed: %s", name+"/Close/"+k, err)
		}
	}
}

const (
	testSqlTableName = "test_user"
)

var (
	// first column name is primary key
	sqlTableColNames = []string{
		"userid", "userid", "uname", "is_actived", "col_int", "col_real",
		"col_time", "col_date", "col_datetime", "col_timestamp"}
	sqlTableColTypes = map[string][]string{
		"sqlite": {"VARCHAR(16)", "VARCHAR(64)", "CHAR(1)", "INT", "DOUBLE", "TIME", "DATE", "DATETIME", "TIMESTAMP"},
		"mssql":  {"VARCHAR(16)", "NVARCHAR(64)", "CHAR(1)", "INT", "REAL", "TIME", "DATE", "DATETIME2", "DATETIMEOFFSET"},
		"mysql":  {"VARCHAR(16)", "VARCHAR(64)", "CHAR(1)", "INT", "DOUBLE", "TIME", "DATE", "DATETIME", "TIMESTAMP"},
		"oracle": {"NVARCHAR2(16)", "NVARCHAR2(64)", "NCHAR(1)", "INT", "BINARY_DOUBLE", "DATE", "DATE", "DATE", "TIMESTAMP WITH TIME ZONE"},
		"pgsql":  {"VARCHAR(16)", "VARCHAR(64)", "CHAR(1)", "INT", "DOUBLE PRECISION", "TIME", "DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE"},
	}
	yesNoMapping = map[bool]string{true: "Y", false: "N"}
)

func _generatePlaceholders(num int, dbtype string) string {
	result := ""
	for i := 1; i <= num; i++ {
		switch dbtype {
		case "mssql":
			result += "@p" + strconv.Itoa(i)
		case "oracle":
			result += ":" + strconv.Itoa(i)
		case "pgsql", "postgresql", "cosmos", "cosmosdb":
			result += "$" + strconv.Itoa(i)
		default:
			result += "?"
		}
		if i < num {
			result += ","
		}
	}
	return result
}

func sqlInitTable(sqlc *SqlConnect, table, dbtype string, insertSampleRows bool) error {
	if dbtype == "cosmos" || dbtype == "cosmosdb" {
		sqlc.GetDB().Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS prom"))
	}
	sqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE %s", table))
	sqlCreate := "CREATE TABLE %s (%s, PRIMARY KEY (%s))"
	sqlInsert := "INSERT INTO %s (%s) VALUES (%s)"
	partCreateCols := ""
	partInsertCols := ""
	partInsertValues := ""
	pkName := sqlTableColNames[0]

	for i, n := 1, len(sqlTableColNames); i < n; i++ {
		partInsertCols += sqlTableColNames[i]
		if i < n-1 {
			partInsertCols += ","
		}
	}
	partInsertValues = _generatePlaceholders(len(sqlTableColNames)-1, dbtype)

	switch dbtype {
	case "sqlite", "sqlite3", "mssql", "mysql", "oracle", "pgsql", "postgresql":
		for i, n := 1, len(sqlTableColNames); i < n; i++ {
			partCreateCols += sqlTableColNames[i] + " " + sqlTableColTypes[dbtype][i-1]
			if i < n-1 {
				partCreateCols += ","
			}
		}
	case "cosmos", "cosmosdb":
		sqlCreate = "CREATE COLLECTION %s WITH pk=/%s%s WITH maxru=10000"
		partCreateCols = ""
		pkName = sqlTableColNames[0]
	default:
		return fmt.Errorf("unknown database type %s", dbtype)
	}
	sqlCreate = fmt.Sprintf(sqlCreate, table, partCreateCols, pkName)
	if _, err := sqlc.GetDB().Exec(sqlCreate); err != nil {
		return err
	}

	if !insertSampleRows {
		return nil
	}
	sqlInsert = fmt.Sprintf(sqlInsert, table, partInsertCols, partInsertValues)
	for i := 0; i < 10; i++ {
		uid := strconv.Itoa(i)
		uname := "user" + strconv.Itoa(i)
		isActived := yesNoMapping[i%3 == 0]
		valInt := rand.Int() % time.Now().Year()
		valReal := math.Pow(math.Pi, float64(i+1))
		valTime := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(sqlc.GetLocation())
		if i%3 == 0 {
			valTime = time.Now().In(sqlc.GetLocation())
		}
		params := []interface{}{uid, uname, isActived, valInt, valReal, valTime, valTime, valTime, valTime}
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
			params = append(params, uid)
		}
		if _, err := sqlc.GetDB().Exec(sqlInsert, params...); err != nil {
			return err
		}
	}
	return nil
}

func TestSqlConnect_Unicode(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	name := "TestSqlConnect_Unicode"
	urlMap := sqlGetUrlFromEnv()
	for dbtype, info := range urlMap {
		var sqlc *SqlConnect
		var err error
		switch dbtype {
		case "sqlite", "sqlite3":
			sqlc, err = newSqlConnectSqlite(info.driver, info.url, timezoneSql, 10000, nil)
		case "mssql":
			sqlc, err = newSqlConnectMssql(info.driver, info.url, timezoneSql, 10000, nil)
		case "mysql":
			sqlc, err = newSqlConnectMysql(info.driver, info.url, timezoneSql, 10000, nil)
		case "oracle":
			sqlc, err = newSqlConnectOracle(info.driver, info.url, timezoneSql, 10000, nil)
		case "pgsql", "postgresql":
			sqlc, err = newSqlConnectPgsql(info.driver, info.url, timezoneSql, 10000, nil)
		case "cosmos", "cosmosdb":
			sqlc, err = newSqlConnectCosmosdb(info.driver, info.url, timezoneSql, 10000, nil)
		default:
			t.Fatalf("%s failed: unknown database type [%s]", name, dbtype)
		}
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/"+dbtype, err)
		} else if sqlc == nil {
			t.Fatalf("%s failed: nil", name+"/"+dbtype)
		}
		err = sqlInitTable(sqlc, testSqlTableName, dbtype, false)
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/sqlInitTable/"+dbtype, err)
		}

		strs := []string{"Xin chào, đây là thư viện prom", "您好", "مرحبا", "हैलो", "こんにちは", "សួស្តី", "여보세요", "ສະບາຍດີ", "สวัสดี"}
		for i, str := range strs {
			sqlInsert := "INSERT INTO %s (%s, %s) VALUES (%s)"
			placeholders := _generatePlaceholders(2, dbtype)
			sqlInsert = fmt.Sprintf(sqlInsert, testSqlTableName, sqlTableColNames[1], sqlTableColNames[2], placeholders)
			params := []interface{}{strconv.Itoa(i), str}
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
				params = append(params, strconv.Itoa(i))
			}
			_, err := sqlc.GetDB().Exec(sqlInsert, params...)
			if err != nil {
				t.Fatalf("%s failed: error [%s]", name+"/insert/"+dbtype, err)
			}

			placeholders = _generatePlaceholders(1, dbtype)
			sqlSelect := "SELECT %s FROM %s WHERE %s=%s"
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
				sqlSelect = "SELECT t.%s FROM %s t WHERE t.%s=%s"
			}
			sqlSelect = fmt.Sprintf(sqlSelect, sqlTableColNames[2], testSqlTableName, sqlTableColNames[0], placeholders)
			params = []interface{}{strconv.Itoa(i)}
			dbRow := sqlc.GetDB().QueryRow(sqlSelect, params...)
			dataRow, err := sqlc.FetchRow(dbRow, 1)
			if err != nil {
				t.Fatalf("%s failed: error [%s]", name+"/FetchRow/"+dbtype, err)
			} else if dataRow == nil {
				t.Fatalf("%s failed: nil", name+"/FetchRow/"+dbtype)
			}
			val := dataRow[0]
			if _, ok := val.([]byte); ok {
				val = string(val.([]byte))
			}
			if val != str {
				t.Fatalf("%s failed: expected %#v but received %#v", name+"/FetchRow/"+dbtype, str, dataRow[0])
			}
		}

		sqlSelect := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s", sqlTableColNames[2], testSqlTableName, sqlTableColNames[0])
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
			sqlSelect = fmt.Sprintf("SELECT t.%s FROM %s t WITH cross_partition=true", sqlTableColNames[2], testSqlTableName)
		}
		params := make([]interface{}, 0)
		dbRows, err := sqlc.GetDB().Query(sqlSelect, params...)
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/Query/"+dbtype, err)
		} else if dbRows == nil {
			t.Fatalf("%s failed: nil", name+"/Query/"+dbtype)
		}
		dataRows, err := sqlc.FetchRows(dbRows)
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/FetchRows/"+dbtype, err)
		} else if dataRows == nil {
			t.Fatalf("%s failed: nil", name+"/FetchRows/"+dbtype)
		}
		for i, row := range dataRows {
			for col, val := range row {
				row[strings.ToLower(col)] = val
			}
			if row[sqlTableColNames[2]] != strs[i] {
				t.Fatalf("%s failed: expected %#v but received %#v", name+"/FetchRow/"+dbtype, strs[i], row)
			}
		}
	}
}

func TestSqlConnect_FetchRow(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	name := "TestSqlConnect_FetchRow"
	urlMap := sqlGetUrlFromEnv()
	for dbtype, info := range urlMap {
		var sqlc *SqlConnect
		var err error
		switch dbtype {
		case "sqlite", "sqlite3":
			sqlc, err = newSqlConnectSqlite(info.driver, info.url, timezoneSql, 10000, nil)
		case "mssql":
			sqlc, err = newSqlConnectMssql(info.driver, info.url, timezoneSql, 10000, nil)
		case "mysql":
			sqlc, err = newSqlConnectMysql(info.driver, info.url, timezoneSql, 10000, nil)
		case "oracle":
			sqlc, err = newSqlConnectOracle(info.driver, info.url, timezoneSql, 10000, nil)
		case "pgsql", "postgresql":
			sqlc, err = newSqlConnectPgsql(info.driver, info.url, timezoneSql, 10000, nil)
		case "cosmos", "cosmosdb":
			sqlc, err = newSqlConnectCosmosdb(info.driver, info.url, timezoneSql, 10000, nil)
		default:
			t.Fatalf("%s failed: unknown database type [%s]", name, dbtype)
		}
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/"+dbtype, err)
		} else if sqlc == nil {
			t.Fatalf("%s failed: nil", name+"/"+dbtype)
		}
		err = sqlInitTable(sqlc, testSqlTableName, dbtype, true)
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/sqlInitTable/"+dbtype, err)
		}

		placeholder := _generatePlaceholders(1, dbtype)
		sqlSelect := fmt.Sprintf("SELECT * FROM %s WHERE %s=%s", testSqlTableName, sqlTableColNames[0], placeholder)
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
			colList := "t." + sqlTableColNames[1]
			for i, n := 2, len(sqlTableColNames); i < n; i++ {
				colList += ",t." + sqlTableColNames[i]
			}
			sqlSelect = fmt.Sprintf("SELECT %s FROM %s t WHERE t.%s=%s", colList, testSqlTableName, sqlTableColNames[0], placeholder)
		}
		params := []interface{}{strconv.Itoa(rand.Intn(10))}
		dbRow := sqlc.GetDB().QueryRow(sqlSelect, params...)
		dataRow, err := sqlc.FetchRow(dbRow, len(sqlTableColNames)-1)
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/FetchRow/"+dbtype, err)
		} else if dataRow == nil {
			t.Fatalf("%s failed: nil", name+"/FetchRow/"+dbtype)
		} else if len(dataRow) != len(sqlTableColNames)-1 {
			t.Fatalf("%s failed: expected %d fields but received %d", name+"/FetchRow/"+dbtype, len(sqlTableColNames)-1, len(dataRow))
		}
	}
}

func TestSqlConnect_FetchRows(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	name := "TestSqlConnect_FetchRows"
	urlMap := sqlGetUrlFromEnv()
	for dbtype, info := range urlMap {
		var sqlc *SqlConnect
		var err error
		switch dbtype {
		case "sqlite", "sqlite3":
			sqlc, err = newSqlConnectSqlite(info.driver, info.url, timezoneSql, 10000, nil)
		case "mssql":
			sqlc, err = newSqlConnectMssql(info.driver, info.url, timezoneSql, 10000, nil)
		case "mysql":
			sqlc, err = newSqlConnectMysql(info.driver, info.url, timezoneSql, 10000, nil)
		case "oracle":
			sqlc, err = newSqlConnectOracle(info.driver, info.url, timezoneSql, 10000, nil)
		case "pgsql", "postgresql":
			sqlc, err = newSqlConnectPgsql(info.driver, info.url, timezoneSql, 10000, nil)
		case "cosmos", "cosmosdb":
			sqlc, err = newSqlConnectPgsql(info.driver, info.url, timezoneSql, 10000, nil)
		default:
			t.Fatalf("%s failed: unknown database type [%s]", name, dbtype)
		}
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/"+dbtype, err)
		} else if sqlc == nil {
			t.Fatalf("%s failed: nil", name+"/"+dbtype)
		}
		err = sqlInitTable(sqlc, testSqlTableName, dbtype, true)
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/sqlInitTable/"+dbtype, err)
		}
		placeholder := _generatePlaceholders(1, dbtype)
		sqlSelect := "SELECT * FROM %s WHERE userid < %s"
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
			sqlSelect = "SELECT CROSS PARTITION * FROM %s t WHERE t.userid < %s"
		}
		i := rand.Intn(10)
		params := []interface{}{strconv.Itoa(i)}
		dbRows, err := sqlc.GetDB().Query(fmt.Sprintf(sqlSelect, testSqlTableName, placeholder), params...)
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/Query/"+dbtype, err)
		} else if dbRows == nil {
			t.Fatalf("%s failed: nil", name+"/Query/"+dbtype)
		}
		dataRows, err := sqlc.FetchRows(dbRows)
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/FetchRows/"+dbtype, err)
		} else if dataRows == nil {
			t.Fatalf("%s failed: nil", name+"/FetchRows/"+dbtype)
		} else if len(dataRows) != i {
			t.Fatalf("%s failed: expected %d fields but received %d", name+"/FetchRows/"+dbtype, i, len(dataRows))
		}
	}
}

func TestSqlConnect_FetchRowsCallback(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	name := "TestSqlConnect_FetchRowsCallback"
	urlMap := sqlGetUrlFromEnv()
	for dbtype, info := range urlMap {
		var sqlc *SqlConnect
		var err error
		switch dbtype {
		case "sqlite", "sqlite3":
			sqlc, err = newSqlConnectSqlite(info.driver, info.url, timezoneSql, 10000, nil)
		case "mssql":
			sqlc, err = newSqlConnectMssql(info.driver, info.url, timezoneSql, 10000, nil)
		case "mysql":
			sqlc, err = newSqlConnectMysql(info.driver, info.url, timezoneSql, 10000, nil)
		case "oracle":
			sqlc, err = newSqlConnectOracle(info.driver, info.url, timezoneSql, 10000, nil)
		case "pgsql", "postgresql":
			sqlc, err = newSqlConnectPgsql(info.driver, info.url, timezoneSql, 10000, nil)
		case "cosmos", "cosmosdb":
			sqlc, err = newSqlConnectCosmosdb(info.driver, info.url, timezoneSql, 10000, nil)
		default:
			t.Fatalf("%s failed: unknown database type [%s]", name, dbtype)
		}
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/"+dbtype, err)
		} else if sqlc == nil {
			t.Fatalf("%s failed: nil", name+"/"+dbtype)
		}
		err = sqlInitTable(sqlc, testSqlTableName, dbtype, true)
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/sqlInitTable/"+dbtype, err)
		}
		placeholder := _generatePlaceholders(1, dbtype)
		sqlSelect := "SELECT * FROM %s WHERE userid < %s"
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
			sqlSelect = "SELECT CROSS PARTITION * FROM %s t WHERE t.userid < %s"
		}
		i := rand.Intn(10)
		params := []interface{}{strconv.Itoa(i)}
		dbRows, err := sqlc.GetDB().Query(fmt.Sprintf(sqlSelect, testSqlTableName, placeholder), params...)
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/Query/"+dbtype, err)
		} else if dbRows == nil {
			t.Fatalf("%s failed: nil", name+"/Query/"+dbtype)
		}
		dataRows := make([]map[string]interface{}, 0)
		sqlc.FetchRowsCallback(dbRows, func(row map[string]interface{}, err error) bool {
			if err != nil {
				return false
			}
			dataRows = append(dataRows, row)
			return true
		})
		if len(dataRows) != i {
			t.Fatalf("%s failed: expected %d fields but received %d", name+"/FetchRowsCallback/"+dbtype, i, len(dataRows))
		}
	}
}

/*----------------------------------------------------------------------*/

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

var sqlColNames_TestDataTypeInt = []string{"id",
	"data_int", "data_integer", "data_decimal", "data_number", "data_numeric",
	"data_tinyint", "data_smallint", "data_mediumint", "data_bigint",
	"data_int1", "data_int2", "data_int4", "data_int8"}

func _testSqlDataTypeInt(t *testing.T, name, dbtype string, sqlc *SqlConnect, colTypes []string) {
	tblName := "tbl_test"
	rand.Seed(time.Now().UnixNano())

	colNameList := sqlColNames_TestDataTypeInt

	// init
	sqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE %s", tblName))
	if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
	sql += _generatePlaceholders(len(colNameList), dbtype) + ")"
	for i := 1; i <= numRows; i++ {
		vInt := rand.Int63()
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
			params = append(params, row.id)
		}
		_, err := sqlc.GetDB().Exec(sql, params...)
		if err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
	}

	// query some rows
	id := rand.Intn(numRows) + 1
	placeholder := _generatePlaceholders(1, dbtype)
	sql = "SELECT * FROM %s WHERE id>=%s ORDER BY id"
	if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
		return false
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
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
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

	_testSqlDataTypeInt(t, name, "cosmos", sqlc, nil)
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
	_testSqlDataTypeInt(t, name, dbtype, sqlc, sqlColTypes)
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
	_testSqlDataTypeInt(t, name, dbtype, sqlc, sqlColTypes)
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
	_testSqlDataTypeInt(t, name, dbtype, sqlc, sqlColTypes)
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
	_testSqlDataTypeInt(t, name, dbtype, sqlc, sqlColTypes)
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

	// DECIMAL(*,0), NUMBER(*,0) or NUMERIC(*,0) can be used as integer
	sqlColTypes := []string{"NVARCHAR(8)",
		"INT", "INTEGER", "DECIMAL(32,0)", "NUMBER(32,0)", "NUMERIC(32,0)",
		"TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT",
		"INT1", "INT2", "INT4", "INT8"}
	_testSqlDataTypeInt(t, name, dbtype, sqlc, sqlColTypes)
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
	sql += _generatePlaceholders(len(colNameList), dbtype) + ")"
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
	placeholder := _generatePlaceholders(1, dbtype)
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

/*----------------------------------------------------------------------*/

func _toFloatIfReal(v interface{}) (float64, error) {
	if v == nil {
		return 0, errors.New("input is nil")
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Float32, reflect.Float64:
		return rv.Float(), nil
	}
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

var sqlColNames_TestDataTypeReal = []string{"id",
	"data_float", "data_double", "data_real",
	"data_decimal", "data_number", "data_numeric",
	"data_float32", "data_float64",
	"data_float4", "data_float8"}

func _testSqlDataTypeReal(t *testing.T, name, dbtype string, sqlc *SqlConnect, colTypes []string) {
	tblName := "tbl_test"
	rand.Seed(time.Now().UnixNano())

	colNameList := sqlColNames_TestDataTypeReal

	// init
	sqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE %s", tblName))
	if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
	sql += _generatePlaceholders(len(colNameList), dbtype) + ")"
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
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
			params = append(params, row.id)
		}
		_, err := sqlc.GetDB().Exec(sql, params...)
		if err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
	}

	// query some rows
	sql = "SELECT * FROM %s ORDER BY id"
	if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
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

	_testSqlDataTypeReal(t, name, "cosmos", sqlc, nil)
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
	_testSqlDataTypeReal(t, name, dbtype, sqlc, sqlColTypes)
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
	_testSqlDataTypeReal(t, name, dbtype, sqlc, sqlColTypes)
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
	_testSqlDataTypeReal(t, name, dbtype, sqlc, sqlColTypes)
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
	_testSqlDataTypeReal(t, name, dbtype, sqlc, sqlColTypes)
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
	_testSqlDataTypeReal(t, name, dbtype, sqlc, sqlColTypes)
}

/*----------------------------------------------------------------------*/

var sqlColNames_TestDataTypeString = []string{"id",
	"data_char", "data_varchar", "data_binchar", "data_text",
	"data_uchar", "data_uvchar", "data_utext",
	"data_clob", "data_uclob", "data_blob"}

func _testSqlDataTypeString(t *testing.T, name, dbtype string, sqlc *SqlConnect, colTypes []string) {
	tblName := "tbl_test"
	rand.Seed(time.Now().UnixNano())

	colNameList := sqlColNames_TestDataTypeString

	// init
	sqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE %s", tblName))
	if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
	sql += _generatePlaceholders(len(colNameList), dbtype) + ")"
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
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
			params = append(params, row.id)
		}
		_, err := sqlc.GetDB().Exec(sql, params...)
		if err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
	}

	// query some rows
	id := rand.Intn(numRows) + 1
	placeholder := _generatePlaceholders(1, dbtype)
	sql = "SELECT * FROM %s WHERE id>=%s ORDER BY id"
	if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
		return false
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
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
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

	_testSqlDataTypeString(t, name, "cosmos", sqlc, nil)
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
	_testSqlDataTypeString(t, name, dbtype, sqlc, sqlColTypes)
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
	_testSqlDataTypeString(t, name, dbtype, sqlc, sqlColTypes)
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
	_testSqlDataTypeString(t, name, dbtype, sqlc, sqlColTypes)
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
	_testSqlDataTypeString(t, name, dbtype, sqlc, sqlColTypes)
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
	_testSqlDataTypeString(t, name, dbtype, sqlc, sqlColTypes)
}

/*----------------------------------------------------------------------*/

var sqlColNames_TestDataTypeMoney = []string{"id",
	"data_money2", "data_money4", "data_money6", "data_money8"}

func _testSqlDataTypeMoney(t *testing.T, name, dbtype string, sqlc *SqlConnect, colTypes []string) {
	tblName := "tbl_test"
	rand.Seed(time.Now().UnixNano())

	colNameList := sqlColNames_TestDataTypeMoney

	// init
	sqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE %s", tblName))
	if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
		id         string
		dataMoney2 float64
		dataMoney4 float64
		dataMoney6 float64
		dataMoney8 float64
	}
	rowArr := make([]Row, 0)
	numRows := 100

	// insert some rows
	sql := fmt.Sprintf("INSERT INTO %s (", tblName)
	sql += strings.Join(colNameList, ",")
	sql += ") VALUES ("
	sql += _generatePlaceholders(len(colNameList), dbtype) + ")"
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
		if dbtype == "cosmos" || dbtype == "cosmosdb" {
			params = append(params, row.id)
		}
		_, err := sqlc.GetDB().Exec(sql, params...)
		if err != nil {
			t.Fatalf("%s failed: %s", name, err)
		}
	}

	// query some rows
	sql = "SELECT * FROM %s ORDER BY id"
	if dbtype == "cosmos" || dbtype == "cosmosdb" {
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
			e := expected.dataMoney2
			f := colNameList[1]
			v, err := _toFloatIfReal(row[f])
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.2f", e), fmt.Sprintf("%.2f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v/%.10f(%T) but received %#v/%.10f(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, e, vstr, v, row[f], err)
			}
		}
		{
			e := expected.dataMoney4
			f := colNameList[2]
			v, err := _toFloatIfReal(row[f])
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.4f", e), fmt.Sprintf("%.4f", v); err != nil || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v/%.10f(%T) but received %#v/%.10f(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, e, vstr, v, row[f], err)
			}
		}
		{
			e := expected.dataMoney6
			f := colNameList[3]
			v, err := _toFloatIfReal(row[f])
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.6f", e), fmt.Sprintf("%.6f", v); err != nil || vstr != estr {
				fmt.Printf("\tDEBUG: Row %#v(%.10f) / Expected %#v(%.10f)\n", e, e, row[f], row[f])
				t.Fatalf("%s failed: [%s] expected %#v/%.10f(%T) but received %#v/%.10f(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, e, vstr, v, row[f], err)
			}
		}
		{
			e := expected.dataMoney8
			f := colNameList[4]
			v, err := _toFloatIfReal(row[f])
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
				v, err = _toFloatIfNumber(row[f])
			}
			if estr, vstr := fmt.Sprintf("%.8f", e), fmt.Sprintf("%.8f", v); err != nil || vstr != estr {
				fmt.Printf("\tDEBUG: Row %#v / Expected %#v\n", row[f], e)
				t.Fatalf("%s failed: [%s] expected %#v/%.10f(%T) but received %#v/%.10f(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, e, vstr, v, row[f], err)
			}
		}
	}
}

func TestSql_DataTypeMoney_Cosmos(t *testing.T) {
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

	_testSqlDataTypeMoney(t, name, "cosmos", sqlc, nil)
}

func TestSql_DataTypeMoney_Mssql(t *testing.T) {
	name := "TestSql_DataTypeMoney_Mssql"
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
	// SMALLMONEY & MONEY have 4 decimal point digits
	sqlColTypes := []string{"NVARCHAR(8)",
		"DEC(36,2)", "MONEY", "DECIMAL(36,6)", "NUMERIC(36,8)"}
	_testSqlDataTypeMoney(t, name, dbtype, sqlc, sqlColTypes)
}

func TestSql_DataTypeMoney_Mysql(t *testing.T) {
	name := "TestSql_DataTypeMoney_Mysql"
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
		"DECIMAL(24,2)", "NUMERIC(28,4)", "DEC(32,6)", "NUMERIC(36,8)"}
	_testSqlDataTypeMoney(t, name, dbtype, sqlc, sqlColTypes)
}

func TestSql_DataTypeMoney_Oracle(t *testing.T) {
	name := "TestSql_DataTypeMoney_Oracle"
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

	sqlColTypes := []string{"VARCHAR(8)",
		"NUMERIC(24,2)", "DECIMAL(28,4)", "DEC(32,6)", "NUMERIC(36,8)"}
	_testSqlDataTypeMoney(t, name, dbtype, sqlc, sqlColTypes)
}

func TestSql_DataTypeMoney_Pgsql(t *testing.T) {
	name := "TestSql_DataTypeMoney_Pgsql"
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
		"MONEY", "NUMERIC(28,4)", "DECIMAL(32,6)", "DEC(36,8)"}
	_testSqlDataTypeMoney(t, name, dbtype, sqlc, sqlColTypes)
}

func TestSql_DataTypeMoney_Sqlite(t *testing.T) {
	name := "TestSql_DataTypeMoney_Sqlite"
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
		"DECIMAL(24,2)", "NUMERIC(28,4)", "DEC(32,6)", "NUMERIC(36,8)"}
	_testSqlDataTypeMoney(t, name, dbtype, sqlc, sqlColTypes)
}

// func TestSql_DataType_Mysql(t *testing.T) {
// 	name := "TestSql_DataType_Mysql"
// 	urlMap := sqlGetUrlFromEnv()
// 	info, ok := urlMap["mysql"]
// 	if !ok {
// 		t.Skipf("%s skipped", name)
// 	}
// 	sqlc, err := newSqlConnectMysql(info.driver, info.url, timezoneSql, -1, nil)
// 	if err != nil {
// 		t.Fatalf("%s failed: error [%s]", name, err)
// 	} else if sqlc == nil {
// 		t.Fatalf("%s failed: nil", name)
// 	}
//
// 	sqlColTypes := []string{"VARCHAR(8)", "VARCHAR(64)", "CHAR(1)",
// 		"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT",
// 		"FLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION",
// 		"NUMERIC(12,2)", "DECIMAL(12,4)", "NUMERIC(12,6)", "DECIMAL(12,8)",
// 		"TIME", "TIME",
// 		"DATE", "DATE",
// 		"DATETIME", "DATETIME",
// 		"TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"}
// 	_testSqlDataType(t, name, "mysql", sqlc, sqlColTypes)
// }
//
// func TestSql_DataType_Oracle(t *testing.T) {
// 	name := "TestSql_DataType_Oracle"
// 	urlMap := sqlGetUrlFromEnv()
// 	info, ok := urlMap["oracle"]
// 	if !ok {
// 		t.Skipf("%s skipped", name)
// 	}
// 	sqlc, err := newSqlConnectOracle(info.driver, info.url, timezoneSql, -1, nil)
// 	if err != nil {
// 		t.Fatalf("%s failed: error [%s]", name, err)
// 	} else if sqlc == nil {
// 		t.Fatalf("%s failed: nil", name)
// 	}
//
// 	sqlColTypes := []string{"NVARCHAR2(8)", "NVARCHAR2(64)", "NCHAR(1)",
// 		"SMALLINT", "SMALLINT", "INT", "INTEGER", "INTEGER",
// 		"FLOAT", "BINARY_FLOAT", "REAL", "BINARY_DOUBLE",
// 		"NUMERIC(12,2)", "DECIMAL(12,4)", "NUMBER(12,6)", "DECIMAL(12,8)",
// 		"DATE", "TIMESTAMP WITH TIME ZONE",
// 		"DATE", "TIMESTAMP WITH TIME ZONE",
// 		"DATE", "TIMESTAMP WITH TIME ZONE",
// 		"TIMESTAMP", "TIMESTAMP WITH TIME ZONE"}
// 	_testSqlDataType(t, name, "oracle", sqlc, sqlColTypes)
// }
//
// func TestSql_DataType_Pgsql(t *testing.T) {
// 	name := "TestSql_DataType_Pgsql"
// 	urlMap := sqlGetUrlFromEnv()
// 	info, ok := urlMap["pgsql"]
// 	if !ok {
// 		info, ok = urlMap["postgresql"]
// 		if !ok {
// 			t.Skipf("%s skipped", name)
// 		}
// 	}
// 	sqlc, err := newSqlConnectPgsql(info.driver, info.url, timezoneSql, -1, nil)
// 	if err != nil {
// 		t.Fatalf("%s failed: error [%s]", name, err)
// 	} else if sqlc == nil {
// 		t.Fatalf("%s failed: nil", name)
// 	}
//
// 	sqlColTypes := []string{"VARCHAR(8)", "VARCHAR(64)", "CHAR(1)",
// 		"SMALLINT", "INT2", "INT4", "INT", "BIGINT",
// 		"FLOAT", "REAL", "DOUBLE PRECISION", "DOUBLE PRECISION",
// 		"NUMERIC(12,2)", "DECIMAL(12,4)", "NUMERIC(12,6)", "DECIMAL(12,8)",
// 		"TIME", "TIME WITH TIME ZONE",
// 		"DATE", "DATE",
// 		"TIMESTAMP", "TIMESTAMP WITH TIME ZONE",
// 		"TIMESTAMP", "TIMESTAMP WITH TIME ZONE"}
// 	_testSqlDataType(t, name, "pgsql", sqlc, sqlColTypes)
// }
//
// func TestSql_DataType_Sqlite(t *testing.T) {
// 	name := "TestSql_DataType_Sqlite"
// 	urlMap := sqlGetUrlFromEnv()
// 	info, ok := urlMap["sqlite"]
// 	if !ok {
// 		info, ok = urlMap["sqlite3"]
// 		if !ok {
// 			t.Skipf("%s skipped", name)
// 		}
// 	}
// 	sqlc, err := newSqlConnectSqlite(info.driver, info.url, timezoneSql, -1, nil)
// 	if err != nil {
// 		t.Fatalf("%s failed: error [%s]", name, err)
// 	} else if sqlc == nil {
// 		t.Fatalf("%s failed: nil", name)
// 	}
//
// 	sqlColTypes := []string{"VARCHAR(8)", "VARCHAR(64)", "CHAR(1)",
// 		"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT",
// 		"FLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION",
// 		"NUMERIC(12,2)", "DECIMAL(12,4)", "NUMERIC(12,6)", "DECIMAL(12,8)",
// 		"TIME", "TIME",
// 		"DATE", "DATE",
// 		"DATETIME", "DATETIME",
// 		"TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"}
// 	_testSqlDataType(t, name, "sqlite", sqlc, sqlColTypes)
// }

// /*----------------------------------------------------------------------*/
//
// var sqlColNamesTestDataType = []string{"id", "data_str", "data_bool",
// 	"data_int_tiny", "data_int_small", "data_int_medium", "data_int", "data_int_big",
// 	"data_float1", "data_float2", "data_float3", "data_float4",
// 	"data_fix1", "data_fix2", "data_fix3", "data_fix4",
// 	"data_time", "data_timez",
// 	"data_date", "data_datez",
// 	"data_datetime", "data_datetimez",
// 	"data_timestamp", "data_timestampz"}
//
// func _testSqlDataType(t *testing.T, name, dbtype string, sqlc *SqlConnect, colTypes []string) {
// 	tblName := "tbl_test"
// 	rand.Seed(time.Now().UnixNano())
// 	{
// 		sqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE %s", tblName))
//
// 		if dbtype == "cosmos" || dbtype == "cosmosdb" {
// 			if _, err := sqlc.GetDB().Exec(fmt.Sprintf("CREATE TABLE %s WITH pk=/%s WITH maxru=10000", tblName, sqlColNamesTestDataType[0])); err != nil {
// 				t.Fatalf("%s failed: %s", name, err)
// 			}
// 		} else {
// 			sql := fmt.Sprintf("CREATE TABLE %s (", tblName)
// 			for i := range sqlColNamesTestDataType {
// 				sql += sqlColNamesTestDataType[i] + " " + colTypes[i] + ","
// 			}
// 			sql += fmt.Sprintf("PRIMARY KEY(%s))", sqlColNamesTestDataType[0])
// 			if _, err := sqlc.GetDB().Exec(sql); err != nil {
// 				t.Fatalf("%s failed: %s\n%s", name, err, sql)
// 			}
// 		}
// 	}
//
// 	timeLayoutFull := "2006-01-02T15:04:05-07:00"
// 	timeLayoutTime := "15:04:05-07:00"
// 	timeLayoutDate := "2006-01-02"
// 	loc, _ := time.LoadLocation(timezoneSql)
// 	now := time.Now().Round(time.Second).In(loc)
// 	// fmt.Printf("Now: %s / %s\n", time.Now().Format(timeLayoutFull), now.Format(timeLayoutFull))
// 	type Row struct {
// 		id             string
// 		dataStr        string
// 		dataBool       string
// 		dataIntTiny    int8
// 		dataIntSmall   int16
// 		dataIntMedium  int
// 		dataInt        int32
// 		dataIntBig     int64
// 		dataFloat1     float64
// 		dataFloat2     float64
// 		dataFloat3     float64
// 		dataFloat4     float64
// 		dataFix1       float64
// 		dataFix2       float64
// 		dataFix3       float64
// 		dataFix4       float64
// 		dataTime       time.Time
// 		dataTimez      time.Time
// 		dataDate       time.Time
// 		dataDatez      time.Time
// 		dataDatetime   time.Time
// 		dataDatetimez  time.Time
// 		dataTimestamp  time.Time
// 		dataTimestampz time.Time
// 	}
// 	rowArr := make([]Row, 0)
// 	numRows := 100
//
// 	// insert some rows
// 	{
// 		sql := fmt.Sprintf("INSERT INTO %s (", tblName)
// 		sql += strings.Join(sqlColNamesTestDataType, ",")
// 		sql += ") VALUES ("
// 		sql += _generatePlaceholders(len(sqlColNamesTestDataType), dbtype) + ")"
// 		for i := 1; i <= numRows; i++ {
// 			// ti := now.Add(time.Duration(i) * time.Minute)
// 			ti := now
// 			vInt := rand.Int63()
// 			if dbtype == "cosmos" || dbtype == "cosmosdb" {
// 				vInt >>= 63 - 48
// 			}
// 			row := Row{
// 				id:             fmt.Sprintf("%03d", i),
// 				dataStr:        ti.Format(timeLayoutFull),
// 				dataBool:       strconv.Itoa(int(vInt % 2)),
// 				dataIntTiny:    int8(vInt % (2 ^ 8)),
// 				dataIntSmall:   int16(vInt % (2 ^ 16)),
// 				dataIntMedium:  int(vInt % (2 ^ 24)),
// 				dataInt:        int32(vInt % (2 ^ 32)),
// 				dataIntBig:     vInt,
// 				dataFloat1:     math.Round(rand.Float64()*10e5) / 10e5,
// 				dataFloat2:     math.Round(rand.Float64()*10e5) / 10e5,
// 				dataFloat3:     math.Round(rand.Float64()*10e8) / 10e8,
// 				dataFloat4:     math.Round(rand.Float64()*10e8) / 10e8,
// 				dataFix1:       math.Round(rand.Float64()*10e1) / 10e1,
// 				dataFix2:       math.Round(rand.Float64()*10e3) / 10e3,
// 				dataFix3:       math.Round(rand.Float64()*10e5) / 10e5,
// 				dataFix4:       math.Round(rand.Float64()*10e7) / 10e7,
// 				dataTime:       ti,
// 				dataTimez:      ti,
// 				dataDate:       ti,
// 				dataDatez:      ti,
// 				dataDatetime:   ti,
// 				dataDatetimez:  ti,
// 				dataTimestamp:  ti,
// 				dataTimestampz: ti,
// 			}
// 			rowArr = append(rowArr, row)
// 			params := []interface{}{row.id, row.dataStr, row.dataBool,
// 				row.dataIntTiny, row.dataIntSmall, row.dataIntMedium, row.dataInt, row.dataIntBig,
// 				row.dataFloat1, row.dataFloat2, row.dataFloat3, row.dataFloat4,
// 				row.dataFix1, row.dataFix2, row.dataFix3, row.dataFix4,
// 				row.dataTime, row.dataTimez, row.dataDate, row.dataDatez,
// 				row.dataDatetime, row.dataDatetimez, row.dataTimestamp, row.dataTimestampz}
// 			if dbtype == "cosmos" || dbtype == "cosmosdb" {
// 				params = append(params, row.id)
// 			}
// 			_, err := sqlc.GetDB().Exec(sql, params...)
// 			if err != nil {
// 				t.Fatalf("%s failed: %s", name, err)
// 			}
// 		}
// 	}
//
// 	// query some rows
// 	{
// 		id := rand.Intn(numRows) + 1
// 		placeholder := _generatePlaceholders(1, dbtype)
// 		sql := "SELECT * FROM %s WHERE id>=%s ORDER BY id"
// 		if dbtype == "cosmos" || dbtype == "cosmosdb" {
// 			sql = "SELECT * FROM %s t WHERE t.id>=%s WITH cross_partition=true"
// 		}
// 		sql = fmt.Sprintf(sql, tblName, placeholder)
// 		params := []interface{}{fmt.Sprintf("%03d", id)}
// 		dbRows, err := sqlc.GetDB().Query(sql, params...)
// 		if err != nil {
// 			t.Fatalf("%s failed: %s", name, err)
// 		}
// 		defer dbRows.Close()
// 		rows := make([]map[string]interface{}, 0)
// 		err = sqlc.FetchRowsCallback(dbRows, func(row map[string]interface{}, err error) bool {
// 			rows = append(rows, row)
// 			return false
// 		})
// 		// rows, err := sqlc.FetchRows(dbRows)
// 		if err != nil {
// 			t.Fatalf("%s failed: %s", name, err)
// 		}
// 		expected := rowArr[id-1]
// 		row := rows[0]
// 		// fmt.Printf("DEBUG: %#v\n", row)
// 		// fmt.Println(id, row)
// 		for k, v := range row {
// 			// transform to lower-cases
// 			row[strings.ToLower(k)] = v
// 		}
// 		{
// 			f := "id"
// 			e, _ := reddo.ToInt(expected.id)
// 			v, _ := reddo.ToInt(row[f])
// 			if v != e {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e, v)
// 			}
// 		}
// 		{
// 			f := "data_int_tiny"
// 			e, _ := reddo.ToInt(expected.dataIntTiny)
// 			v, _ := reddo.ToInt(row[f])
// 			if v != e {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e, v)
// 			}
// 		}
// 		{
// 			f := "data_int_small"
// 			e, _ := reddo.ToInt(expected.dataIntSmall)
// 			v, _ := reddo.ToInt(row[f])
// 			if v != e {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e, v)
// 			}
// 		}
// 		{
// 			f := "data_int_medium"
// 			e, _ := reddo.ToInt(expected.dataIntMedium)
// 			v, _ := reddo.ToInt(row[f])
// 			if v != e {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e, v)
// 			}
// 		}
// 		{
// 			f := "data_int"
// 			e, _ := reddo.ToInt(expected.dataInt)
// 			v, _ := reddo.ToInt(row[f])
// 			if v != e {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e, v)
// 			}
// 		}
// 		{
// 			f := "data_int_big"
// 			e, _ := reddo.ToInt(expected.dataIntBig)
// 			v, _ := reddo.ToInt(row[f])
// 			if v != e {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e, v)
// 			}
// 		}
// 		{
// 			f := "data_float1"
// 			e, _ := reddo.ToFloat(expected.dataFloat1)
// 			v, _ := reddo.ToFloat(row[f])
// 			if math.Round(v*10e5)/10e5 != math.Round(e*10e5)/10e5 {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e, v)
// 			}
// 		}
// 		{
// 			f := "data_float2"
// 			e, _ := reddo.ToFloat(expected.dataFloat2)
// 			v, _ := reddo.ToFloat(row[f])
// 			if math.Round(v*10e5)/10e5 != math.Round(e*10e5)/10e5 {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e, v)
// 			}
// 		}
// 		{
// 			f := "data_float3"
// 			e, _ := reddo.ToFloat(expected.dataFloat3)
// 			v, _ := reddo.ToFloat(row[f])
// 			if math.Round(v*10e8)/10e8 != math.Round(e*10e8)/10e8 {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e, v)
// 			}
// 		}
// 		{
// 			f := "data_float4"
// 			e, _ := reddo.ToFloat(expected.dataFloat4)
// 			v, _ := reddo.ToFloat(row[f])
// 			if math.Round(v*10e8)/10e8 != math.Round(e*10e8)/10e8 {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e, v)
// 			}
// 		}
// 		{
// 			f := "data_fix1"
// 			e, _ := reddo.ToFloat(expected.dataFix1)
// 			v, _ := reddo.ToFloat(row[f])
// 			if math.Round(v*10e1)/10e1 != math.Round(e*10e1)/10e1 {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e, v)
// 			}
// 		}
// 		{
// 			f := "data_fix2"
// 			e, _ := reddo.ToFloat(expected.dataFix2)
// 			v, _ := reddo.ToFloat(row[f])
// 			if math.Round(v*10e3)/10e3 != math.Round(e*10e3)/10e3 {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e, v)
// 			}
// 		}
// 		{
// 			f := "data_fix3"
// 			e, _ := reddo.ToFloat(expected.dataFix3)
// 			v, _ := reddo.ToFloat(row[f])
// 			if math.Round(v*10e5)/10e5 != math.Round(e*10e5)/10e5 {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e, v)
// 			}
// 		}
// 		{
// 			f := "data_fix4"
// 			e, _ := reddo.ToFloat(expected.dataFix4)
// 			v, _ := reddo.ToFloat(row[f])
// 			if math.Round(v*10e7)/10e7 != math.Round(e*10e7)/10e7 {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e, v)
// 			}
// 		}
// 		{
// 			f := "data_str"
// 			e, _ := reddo.ToString(expected.dataStr)
// 			v, _ := reddo.ToString(row[f])
// 			if v != e {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e, v)
// 			}
// 		}
// 		{
// 			f := "data_bool"
// 			e, _ := reddo.ToBool(expected.dataBool)
// 			v, _ := reddo.ToBool(row[f])
// 			if v != e {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e, v)
// 			}
// 		}
// 		{
// 			tz := time.Now().In(loc).Format("-07:00")
// 			for _, f := range []string{"data_time", "data_timez", "data_date", "data_datez", "data_datetime", "data_datetimez", "data_timestamp", "data_timestampz"} {
// 				v, _ := reddo.ToTime(row[f])
// 				if str, ok := row[f].(string); ok {
// 					v, _ = reddo.ToTimeWithLayout(str, time.RFC3339)
// 				}
// 				if v.Format("-07:00") != tz {
// 					t.Fatalf("%s failed: [%s] expected timezone %#v but received %#v", name, row["id"].(string)+"/"+f, tz, v.Format("-07:00"))
// 				}
// 			}
// 		}
// 		{
// 			f := "data_time"
// 			e, _ := reddo.ToTime(expected.dataTime)
// 			v, _ := reddo.ToTime(row[f])
// 			if str, ok := row[f].(string); ok {
// 				v, _ = reddo.ToTimeWithLayout(str, time.RFC3339)
// 			}
// 			if v.In(loc).Format(timeLayoutTime) != e.In(loc).Format(timeLayoutTime) {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e.In(loc).Format(timeLayoutTime), v.In(loc).Format(timeLayoutTime))
// 			}
// 		}
// 		{
// 			f := "data_timez"
// 			e, _ := reddo.ToTime(expected.dataTimez)
// 			v, _ := reddo.ToTime(row[f])
// 			if str, ok := row[f].(string); ok {
// 				v, _ = reddo.ToTimeWithLayout(str, time.RFC3339)
// 			}
// 			if v.In(loc).Format(timeLayoutTime) != e.In(loc).Format(timeLayoutTime) {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e.In(loc).Format(timeLayoutTime), v.In(loc).Format(timeLayoutTime))
// 			}
// 		}
// 		{
// 			f := "data_date"
// 			e, _ := reddo.ToTime(expected.dataDate)
// 			v, _ := reddo.ToTime(row[f])
// 			if str, ok := row[f].(string); ok {
// 				v, _ = reddo.ToTimeWithLayout(str, time.RFC3339)
// 			}
// 			if v.In(loc).Format(timeLayoutDate) != e.In(loc).Format(timeLayoutDate) {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e.In(loc).Format(timeLayoutDate), v.In(loc).Format(timeLayoutDate))
// 			}
// 		}
// 		{
// 			f := "data_datez"
// 			e, _ := reddo.ToTime(expected.dataDatez)
// 			v, _ := reddo.ToTime(row[f])
// 			if str, ok := row[f].(string); ok {
// 				v, _ = reddo.ToTimeWithLayout(str, time.RFC3339)
// 			}
// 			if v.In(loc).Format(timeLayoutDate) != e.In(loc).Format(timeLayoutDate) {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e.In(loc).Format(timeLayoutDate), v.In(loc).Format(timeLayoutDate))
// 			}
// 		}
// 		{
// 			f := "data_datetime"
// 			e, _ := reddo.ToTime(expected.dataDatetime)
// 			v, _ := reddo.ToTime(row[f])
// 			if str, ok := row[f].(string); ok {
// 				v, _ = reddo.ToTimeWithLayout(str, time.RFC3339)
// 			}
// 			if v.In(loc).Format(timeLayoutFull) != e.In(loc).Format(timeLayoutFull) {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e.In(loc).Format(timeLayoutFull), v.In(loc).Format(timeLayoutFull))
// 			}
// 		}
// 		{
// 			f := "data_datetimez"
// 			e, _ := reddo.ToTime(expected.dataDatetimez)
// 			v, _ := reddo.ToTime(row[f])
// 			if str, ok := row[f].(string); ok {
// 				v, _ = reddo.ToTimeWithLayout(str, time.RFC3339)
// 			}
// 			if v.In(loc).Format(timeLayoutFull) != e.In(loc).Format(timeLayoutFull) {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e.In(loc).Format(timeLayoutFull), v.In(loc).Format(timeLayoutFull))
// 			}
// 		}
// 		{
// 			f := "data_timestamp"
// 			e, _ := reddo.ToTime(expected.dataTimestamp)
// 			v, _ := reddo.ToTime(row[f])
// 			if str, ok := row[f].(string); ok {
// 				v, _ = reddo.ToTimeWithLayout(str, time.RFC3339)
// 			}
// 			if v.In(loc).Format(timeLayoutFull) != e.In(loc).Format(timeLayoutFull) {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e.In(loc).Format(timeLayoutFull), v.In(loc).Format(timeLayoutFull))
// 			}
// 		}
// 		{
// 			f := "data_timestampz"
// 			e, _ := reddo.ToTime(expected.dataTimestampz)
// 			v, _ := reddo.ToTime(row[f])
// 			if str, ok := row[f].(string); ok {
// 				v, _ = reddo.ToTimeWithLayout(str, time.RFC3339)
// 			}
// 			if v.In(loc).Format(timeLayoutFull) != e.In(loc).Format(timeLayoutFull) {
// 				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, row["id"].(string)+"/"+f, e.In(loc).Format(timeLayoutFull), v.In(loc).Format(timeLayoutFull))
// 			}
// 		}
// 	}
// }
//
// func TestSql_DataType_Cosmos(t *testing.T) {
// 	name := "TestSql_DataType_Cosmos"
// 	urlMap := sqlGetUrlFromEnv()
// 	info, ok := urlMap["cosmos"]
// 	if !ok {
// 		info, ok = urlMap["cosmosdb"]
// 		if !ok {
// 			t.Skipf("%s skipped", name)
// 		}
// 	}
// 	sqlc, err := newSqlConnectCosmosdb(info.driver, info.url, timezoneSql, -1, nil)
// 	if err != nil {
// 		t.Fatalf("%s failed: error [%s]", name, err)
// 	} else if sqlc == nil {
// 		t.Fatalf("%s failed: nil", name)
// 	}
//
// 	_testSqlDataType(t, name, "cosmos", sqlc, nil)
// }
//
// func TestSql_DataType_Mssql(t *testing.T) {
// 	name := "TestSql_DataType_Mssql"
// 	urlMap := sqlGetUrlFromEnv()
// 	info, ok := urlMap["mssql"]
// 	if !ok {
// 		t.Skipf("%s skipped", name)
// 	}
// 	sqlc, err := newSqlConnectMssql(info.driver, info.url, timezoneSql, -1, nil)
// 	if err != nil {
// 		t.Fatalf("%s failed: error [%s]", name, err)
// 	} else if sqlc == nil {
// 		t.Fatalf("%s failed: nil", name)
// 	}
//
// 	sqlColTypes := []string{"NVARCHAR(8)", "NVARCHAR(64)", "NCHAR(1)",
// 		"TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT",
// 		"FLOAT(24)", "REAL", "FLOAT(53)", "DOUBLE PRECISION",
// 		"NUMERIC(12,2)", "DECIMAL(12,4)", "NUMERIC(12,6)", "DECIMAL(12,8)",
// 		"TIME", "TIME",
// 		"DATE", "DATE",
// 		"DATETIME", "DATETIMEOFFSET",
// 		"DATETIME2", "DATETIMEOFFSET"}
// 	_testSqlDataType(t, name, "mssql", sqlc, sqlColTypes)
// }
//
// func TestSql_DataType_Mysql(t *testing.T) {
// 	name := "TestSql_DataType_Mysql"
// 	urlMap := sqlGetUrlFromEnv()
// 	info, ok := urlMap["mysql"]
// 	if !ok {
// 		t.Skipf("%s skipped", name)
// 	}
// 	sqlc, err := newSqlConnectMysql(info.driver, info.url, timezoneSql, -1, nil)
// 	if err != nil {
// 		t.Fatalf("%s failed: error [%s]", name, err)
// 	} else if sqlc == nil {
// 		t.Fatalf("%s failed: nil", name)
// 	}
//
// 	sqlColTypes := []string{"VARCHAR(8)", "VARCHAR(64)", "CHAR(1)",
// 		"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT",
// 		"FLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION",
// 		"NUMERIC(12,2)", "DECIMAL(12,4)", "NUMERIC(12,6)", "DECIMAL(12,8)",
// 		"TIME", "TIME",
// 		"DATE", "DATE",
// 		"DATETIME", "DATETIME",
// 		"TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"}
// 	_testSqlDataType(t, name, "mysql", sqlc, sqlColTypes)
// }
//
// func TestSql_DataType_Oracle(t *testing.T) {
// 	name := "TestSql_DataType_Oracle"
// 	urlMap := sqlGetUrlFromEnv()
// 	info, ok := urlMap["oracle"]
// 	if !ok {
// 		t.Skipf("%s skipped", name)
// 	}
// 	sqlc, err := newSqlConnectOracle(info.driver, info.url, timezoneSql, -1, nil)
// 	if err != nil {
// 		t.Fatalf("%s failed: error [%s]", name, err)
// 	} else if sqlc == nil {
// 		t.Fatalf("%s failed: nil", name)
// 	}
//
// 	sqlColTypes := []string{"NVARCHAR2(8)", "NVARCHAR2(64)", "NCHAR(1)",
// 		"SMALLINT", "SMALLINT", "INT", "INTEGER", "INTEGER",
// 		"FLOAT", "BINARY_FLOAT", "REAL", "BINARY_DOUBLE",
// 		"NUMERIC(12,2)", "DECIMAL(12,4)", "NUMBER(12,6)", "DECIMAL(12,8)",
// 		"DATE", "TIMESTAMP WITH TIME ZONE",
// 		"DATE", "TIMESTAMP WITH TIME ZONE",
// 		"DATE", "TIMESTAMP WITH TIME ZONE",
// 		"TIMESTAMP", "TIMESTAMP WITH TIME ZONE"}
// 	_testSqlDataType(t, name, "oracle", sqlc, sqlColTypes)
// }
//
// func TestSql_DataType_Pgsql(t *testing.T) {
// 	name := "TestSql_DataType_Pgsql"
// 	urlMap := sqlGetUrlFromEnv()
// 	info, ok := urlMap["pgsql"]
// 	if !ok {
// 		info, ok = urlMap["postgresql"]
// 		if !ok {
// 			t.Skipf("%s skipped", name)
// 		}
// 	}
// 	sqlc, err := newSqlConnectPgsql(info.driver, info.url, timezoneSql, -1, nil)
// 	if err != nil {
// 		t.Fatalf("%s failed: error [%s]", name, err)
// 	} else if sqlc == nil {
// 		t.Fatalf("%s failed: nil", name)
// 	}
//
// 	sqlColTypes := []string{"VARCHAR(8)", "VARCHAR(64)", "CHAR(1)",
// 		"SMALLINT", "INT2", "INT4", "INT", "BIGINT",
// 		"FLOAT", "REAL", "DOUBLE PRECISION", "DOUBLE PRECISION",
// 		"NUMERIC(12,2)", "DECIMAL(12,4)", "NUMERIC(12,6)", "DECIMAL(12,8)",
// 		"TIME", "TIME WITH TIME ZONE",
// 		"DATE", "DATE",
// 		"TIMESTAMP", "TIMESTAMP WITH TIME ZONE",
// 		"TIMESTAMP", "TIMESTAMP WITH TIME ZONE"}
// 	_testSqlDataType(t, name, "pgsql", sqlc, sqlColTypes)
// }
//
// func TestSql_DataType_Sqlite(t *testing.T) {
// 	name := "TestSql_DataType_Sqlite"
// 	urlMap := sqlGetUrlFromEnv()
// 	info, ok := urlMap["sqlite"]
// 	if !ok {
// 		info, ok = urlMap["sqlite3"]
// 		if !ok {
// 			t.Skipf("%s skipped", name)
// 		}
// 	}
// 	sqlc, err := newSqlConnectSqlite(info.driver, info.url, timezoneSql, -1, nil)
// 	if err != nil {
// 		t.Fatalf("%s failed: error [%s]", name, err)
// 	} else if sqlc == nil {
// 		t.Fatalf("%s failed: nil", name)
// 	}
//
// 	sqlColTypes := []string{"VARCHAR(8)", "VARCHAR(64)", "CHAR(1)",
// 		"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT",
// 		"FLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION",
// 		"NUMERIC(12,2)", "DECIMAL(12,4)", "NUMERIC(12,6)", "DECIMAL(12,8)",
// 		"TIME", "TIME",
// 		"DATE", "DATE",
// 		"DATETIME", "DATETIME",
// 		"TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"}
// 	_testSqlDataType(t, name, "sqlite", sqlc, sqlColTypes)
// }

// /*----------------------------------------------------------------------*/
//
// var sqlColNamesTestNullValue = []string{"id", "data_int", "data_float", "data_str", "data_time"}
//
// func _testSqlNullValue(t *testing.T, name, dbtype string, sqlc *SqlConnect, colTypes []string) {
// 	tblName := "tbl_test"
// 	rand.Seed(time.Now().UnixNano())
// 	{
// 		sqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE %s", tblName))
//
// 		if dbtype == "cosmos" || dbtype == "cosmosdb" {
// 			if _, err := sqlc.GetDB().Exec(fmt.Sprintf("CREATE TABLE %s WITH pk=/%s WITH maxru=10000", tblName, sqlColNamesTestDataType[0])); err != nil {
// 				t.Fatalf("%s failed: %s", name, err)
// 			}
// 		} else {
// 			sql := fmt.Sprintf("CREATE TABLE %s (", tblName)
// 			for i := range sqlColNamesTestNullValue {
// 				sql += sqlColNamesTestNullValue[i] + " " + colTypes[i] + ","
// 			}
// 			sql += fmt.Sprintf("PRIMARY KEY(%s))", sqlColNamesTestNullValue[0])
// 			if _, err := sqlc.GetDB().Exec(sql); err != nil {
// 				t.Fatalf("%s failed: %s\n%s", name, err, sql)
// 			}
// 		}
// 	}
//
// 	// timeLayoutFull := "2006-01-02T15:04:05-07:00"
// 	// timeLayoutTime := "15:04:05-07:00"
// 	// timeLayoutDate := "2006-01-02"
// 	loc, _ := time.LoadLocation(timezoneSql)
// 	type Row struct {
// 		id        string
// 		dataInt   *int64
// 		dataFloat *float64
// 		dataStr   *string
// 		dataTime  *time.Time
// 	}
// 	rowArr := make([]Row, 0)
// 	numRows := 100
//
// 	// insert some rows
// 	{
// 		sql := fmt.Sprintf("INSERT INTO %s (", tblName)
// 		sql += strings.Join(sqlColNamesTestNullValue, ",")
// 		sql += ") VALUES ("
// 		sql += _generatePlaceholders(len(sqlColNamesTestNullValue), dbtype) + ")"
// 		for i := 1; i <= numRows; i++ {
// 			vInt := int64(rand.Int31())
// 			vFloat := float64(rand.Float32())
// 			vStr := fmt.Sprintf("%0.6f", rand.Float64())
// 			vTime := time.Now().Add(time.Duration(rand.Intn(1234567)) * time.Second).Round(time.Second).In(loc)
// 			if dbtype == "cosmos" || dbtype == "cosmosdb" {
// 				vInt >>= 63 - 48
// 			}
// 			row := Row{id: fmt.Sprintf("%03d", i)}
// 			if i%2 == 0 {
// 				row.dataInt = &vInt
// 			}
// 			if i%3 == 0 {
// 				row.dataFloat = &vFloat
// 			}
// 			if i%4 == 0 {
// 				row.dataStr = &vStr
// 			}
// 			if i%5 == 0 {
// 				row.dataTime = &vTime
// 			}
// 			rowArr = append(rowArr, row)
// 			params := []interface{}{row.id, row.dataInt, row.dataFloat, row.dataStr, row.dataTime}
// 			if dbtype == "cosmos" || dbtype == "cosmosdb" {
// 				params = append(params, row.id)
// 			}
// 			_, err := sqlc.GetDB().Exec(sql, params...)
// 			if err != nil {
// 				t.Fatalf("%s failed: %s", name, err)
// 			}
// 		}
// 	}
//
// 	// query some rows
// 	{
// 		for i := 1; i <= numRows; i++ {
// 			id := fmt.Sprintf("%03d", i)
// 			placeholder := _generatePlaceholders(1, dbtype)
// 			sql := "SELECT * FROM %s WHERE id=%s ORDER BY id"
// 			if dbtype == "cosmos" || dbtype == "cosmosdb" {
// 				sql = "SELECT * FROM %s t WHERE t.id=%s WITH cross_partition=true"
// 			}
// 			sql = fmt.Sprintf(sql, tblName, placeholder)
// 			params := []interface{}{id}
// 			dbRows, err := sqlc.GetDB().Query(sql, params...)
// 			if err != nil {
// 				t.Fatalf("%s failed: %s", name, err)
// 			}
// 			rows, err := sqlc.FetchRows(dbRows)
// 			if err != nil {
// 				t.Fatalf("%s failed: %s", name, err)
// 			}
// 			row := rows[0]
// 			for k, v := range row {
// 				// transform to lower-cases
// 				row[strings.ToLower(k)] = v
// 			}
//
// 			if i%2 != 0 {
// 				if v, ok := row[sqlColNamesTestNullValue[1]]; !ok || v != (*int64)(nil) {
// 					t.Fatalf("%s failed: column %s / value %#v / status %#v", name, sqlColNamesTestNullValue[1], v, ok)
// 				}
// 			}
// 			if i%3 != 0 {
// 				if v, ok := row[sqlColNamesTestNullValue[2]]; !ok || v != (*float64)(nil) {
// 					t.Fatalf("%s failed: column %s / value %#v / status %#v", name, sqlColNamesTestNullValue[2], v, ok)
// 				}
// 			}
// 			if i%4 != 0 {
// 				if v, ok := row[sqlColNamesTestNullValue[3]]; !ok || v != (*string)(nil) {
// 					t.Fatalf("%s failed: column %s / value %#v / status %#v", name, sqlColNamesTestNullValue[3], v, ok)
// 				}
// 			}
// 			if i%5 != 0 {
// 				if v, ok := row[sqlColNamesTestNullValue[4]]; !ok || v != (*time.Time)(nil) {
// 					t.Fatalf("%s failed: column %s / value %#v / status %#v", name, sqlColNamesTestNullValue[4], v, ok)
// 				}
// 			}
// 		}
// 	}
// }
//
// func TestSql_NullValue_Mssql(t *testing.T) {
// 	name := "TestSql_NullValue_Mssql"
// 	dbtype := "mssql"
// 	urlMap := sqlGetUrlFromEnv()
// 	info, ok := urlMap[dbtype]
// 	if !ok {
// 		t.Skipf("%s skipped", name)
// 	}
// 	sqlc, err := newSqlConnectMssql(info.driver, info.url, timezoneSql, -1, nil)
// 	if err != nil {
// 		t.Fatalf("%s failed: error [%s]", name, err)
// 	} else if sqlc == nil {
// 		t.Fatalf("%s failed: nil", name)
// 	}
//
// 	sqlColTypes := []string{"NVARCHAR(8)", "INT", "REAL", "NVARCHAR(64)", "DATETIME"}
// 	_testSqlNullValue(t, name, dbtype, sqlc, sqlColTypes)
// }
//
// func TestSql_NullValue_Mysql(t *testing.T) {
// 	name := "TestSql_NullValue_Mysql"
// 	dbtype := "mysql"
// 	urlMap := sqlGetUrlFromEnv()
// 	info, ok := urlMap[dbtype]
// 	if !ok {
// 		t.Skipf("%s skipped", name)
// 	}
// 	sqlc, err := newSqlConnectMysql(info.driver, info.url, timezoneSql, -1, nil)
// 	if err != nil {
// 		t.Fatalf("%s failed: error [%s]", name, err)
// 	} else if sqlc == nil {
// 		t.Fatalf("%s failed: nil", name)
// 	}
//
// 	sqlColTypes := []string{"VARCHAR(8)", "INT", "REAL", "VARCHAR(64)", "DATETIME"}
// 	_testSqlNullValue(t, name, dbtype, sqlc, sqlColTypes)
// }
//
// func TestSql_NullValue_Oracle(t *testing.T) {
// 	name := "TestSql_NullValue_Oracle"
// 	dbtype := "oracle"
// 	urlMap := sqlGetUrlFromEnv()
// 	info, ok := urlMap[dbtype]
// 	if !ok {
// 		t.Skipf("%s skipped", name)
// 	}
// 	sqlc, err := newSqlConnectOracle(info.driver, info.url, timezoneSql, -1, nil)
// 	if err != nil {
// 		t.Fatalf("%s failed: error [%s]", name, err)
// 	} else if sqlc == nil {
// 		t.Fatalf("%s failed: nil", name)
// 	}
//
// 	sqlColTypes := []string{"NVARCHAR2(8)", "INT", "REAL", "NVARCHAR2(64)", "TIMESTAMP"}
// 	_testSqlNullValue(t, name, dbtype, sqlc, sqlColTypes)
// }
//
// func TestSql_NullValue_Pgsql(t *testing.T) {
// 	name := "TestSql_NullValue_Pgsql"
// 	dbtype := "pgsql"
// 	urlMap := sqlGetUrlFromEnv()
// 	info, ok := urlMap[dbtype]
// 	if !ok {
// 		t.Skipf("%s skipped", name)
// 	}
// 	sqlc, err := newSqlConnectPgsql(info.driver, info.url, timezoneSql, -1, nil)
// 	if err != nil {
// 		t.Fatalf("%s failed: error [%s]", name, err)
// 	} else if sqlc == nil {
// 		t.Fatalf("%s failed: nil", name)
// 	}
//
// 	sqlColTypes := []string{"VARCHAR(8)", "INT", "REAL", "VARCHAR(64)", "TIMESTAMP"}
// 	_testSqlNullValue(t, name, dbtype, sqlc, sqlColTypes)
// }
//
// func TestSql_NullValue_Sqlite(t *testing.T) {
// 	name := "TestSql_NullValue_Sqlite"
// 	dbtype := "sqlite"
// 	urlMap := sqlGetUrlFromEnv()
// 	info, ok := urlMap[dbtype]
// 	if !ok {
// 		t.Skipf("%s skipped", name)
// 	}
// 	sqlc, err := newSqlConnectSqlite(info.driver, info.url, timezoneSql, -1, nil)
// 	if err != nil {
// 		t.Fatalf("%s failed: error [%s]", name, err)
// 	} else if sqlc == nil {
// 		t.Fatalf("%s failed: nil", name)
// 	}
//
// 	sqlColTypes := []string{"VARCHAR(8)", "INT", "REAL", "VARCHAR(64)", "DATETIME"}
// 	_testSqlNullValue(t, name, dbtype, sqlc, sqlColTypes)
// }
