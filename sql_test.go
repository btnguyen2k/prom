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
	timezoneSql  = "Asia/Kabul"
	timezoneSql2 = "Europe/Rome"
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
		if sqlc.GetDriver() != info.driver {
			t.Fatalf("%s failed: expected driver %#v but received %#v", name+"/"+k, info.driver, sqlc.GetDriver())
		}
		sqlc.SetDriver("_default_")
		if sqlc.GetDriver() != "_default_" {
			t.Fatalf("%s failed: expected driver %#v but received %#v", name+"/"+k, "_default_", sqlc.GetDriver())
		}
		// if sqlc.GetDsn() != info.dsn {
		// 	t.Fatalf("%s failed: expected dsn %#v but received %#v", name+"/"+k, info.dsn, sqlc.GetDsn())
		// }
		sqlc.SetDsn("_default_")
		if sqlc.GetDsn() != "_default_" {
			t.Fatalf("%s failed: expected dsn %#v but received %#v", name+"/"+k, "_default_", sqlc.GetDsn())
		}
		sqlc.SetTimeoutMs(1234)
		if sqlc.GetTimeoutMs() != 1234 {
			t.Fatalf("%s failed: expected timeout %#v but received %#v", name+"/"+k, 1234, sqlc.GetDsn())
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
		if sqlc.GetSqlPoolOptions() == nil {
			t.Fatalf("%s failed: sqlPoolOptions is nil", name+"/"+k)
		}
		sqlc.SetSqlPoolOptions(nil)
		if sqlc.GetSqlPoolOptions() != nil {
			t.Fatalf("%s failed: expect sqlPoolOptions to be nil", name+"/"+k)
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

var sqlColNamesTestDataTypeInt = []string{"id",
	"data_int", "data_integer", "data_decimal", "data_number", "data_numeric",
	"data_tinyint", "data_smallint", "data_mediumint", "data_bigint",
	"data_int1", "data_int2", "data_int4", "data_int8"}

func _testSqlDataTypeInt(t *testing.T, name, dbtype string, sqlc *SqlConnect, colTypes []string) {
	tblName := "tbl_test"
	rand.Seed(time.Now().UnixNano())

	colNameList := sqlColNamesTestDataTypeInt

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

	// doNOT use DECIMAL(*,0), NUMBER(*,0) or NUMERIC(*,0) as integer
	sqlColTypes := []string{"NVARCHAR(8)",
		// "INT", "INTEGER", "DECIMAL(32,0)", "NUMBER(32,0)", "NUMERIC(32,0)",
		"INT", "INTEGER", "INTEGER", "INT", "INT",
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

func _testSqlDataTypeReal(t *testing.T, name, dbtype string, sqlc *SqlConnect, colTypes []string) {
	tblName := "tbl_test"
	rand.Seed(time.Now().UnixNano())

	colNameList := sqlColNamesTestDataTypeReal

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

func _testSqlDataTypeRealZero(t *testing.T, name, dbtype string, sqlc *SqlConnect, colTypes []string) {
	tblName := "tbl_test"
	rand.Seed(time.Now().UnixNano())

	colNameList := sqlColNamesTestDataTypeReal

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

func TestSql_DataTypeRealZero_Cosmos(t *testing.T) {
	name := "TestSql_DataTypeRealZero_Cosmos"
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

	_testSqlDataTypeRealZero(t, name, "cosmos", sqlc, nil)
}

func TestSql_DataTypeRealZero_Mssql(t *testing.T) {
	name := "TestSql_DataTypeRealZero_Mssql"
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
	_testSqlDataTypeRealZero(t, name, dbtype, sqlc, sqlColTypes)
}

func TestSql_DataTypeRealZero_Mysql(t *testing.T) {
	name := "TestSql_DataTypeRealZero_Mysql"
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
	_testSqlDataTypeRealZero(t, name, dbtype, sqlc, sqlColTypes)
}

func TestSql_DataTypeRealZero_Oracle(t *testing.T) {
	name := "TestSql_DataTypeRealZero_Oracle"
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
	_testSqlDataTypeRealZero(t, name, dbtype, sqlc, sqlColTypes)
}

func TestSql_DataTypeRealZero_Pgsql(t *testing.T) {
	name := "TestSql_DataTypeRealZero_Pgsql"
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
	_testSqlDataTypeRealZero(t, name, dbtype, sqlc, sqlColTypes)
}

func TestSql_DataTypeRealZero_Sqlite(t *testing.T) {
	name := "TestSql_DataTypeRealZero_Sqlite"
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
	_testSqlDataTypeRealZero(t, name, dbtype, sqlc, sqlColTypes)
}

/*----------------------------------------------------------------------*/

var sqlColNamesTestDataTypeString = []string{"id",
	"data_char", "data_varchar", "data_binchar", "data_text",
	"data_uchar", "data_uvchar", "data_utext",
	"data_clob", "data_uclob", "data_blob"}

func _testSqlDataTypeString(t *testing.T, name, dbtype string, sqlc *SqlConnect, colTypes []string) {
	tblName := "tbl_test"
	rand.Seed(time.Now().UnixNano())

	colNameList := sqlColNamesTestDataTypeString

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

var sqlColNamesTestDataTypeMoney = []string{"id",
	"data_money2", "data_money4", "data_money6", "data_money8"}

func _testSqlDataTypeMoney(t *testing.T, name, dbtype string, sqlc *SqlConnect, colTypes []string) {
	tblName := "tbl_test"
	rand.Seed(time.Now().UnixNano())

	colNameList := sqlColNamesTestDataTypeMoney

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

	sqlColTypes := []string{"NVARCHAR2(8)",
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

/*----------------------------------------------------------------------*/

func _startOfDay(t time.Time) time.Time {
	arr := []byte(t.Format(time.RFC3339))
	arr[11], arr[12], arr[14], arr[15], arr[17], arr[18] = '0', '0', '0', '0', '0', '0'
	t, _ = time.ParseInLocation(time.RFC3339, string(arr), t.Location())
	return t
}

var sqlColNamesTestDataTypeDatetime = []string{"id",
	"data_date", "data_datez",
	"data_time", "data_timez",
	"data_datetime", "data_datetimez",
	"data_duration"}

func _testSqlDataTypeDatetime(t *testing.T, name, dbtype string, sqlc *SqlConnect, colTypes []string) {
	tblName := "tbl_test"
	rand.Seed(time.Now().UnixNano())

	colNameList := sqlColNamesTestDataTypeDatetime

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
		dataDate      time.Time
		dataDatez     time.Time
		dataTime      time.Time
		dataTimez     time.Time
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
	sql += _generatePlaceholders(len(colNameList), dbtype) + ")"
	for i := 1; i <= numRows; i++ {
		vDatetime, _ := time.ParseInLocation("2006-01-02T15:04:05", "2021-02-28T23:24:25", LOC)
		vDatetime = vDatetime.Add(time.Duration(rand.Intn(1024)) * time.Minute)
		row := Row{
			id:            fmt.Sprintf("%03d", i),
			dataDate:      _startOfDay(vDatetime),
			dataDatez:     _startOfDay(vDatetime),
			dataTime:      vDatetime,
			dataTimez:     vDatetime,
			dataDatetime:  vDatetime,
			dataDatetimez: vDatetime,
			dataDuration:  time.Duration(rand.Int63n(1024)) * time.Second,
		}
		rowArr = append(rowArr, row)
		params := []interface{}{row.id, row.dataDate, row.dataDatez, row.dataTime, row.dataTimez,
			row.dataDatetime, row.dataDatetimez, row.dataDuration}
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
			layout := "2006-01-02"
			e := expected.dataDate
			f := colNameList[1]
			v, ok := row[f].(time.Time)
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
				t, err := time.ParseInLocation(time.RFC3339, row[f].(string), LOC)
				v = t
				ok = err == nil
				// } else {
				// 	e = _startOfDay(e)
			}
			if eloc, vloc := e.Location(), v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
				t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", name, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
			}
			if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
				t.Fatalf("%s failed: [%s]\nexpected %#v/%#v(%T)\nbut received %#v/%#v(%T) (Ok: %#v)", name,
					row["id"].(string)+"/"+f, estr, e.Format(time.RFC3339), e,
					vstr, v.Format(time.RFC3339), row[f], ok)
			}
		}
		{
			layout := "2006-01-02"
			e := expected.dataDatez
			f := colNameList[2]
			v, ok := row[f].(time.Time)
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
				t, err := time.ParseInLocation(time.RFC3339, row[f].(string), LOC)
				v = t
				ok = err == nil
				// } else {
				// 	e = _startOfDay(e)
			}
			if eloc, vloc := e.Location(), v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
				t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", name, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
			}
			if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
				t.Fatalf("%s failed: [%s]\nexpected %#v/%#v(%T)\nbut received %#v/%#v(%T) (Ok: %#v)", name,
					row["id"].(string)+"/"+f, estr, e.Format(time.RFC3339), e,
					vstr, v.Format(time.RFC3339), row[f], ok)
			}
		}
		{
			layout := "15:04:05"
			e := expected.dataTime
			f := colNameList[3]
			v, ok := row[f].(time.Time)
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
				t, err := time.ParseInLocation(time.RFC3339, row[f].(string), LOC)
				v = t
				ok = err == nil
			}
			if eloc, vloc := e.Location(), v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
				t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", name, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
			}
			if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (Ok: %#v)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], ok)
			}
		}
		{
			layout := "15:04:05"
			e := expected.dataTimez
			f := colNameList[4]
			v, ok := row[f].(time.Time)
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
				t, err := time.ParseInLocation(time.RFC3339, row[f].(string), LOC)
				v = t
				ok = err == nil
			}
			if eloc, vloc := e.Location(), v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
				t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", name, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
			}
			if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (Ok: %#v)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], ok)
			}
		}
		{
			layout := time.RFC3339
			e := expected.dataDatetime
			f := colNameList[5]
			v, ok := row[f].(time.Time)
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
				t, err := time.ParseInLocation(time.RFC3339, row[f].(string), LOC)
				v = t
				ok = err == nil
			}
			if eloc, vloc := e.Location(), v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
				t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", name, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
			}
			if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (Ok: %#v)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], ok)
			}
		}
		{
			layout := time.RFC3339
			e := expected.dataDatetimez
			f := colNameList[6]
			v, ok := row[f].(time.Time)
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
				t, err := time.ParseInLocation(time.RFC3339, row[f].(string), LOC)
				v = t
				ok = err == nil
			}
			if eloc, vloc := e.Location(), v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
				t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", name, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
			}
			if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
				t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (Ok: %#v)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], ok)
			}
		}
		{
			e := expected.dataDuration
			f := colNameList[7]
			v, err := _toIntIfInteger(row[f])
			if dbtype == "cosmos" || dbtype == "cosmosdb" {
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

	_testSqlDataTypeDatetime(t, name, "cosmos", sqlc, nil)
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

	sqlColTypes := []string{"NVARCHAR(8)",
		"DATE", "DATE", "TIME", "TIME", "DATETIME2", "DATETIMEOFFSET", "BIGINT"}
	_testSqlDataTypeDatetime(t, name, dbtype, sqlc, sqlColTypes)
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

	sqlColTypes := []string{"VARCHAR(8)",
		"DATE", "DATE", "TIME", "TIME", "DATETIME", "TIMESTAMP", "BIGINT"}
	_testSqlDataTypeDatetime(t, name, dbtype, sqlc, sqlColTypes)
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

	sqlColTypes := []string{"NVARCHAR2(8)",
		"DATE", "TIMESTAMP(0) WITH TIME ZONE", "DATE", "TIMESTAMP(0) WITH TIME ZONE",
		"DATE", "TIMESTAMP(0) WITH TIME ZONE", "INTERVAL DAY TO SECOND"}
	_testSqlDataTypeDatetime(t, name, dbtype, sqlc, sqlColTypes)
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

	sqlColTypes := []string{"VARCHAR(8)",
		"DATE", "DATE", "TIME(0)", "TIME(0) WITH TIME ZONE",
		"TIMESTAMP(0)", "TIMESTAMP(0) WITH TIME ZONE", "BIGINT"}
	_testSqlDataTypeDatetime(t, name, dbtype, sqlc, sqlColTypes)
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

	sqlColTypes := []string{"VARCHAR(8)",
		"DATE", "DATE", "TIME", "TIME", "DATETIME", "DATETIME", "BIGINT"}
	_testSqlDataTypeDatetime(t, name, dbtype, sqlc, sqlColTypes)
}

/*----------------------------------------------------------------------*/

var sqlColNamesTestDataTypeNull = []string{"id",
	"data_int", "data_float", "data_string", "data_money",
	"data_date", "data_time", "data_datetime", "data_duration"}

func _testSqlDataTypeNull(t *testing.T, name, dbtype string, sqlc *SqlConnect, colTypes []string) {
	tblName := "tbl_test"
	rand.Seed(time.Now().UnixNano())

	colNameList := sqlColNamesTestDataTypeNull

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
	sql += _generatePlaceholders(len(colNameList), dbtype) + ")"
	for i := 1; i <= numRows; i++ {
		vInt := rand.Int63n(1024)
		vFloat := math.Round(rand.Float64()*1e3) / 1e3
		vString := strconv.Itoa(rand.Intn(1024))
		vMoney := math.Round(rand.Float64()*1e2) / 1e2
		vDatetime, _ := time.ParseInLocation("2006-01-02T15:04:05", "2021-02-28T23:24:25", LOC)
		vDatetime = vDatetime.Add(time.Duration(rand.Intn(1024)) * time.Minute)
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
			vDate := _startOfDay(vDatetime)
			row.dataDate = &vDate
			row.dataTime = &vDatetime
			row.dataDatetime = &vDatetime
		}
		if i%6 == 0 {
			row.dataDuration = &vDuration
		}
		rowArr = append(rowArr, row)
		params := []interface{}{row.id, row.dataInt, row.dataFloat, row.dataString, row.dataMoney, row.dataDate, row.dataTime, row.dataDatetime, row.dataDuration}
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
			f := colNameList[1]
			if (i+1)%2 == 0 {
				e := expected.dataInt
				v, err := _toIntIfInteger(row[f])
				if dbtype == "cosmos" || dbtype == "cosmosdb" {
					v, err = _toIntIfNumber(row[f])
				}
				if err != nil || v != *e {
					t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
				}
			} else if row[f] != (*int64)(nil) && ((dbtype == "cosmos" || dbtype == "cosmosdb") && row[f] != nil) {
				t.Fatalf("%s failed: [%s] expected nil but received %#v(%T)", name, row["id"].(string)+"/"+f, row[f], row[f])
			}
		}
		{
			f := colNameList[2]
			if (i+1)%2 == 0 {
				e := expected.dataFloat
				v, err := _toFloatIfReal(row[f])
				if dbtype == "cosmos" || dbtype == "cosmosdb" {
					v, err = _toFloatIfNumber(row[f])
				}
				if estr, vstr := fmt.Sprintf("%.3f", *e), fmt.Sprintf("%.3f", v); err != nil || vstr != estr {
					t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
				}
			} else if row[f] != (*float64)(nil) && ((dbtype == "cosmos" || dbtype == "cosmosdb") && row[f] != nil) {
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
			} else if row[f] != (*string)(nil) && ((dbtype == "cosmos" || dbtype == "cosmosdb") && row[f] != nil) {
				t.Fatalf("%s failed: [%s] expected nil but received %#v(%T)", name, row["id"].(string)+"/"+f, row[f], row[f])
			}
		}
		{
			f := colNameList[4]
			if (i+1)%4 == 0 {
				e := expected.dataMoney
				v, err := _toFloatIfReal(row[f])
				if dbtype == "cosmos" || dbtype == "cosmosdb" {
					v, err = _toFloatIfNumber(row[f])
				}
				if estr, vstr := fmt.Sprintf("%.2f", *e), fmt.Sprintf("%.2f", v); err != nil || vstr != estr {
					t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, estr, e, vstr, row[f], err)
				}
			} else if row[f] != (*float64)(nil) && ((dbtype == "cosmos" || dbtype == "cosmosdb") && row[f] != nil) {
				t.Fatalf("%s failed: [%s] expected nil but received %#v(%T)", name, row["id"].(string)+"/"+f, row[f], row[f])
			}
		}
		{
			f := colNameList[5]
			if (i+1)%5 == 0 {
				layout := "2006-01-02"
				e := expected.dataDate
				v, ok := row[f].(time.Time)
				if dbtype == "cosmos" || dbtype == "cosmosdb" {
					t, err := time.ParseInLocation(time.RFC3339, row[f].(string), LOC)
					v = t
					ok = err == nil
				}
				if eloc, vloc := e.Location(), v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
					t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", name, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
				}
				if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
					t.Fatalf("%s failed: [%s]\nexpected %#v/%#v(%T)\nbut received %#v/%#v(%T) (Ok: %#v)", name,
						row["id"].(string)+"/"+f, estr, e.Format(time.RFC3339), e,
						vstr, v.Format(time.RFC3339), row[f], ok)
				}
			} else if row[f] != (*time.Time)(nil) && ((dbtype == "cosmos" || dbtype == "cosmosdb") && row[f] != nil) {
				t.Fatalf("%s failed: [%s] expected nil but received %#v(%T)", name, row["id"].(string)+"/"+f, row[f], row[f])
			}
		}
		{
			f := colNameList[6]
			if (i+1)%5 == 0 {
				layout := "15:04:05"
				e := expected.dataTime
				v, ok := row[f].(time.Time)
				if dbtype == "cosmos" || dbtype == "cosmosdb" {
					t, err := time.ParseInLocation(time.RFC3339, row[f].(string), LOC)
					v = t
					ok = err == nil
				}
				if eloc, vloc := e.Location(), v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
					t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", name, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
				}
				if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
					t.Fatalf("%s failed: [%s]\nexpected %#v/%#v(%T)\nbut received %#v/%#v(%T) (Ok: %#v)", name,
						row["id"].(string)+"/"+f, estr, e.Format(time.RFC3339), e,
						vstr, v.Format(time.RFC3339), row[f], ok)
				}
			} else if row[f] != (*time.Time)(nil) && ((dbtype == "cosmos" || dbtype == "cosmosdb") && row[f] != nil) {
				t.Fatalf("%s failed: [%s] expected nil but received %#v(%T)", name, row["id"].(string)+"/"+f, row[f], row[f])
			}
		}
		{
			f := colNameList[7]
			if (i+1)%5 == 0 {
				layout := time.RFC3339
				e := expected.dataDatetime
				v, ok := row[f].(time.Time)
				if dbtype == "cosmos" || dbtype == "cosmosdb" {
					t, err := time.ParseInLocation(time.RFC3339, row[f].(string), LOC)
					v = t
					ok = err == nil
				}
				if eloc, vloc := e.Location(), v.Location(); eloc == nil || vloc == nil || eloc.String() != vloc.String() {
					t.Fatalf("%s failed: [%s] expected %s(%T) but received %s(%T)", name, row["id"].(string)+"/"+f, eloc, eloc, vloc, vloc)
				}
				if estr, vstr := e.In(LOC2).Format(layout), v.In(LOC2).Format(layout); !ok || vstr != estr {
					t.Fatalf("%s failed: [%s]\nexpected %#v/%#v(%T)\nbut received %#v/%#v(%T) (Ok: %#v)", name,
						row["id"].(string)+"/"+f, estr, e.Format(time.RFC3339), e,
						vstr, v.Format(time.RFC3339), row[f], ok)
				}
			} else if row[f] != (*time.Time)(nil) && ((dbtype == "cosmos" || dbtype == "cosmosdb") && row[f] != nil) {
				t.Fatalf("%s failed: [%s] expected nil but received %#v(%T)", name, row["id"].(string)+"/"+f, row[f], row[f])
			}
		}
		{
			f := colNameList[8]
			if (i+1)%6 == 0 {
				e := expected.dataDuration
				v, err := _toIntIfInteger(row[f])
				if dbtype == "cosmos" || dbtype == "cosmosdb" {
					v, err = _toIntIfNumber(row[f])
				}
				if err != nil || v != int64(*e) {
					t.Fatalf("%s failed: [%s] expected %#v(%T) but received %#v(%T) (error: %s)", name, row["id"].(string)+"/"+f, e, e, row[f], row[f], err)
				}
			} else if row[f] != (*int64)(nil) && ((dbtype == "cosmos" || dbtype == "cosmosdb") && row[f] != nil) {
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

	_testSqlDataTypeNull(t, name, "cosmos", sqlc, nil)
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
	_testSqlDataTypeNull(t, name, dbtype, sqlc, sqlColTypes)
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
	_testSqlDataTypeNull(t, name, dbtype, sqlc, sqlColTypes)
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
	_testSqlDataTypeNull(t, name, dbtype, sqlc, sqlColTypes)
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
	_testSqlDataTypeNull(t, name, dbtype, sqlc, sqlColTypes)
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
	_testSqlDataTypeNull(t, name, dbtype, sqlc, sqlColTypes)
}
