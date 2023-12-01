package sql_test

import (
	"fmt"
	"github.com/btnguyen2k/prom"
	prom_sql "github.com/btnguyen2k/prom/sql"
	"math"
	"math/rand"
	"os"
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

type _testFailedWithMsgFunc func(msg string)

type _testSetupOrTeardownFunc func(t *testing.T, testName string)

func setupTest(t *testing.T, testName string, extraSetupFunc, extraTeardownFunc _testSetupOrTeardownFunc) func(t *testing.T) {
	if extraSetupFunc != nil {
		extraSetupFunc(t, testName)
	}
	return func(t *testing.T) {
		if extraTeardownFunc != nil {
			extraTeardownFunc(t, testName)
		}
	}
}

func newSqlConnectSqlite(driver, url, timezone string, timeoutMs int, poolOptions *prom_sql.PoolOpts) (*prom_sql.SqlConnect, error) {
	os.Remove(url)
	sqlc, err := prom_sql.NewSqlConnectWithFlavor(driver, url, timeoutMs, poolOptions, prom_sql.FlavorSqlite)
	if err == nil && sqlc != nil {
		loc, _ := time.LoadLocation(timezone)
		sqlc.SetLocation(loc)
	}
	return sqlc, err
}

func newSqlConnectMssql(driver, url, timezone string, timeoutMs int, poolOptions *prom_sql.PoolOpts) (*prom_sql.SqlConnect, error) {
	sqlc, err := prom_sql.NewSqlConnectWithFlavor(driver, url, timeoutMs, poolOptions, prom_sql.FlavorMsSql)
	if err == nil && sqlc != nil {
		loc, _ := time.LoadLocation(timezone)
		sqlc.SetLocation(loc)
	}
	return sqlc, err
}

func newSqlConnectMysql(driver, url, timezone string, timeoutMs int, poolOptions *prom_sql.PoolOpts) (*prom_sql.SqlConnect, error) {
	urlTimezone := strings.ReplaceAll(timezone, "/", "%2f")
	url = strings.ReplaceAll(url, "${loc}", urlTimezone)
	url = strings.ReplaceAll(url, "${tz}", urlTimezone)
	url = strings.ReplaceAll(url, "${timezone}", urlTimezone)
	sqlc, err := prom_sql.NewSqlConnectWithFlavor(driver, url, timeoutMs, poolOptions, prom_sql.FlavorMySql)
	if err == nil && sqlc != nil {
		loc, _ := time.LoadLocation(timezone)
		sqlc.SetLocation(loc)
	}
	sqlc.SetMysqlParseTime(strings.Index(strings.ToLower(url), "parsetime=true") >= 0)
	return sqlc, err
}

func newSqlConnectOracle(driver, url, timezone string, timeoutMs int, poolOptions *prom_sql.PoolOpts) (*prom_sql.SqlConnect, error) {
	sqlc, err := prom_sql.NewSqlConnectWithFlavor(driver, url, timeoutMs, poolOptions, prom_sql.FlavorOracle)
	if err == nil && sqlc != nil {
		loc, _ := time.LoadLocation(timezone)
		sqlc.SetLocation(loc)
	}
	return sqlc, err
}

func newSqlConnectPgsql(driver, url, timezone string, timeoutMs int, poolOptions *prom_sql.PoolOpts) (*prom_sql.SqlConnect, error) {
	sqlc, err := prom_sql.NewSqlConnectWithFlavor(driver, url, timeoutMs, poolOptions, prom_sql.FlavorPgSql)
	if err == nil && sqlc != nil {
		loc, _ := time.LoadLocation(timezone)
		sqlc.SetLocation(loc)
	}
	return sqlc, err
}

func newSqlConnectCosmosdb(driver, url, timezone string, timeoutMs int, poolOptions *prom_sql.PoolOpts) (*prom_sql.SqlConnect, error) {
	sqlc, err := prom_sql.NewSqlConnectWithFlavor(driver, url, timeoutMs, poolOptions, prom_sql.FlavorCosmosDb)
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
	testName := "TestNewSqlConnect"
	driver := "mysql"
	dsn := "test:test@tcp(localhost:3306)/test?charset=utf8mb4,utf8&parseTime=false&loc="
	dsn += strings.ReplaceAll(timezoneSql, "/", "%2f")
	sqlc, err := prom_sql.NewSqlConnectWithFlavor(driver, dsn, 10000, nil, prom_sql.FlavorDefault)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	if sqlc == nil {
		t.Fatalf("%s failed: nil", testName)
	}
}

func TestSqlConnect_GetInfo(t *testing.T) {
	testName := "TestSqlConnect_GetInfo"
	type testInfo struct {
		driver, dsn string
		dbFlavor    prom_sql.DbFlavor
	}
	driver, dsn, dbFlavor := "sqlite3", "./temp/temp.db", prom_sql.FlavorSqlite
	sqlc, err := newSqlConnectSqlite(driver, dsn, timezoneSql, -1, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	} else if sqlc == nil {
		t.Fatalf("%s failed: nil", testName)
	}
	if sqlc.GetDriver() != driver {
		t.Fatalf("%s failed: expected driver %#v but received %#v", testName, driver, sqlc.GetDriver())
	}
	sqlc.SetDriver("_default_")
	if sqlc.GetDriver() != "_default_" {
		t.Fatalf("%s failed: expected driver %#v but received %#v", testName, "_default_", sqlc.GetDriver())
	}
	if sqlc.GetDsn() != dsn {
		t.Fatalf("%s failed: expected dsn %#v but received %#v", testName, dsn, sqlc.GetDsn())
	}
	sqlc.SetDsn("_default_")
	if sqlc.GetDsn() != "_default_" {
		t.Fatalf("%s failed: expected dsn %#v but received %#v", testName, "_default_", sqlc.GetDsn())
	}
	sqlc.SetTimeoutMs(1234)
	if sqlc.GetTimeoutMs() != 1234 {
		t.Fatalf("%s failed: expected timeout %#v but received %#v", testName, 1234, sqlc.GetDsn())
	}
	if sqlc.GetLocation() == nil {
		t.Fatalf("%s failed: GetLocation returns nil", testName)
	}
	if sqlc.GetLocation().String() != timezoneSql {
		t.Fatalf("%s failed: expected timezone %#v but received %#v", testName, timezoneSql, sqlc.GetLocation().String())
	}
	if sqlc.GetDbFlavor() != dbFlavor {
		t.Fatalf("%s failed: expected dbflavor %#v but received %#v", testName, dbFlavor, sqlc.GetDbFlavor())
	}
	sqlc.SetDbFlavor(prom_sql.FlavorDefault)
	if sqlc.GetDbFlavor() != prom_sql.FlavorDefault {
		t.Fatalf("%s failed: expected dbflavor %#v but received %#v", testName, prom_sql.FlavorDefault, sqlc.GetDbFlavor())
	}
	if sqlc.PoolOpts() == nil {
		t.Fatalf("%s failed: PoolOpts is nil", testName)
	}
	sqlc.SetPoolOpts(nil)
	if sqlc.PoolOpts() != nil {
		t.Fatalf("%s failed: expect sqlPoolOptions to be nil", testName)
	}
	for _, value := range []bool{true, false} {
		sqlc.SetMysqlParseTime(value)
		if sqlc.GetMysqlParseTime() != value {
			t.Fatalf("%s failed: expect mysqlParseTime to be %v", testName, value)
		}
	}
	metricsLogger := prom.NewMemoryStoreMetricsLogger(1234)
	if sqlc.MetricsLogger() == metricsLogger {
		t.Fatalf("%s failed: expect a different metricsLogger", testName)
	}
	sqlc.RegisterMetricsLogger(metricsLogger)
	if sqlc.MetricsLogger() != metricsLogger {
		t.Fatalf("%s failed: expect metricsLogger to be set correctly", testName)
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
	testName := "TestSqlConnect_Connection"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if sqlc.GetDB() == nil {
				t.Fatalf("%s failed: GetDB returns nil", testName+"/"+dbtype)
			}
			if err := sqlc.Ping(nil); err != nil {
				t.Fatalf("%s failed: %s", testName+"/Ping/"+dbtype, err)
			}
			if !sqlc.IsConnected() {
				t.Fatalf("%s failed: not connected", testName+"/"+dbtype)
			}
			conn, err := sqlc.Conn(nil)
			if err != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/Conn/"+dbtype, err)
			} else if conn.Close() != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/Conn.Close/"+dbtype, err)
			}
			if err = sqlc.Close(); err != nil {
				t.Fatalf("%s failed: %s", testName+"/Close/"+dbtype, err)
			}
		})
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

func _generatePlaceholders(num int, sqlc *prom_sql.SqlConnect) string {
	result := ""
	for i := 1; i <= num; i++ {
		switch sqlc.GetDbFlavor() {
		case prom_sql.FlavorMsSql:
			result += "@p" + strconv.Itoa(i)
		case prom_sql.FlavorOracle:
			result += ":" + strconv.Itoa(i)
		case prom_sql.FlavorPgSql, prom_sql.FlavorCosmosDb:
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

func _dbtypeStr(flavor prom_sql.DbFlavor) string {
	switch flavor {
	case prom_sql.FlavorSqlite:
		return "sqlite"
	case prom_sql.FlavorMsSql:
		return "mssql"
	case prom_sql.FlavorMySql:
		return "mysql"
	case prom_sql.FlavorOracle:
		return "oracle"
	case prom_sql.FlavorPgSql:
		return "pgsql"
	default:
		return "unknown"
	}
}

func sqlInitTable(sqlc *prom_sql.SqlConnect, table string, insertSampleRows bool) error {
	if sqlc.GetDbFlavor() == prom_sql.FlavorCosmosDb {
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
	partInsertValues = _generatePlaceholders(len(sqlTableColNames)-1, sqlc)

	switch sqlc.GetDbFlavor() {
	case prom_sql.FlavorSqlite, prom_sql.FlavorMsSql, prom_sql.FlavorMySql, prom_sql.FlavorOracle, prom_sql.FlavorPgSql:
		for i, n := 1, len(sqlTableColNames); i < n; i++ {
			partCreateCols += sqlTableColNames[i] + " " + sqlTableColTypes[_dbtypeStr(sqlc.GetDbFlavor())][i-1]
			if i < n-1 {
				partCreateCols += ","
			}
		}
	case prom_sql.FlavorCosmosDb:
		sqlCreate = "CREATE COLLECTION %s WITH pk=/%s%s WITH maxru=10000"
		partCreateCols = ""
		pkName = sqlTableColNames[0]
	default:
		return fmt.Errorf("unknown database type %#v", sqlc.GetDbFlavor())
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
		if sqlc.GetDbFlavor() == prom_sql.FlavorCosmosDb {
			params = append(params, uid)
		}
		if _, err := sqlc.GetDB().Exec(sqlInsert, params...); err != nil {
			return err
		}
	}
	return nil
}

var (
	sqlcList   []*prom_sql.SqlConnect
	sqlc2List  []*prom_sql.SqlConnect
	dbtypeList []string
)

var _setupTestSqlConnect _testSetupOrTeardownFunc = func(t *testing.T, testName string) {
	sqlcList = make([]*prom_sql.SqlConnect, 0)
	sqlc2List = make([]*prom_sql.SqlConnect, 0)
	dbtypeList = make([]string, 0)
	urlMap := sqlGetUrlFromEnv()
	for dbtype, info := range urlMap {
		var sqlc, sqlc2 *prom_sql.SqlConnect
		var err, err2 error
		switch dbtype {
		case "sqlite", "sqlite3":
			sqlc, err = newSqlConnectSqlite(info.driver, info.url, timezoneSql, 10000, nil)
			sqlc2, err2 = newSqlConnectSqlite(info.driver, info.url, timezoneSql2, 10000, nil)
		case "mssql":
			sqlc, err = newSqlConnectMssql(info.driver, info.url, timezoneSql, 10000, nil)
			sqlc2, err2 = newSqlConnectMssql(info.driver, info.url, timezoneSql2, 10000, nil)
		case "mysql":
			sqlc, err = newSqlConnectMysql(info.driver, info.url, timezoneSql, 10000, nil)
			sqlc2, err2 = newSqlConnectMysql(info.driver, info.url, timezoneSql2, 10000, nil)
		case "oracle":
			sqlc, err = newSqlConnectOracle(info.driver, info.url, timezoneSql, 10000, nil)
			sqlc2, err2 = newSqlConnectOracle(info.driver, info.url, timezoneSql2, 10000, nil)
		case "pgsql", "postgresql":
			sqlc, err = newSqlConnectPgsql(info.driver, info.url, timezoneSql, 10000, nil)
			sqlc2, err2 = newSqlConnectPgsql(info.driver, info.url, timezoneSql2, 10000, nil)
		case "cosmos", "cosmosdb":
			sqlc, err = newSqlConnectCosmosdb(info.driver, info.url, timezoneSql, 10000, nil)
			sqlc2, err2 = newSqlConnectCosmosdb(info.driver, info.url, timezoneSql2, 10000, nil)
		default:
			t.Fatalf("%s failed: unknown database type [%s]", testName, dbtype)
		}
		if err != nil {
			t.Fatalf("%s failed: error [%s]", testName+"/"+dbtype, err)
		} else if sqlc == nil {
			t.Fatalf("%s failed: nil", testName+"/"+dbtype)
		}
		if err2 != nil {
			t.Fatalf("%s failed: error [%s]", testName+"/"+dbtype, err2)
		} else if sqlc2 == nil {
			t.Fatalf("%s failed: nil", testName+"/"+dbtype)
		}
		sqlcList = append(sqlcList, sqlc)
		sqlc2List = append(sqlc2List, sqlc2)
		dbtypeList = append(dbtypeList, dbtype)
	}
}

var _teardownTestSqlConnect _testSetupOrTeardownFunc = func(t *testing.T, testName string) {
	for _, sqlc := range sqlcList {
		if sqlc != nil {
			go sqlc.Close()
		}
	}
	for _, sqlc2 := range sqlc2List {
		if sqlc2 != nil {
			go sqlc2.Close()
		}
	}
}

func TestSqlConnect_Unicode(t *testing.T) {
	testName := "TestSqlConnect_Unicode"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if err := sqlInitTable(sqlc, testSqlTableName, false); err != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err)
			}
			strs := []string{"Xin chào, đây là thư viện prom", "您好", "مرحبا", "हैलो", "こんにちは", "សួស្តី", "여보세요", "ສະບາຍດີ", "สวัสดี"}
			for i, str := range strs {
				sqlInsert := "INSERT INTO %s (%s, %s) VALUES (%s)"
				placeholders := _generatePlaceholders(2, sqlc)
				sqlInsert = fmt.Sprintf(sqlInsert, testSqlTableName, sqlTableColNames[1], sqlTableColNames[2], placeholders)
				params := []interface{}{strconv.Itoa(i), str}
				if sqlc.GetDbFlavor() == prom_sql.FlavorCosmosDb {
					params = append(params, strconv.Itoa(i))
				}
				_, err := sqlc.GetDB().Exec(sqlInsert, params...)
				if err != nil {
					t.Fatalf("%s failed: error [%s]", testName+"/insert/"+dbtype, err)
				}

				placeholders = _generatePlaceholders(1, sqlc)
				sqlSelect := "SELECT %s FROM %s WHERE %s=%s"
				if sqlc.GetDbFlavor() == prom_sql.FlavorCosmosDb {
					sqlSelect = "SELECT t.%s FROM %s t WHERE t.%s=%s"
				}
				sqlSelect = fmt.Sprintf(sqlSelect, sqlTableColNames[2], testSqlTableName, sqlTableColNames[0], placeholders)
				params = []interface{}{strconv.Itoa(i)}
				dbRow := sqlc.GetDB().QueryRow(sqlSelect, params...)
				dataRow, err := sqlc.FetchRow(dbRow, 1)
				if err != nil {
					t.Fatalf("%s failed: error [%s]", testName+"/FetchRow/"+dbtype, err)
				} else if dataRow == nil {
					t.Fatalf("%s failed: nil", testName+"/FetchRow/"+dbtype)
				}
				val := dataRow[0]
				if _, ok := val.([]byte); ok {
					val = string(val.([]byte))
				}
				if val != str {
					t.Fatalf("%s failed: expected %#v but received %#v", testName+"/FetchRow/"+dbtype, str, dataRow[0])
				}
			}

			sqlSelect := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s", sqlTableColNames[2], testSqlTableName, sqlTableColNames[0])
			if sqlc.GetDbFlavor() == prom_sql.FlavorCosmosDb {
				sqlSelect = fmt.Sprintf("SELECT t.%s FROM %s t WITH cross_partition=true", sqlTableColNames[2], testSqlTableName)
			}
			params := make([]interface{}, 0)
			dbRows, err := sqlc.GetDB().Query(sqlSelect, params...)
			if err != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/Query/"+dbtype, err)
			} else if dbRows == nil {
				t.Fatalf("%s failed: nil", testName+"/Query/"+dbtype)
			}
			dataRows, err := sqlc.FetchRows(dbRows)
			if err != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/FetchRows/"+dbtype, err)
			} else if dataRows == nil {
				t.Fatalf("%s failed: nil", testName+"/FetchRows/"+dbtype)
			}
			for i, row := range dataRows {
				for col, val := range row {
					row[strings.ToLower(col)] = val
				}
				if row[sqlTableColNames[2]] != strs[i] {
					t.Fatalf("%s failed: expected %#v but received %#v", testName+"/FetchRow/"+dbtype, strs[i], row)
				}
			}
		})
	}
}

func TestSqlConnect_FetchRow(t *testing.T) {
	testName := "TestSqlConnect_FetchRow"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		rand.Seed(time.Now().UnixNano())
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if err := sqlInitTable(sqlc, testSqlTableName, true); err != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err)
			}
			placeholder := _generatePlaceholders(1, sqlc)
			sqlSelect := fmt.Sprintf("SELECT * FROM %s WHERE %s=%s", testSqlTableName, sqlTableColNames[0], placeholder)
			if sqlc.GetDbFlavor() == prom_sql.FlavorCosmosDb {
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
				t.Fatalf("%s failed: error [%s]", testName+"/FetchRow/"+dbtype, err)
			} else if dataRow == nil {
				t.Fatalf("%s failed: nil", testName+"/FetchRow/"+dbtype)
			} else if len(dataRow) != len(sqlTableColNames)-1 {
				t.Fatalf("%s failed: expected %d fields but received %d", testName+"/FetchRow/"+dbtype, len(sqlTableColNames)-1, len(dataRow))
			}
		})
	}
}

func TestSqlConnect_FetchRows(t *testing.T) {
	testName := "TestSqlConnect_FetchRows"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		rand.Seed(time.Now().UnixNano())
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if err := sqlInitTable(sqlc, testSqlTableName, true); err != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err)
			}
			placeholder := _generatePlaceholders(1, sqlc)
			sqlSelect := "SELECT * FROM %s WHERE userid < %s"
			if sqlc.GetDbFlavor() == prom_sql.FlavorCosmosDb {
				sqlSelect = "SELECT CROSS PARTITION * FROM %s t WHERE t.userid < %s"
			}
			i := rand.Intn(10)
			params := []interface{}{strconv.Itoa(i)}
			dbRows, err := sqlc.GetDB().Query(fmt.Sprintf(sqlSelect, testSqlTableName, placeholder), params...)
			if err != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/Query/"+dbtype, err)
			} else if dbRows == nil {
				t.Fatalf("%s failed: nil", testName+"/Query/"+dbtype)
			}
			dataRows, err := sqlc.FetchRows(dbRows)
			if err != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/FetchRows/"+dbtype, err)
			} else if dataRows == nil {
				t.Fatalf("%s failed: nil", testName+"/FetchRows/"+dbtype)
			} else if len(dataRows) != i {
				t.Fatalf("%s failed: expected %d fields but received %d", testName+"/FetchRows/"+dbtype, i, len(dataRows))
			}
		})
	}
}

func TestSqlConnect_FetchRowsCallback(t *testing.T) {
	testName := "TestSqlConnect_FetchRowsCallback"
	teardownTest := setupTest(t, testName, _setupTestSqlConnect, _teardownTestSqlConnect)
	defer teardownTest(t)
	if len(dbtypeList) == 0 {
		t.SkipNow()
	}
	for index, dbtype := range dbtypeList {
		rand.Seed(time.Now().UnixNano())
		sqlc := sqlcList[index]
		t.Run(dbtype, func(t *testing.T) {
			if err := sqlInitTable(sqlc, testSqlTableName, true); err != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/sqlInitTable/"+dbtype, err)
			}
			placeholder := _generatePlaceholders(1, sqlc)
			sqlSelect := "SELECT * FROM %s WHERE userid < %s"
			if sqlc.GetDbFlavor() == prom_sql.FlavorCosmosDb {
				sqlSelect = "SELECT CROSS PARTITION * FROM %s t WHERE t.userid < %s"
			}
			i := rand.Intn(10)
			params := []interface{}{strconv.Itoa(i)}
			dbRows, err := sqlc.GetDB().Query(fmt.Sprintf(sqlSelect, testSqlTableName, placeholder), params...)
			if err != nil {
				t.Fatalf("%s failed: error [%s]", testName+"/Query/"+dbtype, err)
			} else if dbRows == nil {
				t.Fatalf("%s failed: nil", testName+"/Query/"+dbtype)
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
				t.Fatalf("%s failed: expected %d fields but received %d", testName+"/FetchRowsCallback/"+dbtype, i, len(dataRows))
			}
		})
	}
}
