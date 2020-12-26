package prom

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/btnguyen2k/consu/reddo"
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
	sqlc, err := NewSqlConnectWithFlavor(driver, url, timeoutMs, poolOptions, FlavorCosmosDb)
	if err == nil && sqlc != nil {
		loc, _ := time.LoadLocation(timezone)
		sqlc.SetLocation(loc)
	}
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

// func TestSqlConnect_FastFailed(t *testing.T) {
// 	name := "TestSqlConnect_FastFailed"
//
// 	type testInfo struct {
// 		driver, dsn string
// 		dbFlavor    DbFlavor
// 	}
// 	testDataMap := map[string]testInfo{
// 		"sqlite": {driver: "sqlite3", dsn: "./should_not_exist/temp.db", dbFlavor: FlavorSqlite},
// 		"mssql":  {driver: "sqlserver", dsn: "sqlserver://sa:secret@localhost:1234?database=tempdb&connection+timeout=1&dial+timeout=1", dbFlavor: FlavorMsSql},
// 		"mysql":  {driver: "mysql", dsn: "test:test@tcp(localhost:1234)/test?charset=utf8mb4,utf8&parseTime=false", dbFlavor: FlavorMySql},
// 		"oracle": {driver: "godror", dsn: "test/test@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=localhost)(PORT=1234)))(CONNECT_DATA=(SID=ORCLCDB)))", dbFlavor: FlavorOracle},
// 		"pgsql":  {driver: "pgx", dsn: "postgres://test:test@localhost:1234/test?sslmode=disable&client_encoding=UTF-8&application_name=prom&connect_timeout=1", dbFlavor: FlavorPgSql},
// 	}
// 	timeoutMs := 1024
// 	for k, info := range testDataMap {
// 		var sqlc *SqlConnect
// 		switch k {
// 		case "sqlite", "sqlite3":
// 			sqlc, _ = newSqlConnectSqlite(info.driver, info.dsn, timezoneSql, timeoutMs, nil)
// 		case "mssql":
// 			sqlc, _ = newSqlConnectMssql(info.driver, info.dsn, timezoneSql, timeoutMs, nil)
// 		case "mysql":
// 			sqlc, _ = newSqlConnectMysql(info.driver, info.dsn, timezoneSql, timeoutMs, nil)
// 		case "oracle":
// 			sqlc, _ = newSqlConnectOracle(info.driver, info.dsn, timezoneSql, timeoutMs, nil)
// 		case "pgsql":
// 			sqlc, _ = newSqlConnectPgsql(info.driver, info.dsn, timezoneSql, timeoutMs, nil)
// 		default:
// 			t.Fatalf("%s failed: unknown database type [%s]", name, k)
// 		}
//
// 		tstart := time.Now()
// 		err := sqlc.Ping(nil)
// 		if err == nil {
// 			t.Fatalf("%s/%s failed: the operation should not success", name, k)
// 		}
// 		d := time.Duration(time.Now().UnixNano() - tstart.UnixNano())
// 		dmax := time.Duration(float64(time.Duration(timeoutMs)*time.Millisecond) * 1.5)
// 		if d > dmax {
// 			t.Fatalf("%s/%s failed: operation is expected to fail within %#v ms but in fact %#v ms", name, k, dmax/1E6, d/1E6)
// 		}
// 	}
// }

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
		case "pgsql":
			sqlc, err = newSqlConnectPgsql(info.driver, info.url, timezoneSql, 10000, nil)
		default:
			t.Fatalf("%s failed: unknown database type [%s]", name, k)
		}
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/"+k, err)
		} else if sqlc == nil {
			t.Fatalf("%s failed: nil", name+"/"+k)
		}
		err = sqlInitTable(sqlc, testSqlTableName, k, true)
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/sqlInitTable/"+k, err)
		}
		sqlSelect := "SELECT * FROM %s WHERE userid < '%s'"
		i := rand.Intn(10)
		id := strconv.Itoa(i)
		dbRows, err := sqlc.GetDB().Query(fmt.Sprintf(sqlSelect, testSqlTableName, id))
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/Query/"+k, err)
		} else if dbRows == nil {
			t.Fatalf("%s failed: nil", name+"/Query/"+k)
		}
		dataRows, err := sqlc.FetchRows(dbRows)
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/FetchRows/"+k, err)
		} else if dataRows == nil {
			t.Fatalf("%s failed: nil", name+"/FetchRows/"+k)
		} else if len(dataRows) != i {
			t.Fatalf("%s failed: expected %d fields but received %d", name+"/FetchRows/"+k, i, len(dataRows))
		}
	}
}

func TestSqlConnect_FetchRowsCallback(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	name := "TestSqlConnect_FetchRowsCallback"
	urlMap := sqlGetUrlFromEnv()
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
		case "pgsql":
			sqlc, err = newSqlConnectPgsql(info.driver, info.url, timezoneSql, 10000, nil)
		default:
			t.Fatalf("%s failed: unknown database type [%s]", name, k)
		}
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/"+k, err)
		} else if sqlc == nil {
			t.Fatalf("%s failed: nil", name+"/"+k)
		}
		err = sqlInitTable(sqlc, testSqlTableName, k, true)
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/sqlInitTable/"+k, err)
		}
		sqlSelect := "SELECT * FROM %s WHERE userid < '%s'"
		i := rand.Intn(10)
		id := strconv.Itoa(i)
		dbRows, err := sqlc.GetDB().Query(fmt.Sprintf(sqlSelect, testSqlTableName, id))
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/Query/"+k, err)
		} else if dbRows == nil {
			t.Fatalf("%s failed: nil", name+"/Query/"+k)
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
			t.Fatalf("%s failed: expected %d fields but received %d", name+"/FetchRowsCallback/"+k, i, len(dataRows))
		}
	}
}

var sqlColNames = []string{"id", "data_str", "data_bool",
	"data_int_tiny", "data_int_small", "data_int_medium", "data_int", "data_int_big",
	"data_float1", "data_float2", "data_float3", "data_float4",
	"data_fix1", "data_fix2", "data_fix3", "data_fix4",
	"data_time", "data_timez",
	"data_date", "data_datez",
	"data_datetime", "data_datetimez",
	"data_timestamp", "data_timestampz"}

func _testSqlDataTye(t *testing.T, name string, sqlc *SqlConnect, colTypes []string) {
	tblName := "tbl_test"
	rand.Seed(time.Now().UnixNano())
	{
		sqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE %s", tblName))

		sql := fmt.Sprintf("CREATE TABLE %s (", tblName)
		for i := range sqlColNames {
			sql += sqlColNames[i] + " " + colTypes[i] + ","
		}
		sql += "PRIMARY KEY(id))"
		_, err := sqlc.GetDB().Exec(sql)
		if err != nil {
			t.Fatalf("%s failed: %s\n%s", name, err, sql)
		}
	}

	timeLayoutFull := "2006-01-02T15:04:05-07:00"
	timeLayoutTime := "15:04:05-07:00"
	timeLayoutDate := "2006-01-02"
	loc, _ := time.LoadLocation(timezoneSql)
	now := time.Now().Round(time.Second).In(loc)
	// fmt.Printf("Now: %s / %s\n", time.Now().Format(timeLayoutFull), now.Format(timeLayoutFull))
	type Row struct {
		id             int
		dataStr        string
		dataBool       string
		dataIntTiny    int8
		dataIntSmall   int16
		dataIntMedium  int
		dataInt        int32
		dataIntBig     int64
		dataFloat1     float64
		dataFloat2     float64
		dataFloat3     float64
		dataFloat4     float64
		dataFix1       float64
		dataFix2       float64
		dataFix3       float64
		dataFix4       float64
		dataTime       time.Time
		dataTimez      time.Time
		dataDate       time.Time
		dataDatez      time.Time
		dataDatetime   time.Time
		dataDatetimez  time.Time
		dataTimestamp  time.Time
		dataTimestampz time.Time
	}
	rowArr := make([]Row, 0)
	numRows := 100

	// insert some rows
	{
		sql := fmt.Sprintf("INSERT INTO %s (", tblName)
		sql += strings.Join(sqlColNames, ",")
		sql += ") VALUES ("
		switch sqlc.flavor {
		case FlavorMySql, FlavorSqlite:
			sql += strings.Repeat("?,", len(sqlColNames)-1) + "?)"
		case FlavorPgSql:
			for k := range sqlColNames {
				sql += "$" + strconv.Itoa(k+1) + ","
			}
			sql = sql[0:len(sql)-1] + ")"
		case FlavorMsSql:
			for k := range sqlColNames {
				sql += "@p" + strconv.Itoa(k+1) + ","
			}
			sql = sql[0:len(sql)-1] + ")"
		case FlavorOracle:
			for k := range sqlColNames {
				sql += ":" + strconv.Itoa(k+1) + ","
			}
			sql = sql[0:len(sql)-1] + ")"
		}
		for i := 1; i <= numRows; i++ {
			// ti := now.Add(time.Duration(i) * time.Minute)
			ti := now
			vInt := rand.Int63()
			row := Row{
				id:             i,
				dataStr:        ti.Format(timeLayoutFull),
				dataBool:       strconv.Itoa(int(vInt % 2)),
				dataIntTiny:    int8(vInt%2 ^ 8),
				dataIntSmall:   int16(vInt%2 ^ 16),
				dataIntMedium:  int(vInt%2 ^ 24),
				dataInt:        int32(vInt%2 ^ 32),
				dataIntBig:     vInt,
				dataFloat1:     rand.Float64(),
				dataFloat2:     rand.Float64(),
				dataFloat3:     rand.Float64(),
				dataFloat4:     rand.Float64(),
				dataFix1:       rand.Float64(),
				dataFix2:       rand.Float64(),
				dataFix3:       rand.Float64(),
				dataFix4:       rand.Float64(),
				dataTime:       ti,
				dataTimez:      ti,
				dataDate:       ti,
				dataDatez:      ti,
				dataDatetime:   ti,
				dataDatetimez:  ti,
				dataTimestamp:  ti,
				dataTimestampz: ti,
			}
			rowArr = append(rowArr, row)
			_, err := sqlc.GetDB().Exec(sql, row.id, row.dataStr, row.dataBool,
				row.dataIntTiny, row.dataIntSmall, row.dataIntMedium, row.dataInt, row.dataIntBig,
				row.dataFloat1, row.dataFloat2, row.dataFloat3, row.dataFloat4,
				row.dataFix1, row.dataFix2, row.dataFix3, row.dataFix4,
				row.dataTime, row.dataTimez, row.dataDate, row.dataDatez,
				row.dataDatetime, row.dataDatetimez, row.dataTimestamp, row.dataTimestampz)
			if err != nil {
				t.Fatalf("%s failed: %s", name, err)
			}
		}
	}

	// query some rows
	{
		id := rand.Intn(numRows) + 1
		sql := fmt.Sprintf("SELECT * FROM %s WHERE id>=%d ORDER BY id", tblName, id)
		dbRows, err := sqlc.GetDB().Query(sql)
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
		expected := rowArr[id-1]
		row := rows[0]
		// fmt.Println(id, row)
		for k, v := range row {
			// transform to lower-cases
			row[strings.ToLower(k)] = v
		}
		{
			f := "id"
			e, _ := reddo.ToInt(expected.id)
			v, _ := reddo.ToInt(row[f])
			if v != e {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e, v)
			}
		}
		{
			f := "data_int_tiny"
			e, _ := reddo.ToInt(expected.dataIntTiny)
			v, _ := reddo.ToInt(row[f])
			if v != e {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e, v)
			}
		}
		{
			f := "data_int_small"
			e, _ := reddo.ToInt(expected.dataIntSmall)
			v, _ := reddo.ToInt(row[f])
			if v != e {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e, v)
			}
		}
		{
			f := "data_int_medium"
			e, _ := reddo.ToInt(expected.dataIntMedium)
			v, _ := reddo.ToInt(row[f])
			if v != e {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e, v)
			}
		}
		{
			f := "data_int"
			e, _ := reddo.ToInt(expected.dataInt)
			v, _ := reddo.ToInt(row[f])
			if v != e {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e, v)
			}
		}
		{
			f := "data_int_big"
			e, _ := reddo.ToInt(expected.dataIntBig)
			v, _ := reddo.ToInt(row[f])
			if v != e {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e, v)
			}
		}
		{
			f := "data_float1"
			e, _ := reddo.ToFloat(expected.dataFloat1)
			v, _ := reddo.ToFloat(row[f])
			if math.Round(v*10e5)/10e5 != math.Round(e*10e5)/10e5 {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e, v)
			}
		}
		{
			f := "data_float2"
			e, _ := reddo.ToFloat(expected.dataFloat2)
			v, _ := reddo.ToFloat(row[f])
			if math.Round(v*10e5)/10e5 != math.Round(e*10e5)/10e5 {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e, v)
			}
		}
		{
			f := "data_float3"
			e, _ := reddo.ToFloat(expected.dataFloat3)
			v, _ := reddo.ToFloat(row[f])
			if math.Round(v*10e8)/10e8 != math.Round(e*10e8)/10e8 {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e, v)
			}
		}
		{
			f := "data_float4"
			e, _ := reddo.ToFloat(expected.dataFloat4)
			v, _ := reddo.ToFloat(row[f])
			if math.Round(v*10e8)/10e8 != math.Round(e*10e8)/10e8 {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e, v)
			}
		}
		{
			f := "data_fix1"
			e, _ := reddo.ToFloat(expected.dataFix1)
			v, _ := reddo.ToFloat(row[f])
			if math.Round(v*10e1)/10e1 != math.Round(e*10e1)/10e1 {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e, v)
			}
		}
		{
			f := "data_fix2"
			e, _ := reddo.ToFloat(expected.dataFix2)
			v, _ := reddo.ToFloat(row[f])
			if math.Round(v*10e3)/10e3 != math.Round(e*10e3)/10e3 {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e, v)
			}
		}
		{
			f := "data_fix3"
			e, _ := reddo.ToFloat(expected.dataFix3)
			v, _ := reddo.ToFloat(row[f])
			if math.Round(v*10e5)/10e5 != math.Round(e*10e5)/10e5 {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e, v)
			}
		}
		{
			f := "data_fix4"
			e, _ := reddo.ToFloat(expected.dataFix4)
			v, _ := reddo.ToFloat(row[f])
			if math.Round(v*10e7)/10e7 != math.Round(e*10e7)/10e7 {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e, v)
			}
		}
		{
			f := "data_str"
			e, _ := reddo.ToString(expected.dataStr)
			v, _ := reddo.ToString(row[f])
			if v != e {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e, v)
			}
		}
		{
			f := "data_bool"
			e, _ := reddo.ToBool(expected.dataBool)
			v, _ := reddo.ToBool(row[f])
			if v != e {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e, v)
			}
		}
		{
			tz := time.Now().In(loc).Format("-07:00")
			for _, f := range []string{"data_time", "data_timez", "data_date", "data_datez", "data_datetime", "data_datetimez", "data_timestamp", "data_timestampz"} {
				v, _ := reddo.ToTime(row[f])
				if v.Format("-07:00") != tz {
					t.Fatalf("%s failed: [%s] expected timezone %#v but received %#v", name, f, tz, v.Format("-07:00"))
				}
			}
		}
		{
			f := "data_time"
			e, _ := reddo.ToTime(expected.dataTime)
			v, _ := reddo.ToTime(row[f])
			if v.In(loc).Format(timeLayoutTime) != e.In(loc).Format(timeLayoutTime) {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e.In(loc).Format(timeLayoutTime), v.In(loc).Format(timeLayoutTime))
			}
		}
		{
			f := "data_timez"
			e, _ := reddo.ToTime(expected.dataTimez)
			v, _ := reddo.ToTime(row[f])
			if v.In(loc).Format(timeLayoutTime) != e.In(loc).Format(timeLayoutTime) {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e.In(loc).Format(timeLayoutTime), v.In(loc).Format(timeLayoutTime))
			}
		}
		{
			f := "data_date"
			e, _ := reddo.ToTime(expected.dataDate)
			v, _ := reddo.ToTime(row[f])
			if v.In(loc).Format(timeLayoutDate) != e.In(loc).Format(timeLayoutDate) {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e.In(loc).Format(timeLayoutDate), v.In(loc).Format(timeLayoutDate))
			}
		}
		{
			f := "data_datez"
			e, _ := reddo.ToTime(expected.dataDatez)
			v, _ := reddo.ToTime(row[f])
			if v.In(loc).Format(timeLayoutDate) != e.In(loc).Format(timeLayoutDate) {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e.In(loc).Format(timeLayoutDate), v.In(loc).Format(timeLayoutDate))
			}
		}
		{
			f := "data_datetime"
			e, _ := reddo.ToTime(expected.dataDatetime)
			v, _ := reddo.ToTime(row[f])
			if v.In(loc).Format(timeLayoutFull) != e.In(loc).Format(timeLayoutFull) {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e.In(loc).Format(timeLayoutFull), v.In(loc).Format(timeLayoutFull))
			}
		}
		{
			f := "data_datetimez"
			e, _ := reddo.ToTime(expected.dataDatetimez)
			v, _ := reddo.ToTime(row[f])
			if v.In(loc).Format(timeLayoutFull) != e.In(loc).Format(timeLayoutFull) {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e.In(loc).Format(timeLayoutFull), v.In(loc).Format(timeLayoutFull))
			}
		}
		{
			f := "data_timestamp"
			e, _ := reddo.ToTime(expected.dataTimestamp)
			v, _ := reddo.ToTime(row[f])
			if v.In(loc).Format(timeLayoutFull) != e.In(loc).Format(timeLayoutFull) {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e.In(loc).Format(timeLayoutFull), v.In(loc).Format(timeLayoutFull))
			}
		}
		{
			f := "data_timestampz"
			e, _ := reddo.ToTime(expected.dataTimestampz)
			v, _ := reddo.ToTime(row[f])
			if v.In(loc).Format(timeLayoutFull) != e.In(loc).Format(timeLayoutFull) {
				t.Fatalf("%s failed: [%s] expected %#v but received %#v", name, f, e.In(loc).Format(timeLayoutFull), v.In(loc).Format(timeLayoutFull))
			}
		}
	}
}

func TestSql_DataType_Mysql(t *testing.T) {
	name := "TestSql_DataType_Mysql"
	urlMap := sqlGetUrlFromEnv()
	info, ok := urlMap["mysql"]
	if !ok {
		t.Skipf("%s skipped", name)
	}
	sqlc, err := newSqlConnectMysql(info.driver, info.url, timezoneSql, -1, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	} else if sqlc == nil {
		t.Fatalf("%s failed: nil", name)
	}

	sqlColTypes := []string{"INT", "VARCHAR(64)", "CHAR(1)",
		"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT",
		"FLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION",
		"NUMERIC(12,2)", "DECIMAL(12,4)", "NUMERIC(12,6)", "DECIMAL(12,8)",
		"TIME", "TIME",
		"DATE", "DATE",
		"DATETIME", "DATETIME",
		"TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"}
	_testSqlDataTye(t, name, sqlc, sqlColTypes)
}

func TestSql_DataType_Pgsql(t *testing.T) {
	name := "TestSql_DataType_Pgsql"
	urlMap := sqlGetUrlFromEnv()
	info, ok := urlMap["pgsql"]
	if !ok {
		t.Skipf("%s skipped", name)
	}
	sqlc, err := newSqlConnectPgsql(info.driver, info.url, timezoneSql, -1, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	} else if sqlc == nil {
		t.Fatalf("%s failed: nil", name)
	}

	sqlColTypes := []string{"INT", "VARCHAR(64)", "CHAR(1)",
		"SMALLINT", "INT2", "INT4", "INT", "BIGINT",
		"FLOAT", "REAL", "DOUBLE PRECISION", "DOUBLE PRECISION",
		"NUMERIC(12,2)", "DECIMAL(12,4)", "NUMERIC(12,6)", "DECIMAL(12,8)",
		"TIME", "TIME WITH TIME ZONE",
		"DATE", "DATE",
		"TIMESTAMP", "TIMESTAMP WITH TIME ZONE",
		"TIMESTAMP", "TIMESTAMP WITH TIME ZONE"}
	_testSqlDataTye(t, name, sqlc, sqlColTypes)
}

func TestSql_DataType_Mssql(t *testing.T) {
	name := "TestSql_DataType_Mssql"
	urlMap := sqlGetUrlFromEnv()
	info, ok := urlMap["mssql"]
	if !ok {
		t.Skipf("%s skipped", name)
	}
	sqlc, err := newSqlConnectMssql(info.driver, info.url, timezoneSql, -1, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	} else if sqlc == nil {
		t.Fatalf("%s failed: nil", name)
	}

	sqlColTypes := []string{"INT", "NVARCHAR(64)", "NCHAR(1)",
		"TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT",
		"FLOAT(24)", "REAL", "FLOAT(53)", "DOUBLE PRECISION",
		"NUMERIC(12,2)", "DECIMAL(12,4)", "NUMERIC(12,6)", "DECIMAL(12,8)",
		"TIME", "TIME",
		"DATE", "DATE",
		"DATETIME", "DATETIMEOFFSET",
		"DATETIME2", "DATETIMEOFFSET"}
	_testSqlDataTye(t, name, sqlc, sqlColTypes)
}

func TestSql_DataType_Oracle(t *testing.T) {
	name := "TestSql_DataType_Oracle"
	urlMap := sqlGetUrlFromEnv()
	info, ok := urlMap["oracle"]
	if !ok {
		t.Skipf("%s skipped", name)
	}
	sqlc, err := newSqlConnectOracle(info.driver, info.url, timezoneSql, -1, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	} else if sqlc == nil {
		t.Fatalf("%s failed: nil", name)
	}

	sqlColTypes := []string{"INT", "NVARCHAR2(64)", "NCHAR(1)",
		"SMALLINT", "SMALLINT", "INT", "INTEGER", "INTEGER",
		"FLOAT", "BINARY_FLOAT", "REAL", "BINARY_DOUBLE",
		"NUMERIC(12,2)", "DECIMAL(12,4)", "NUMERIC(12,6)", "DECIMAL(12,8)",
		"DATE", "TIMESTAMP WITH TIME ZONE",
		"DATE", "TIMESTAMP WITH TIME ZONE",
		"DATE", "TIMESTAMP WITH TIME ZONE",
		"TIMESTAMP", "TIMESTAMP WITH TIME ZONE"}
	_testSqlDataTye(t, name, sqlc, sqlColTypes)
}

func TestSql_DataType_Sqlite(t *testing.T) {
	name := "TestSql_DataType_Sqlite"
	urlMap := sqlGetUrlFromEnv()
	info, ok := urlMap["sqlite"]
	if !ok {
		t.Skipf("%s skipped", name)
	}
	sqlc, err := newSqlConnectSqlite(info.driver, info.url, timezoneSql, -1, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	} else if sqlc == nil {
		t.Fatalf("%s failed: nil", name)
	}

	sqlColTypes := []string{"INT", "VARCHAR(64)", "CHAR(1)",
		"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT",
		"FLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION",
		"NUMERIC(12,2)", "DECIMAL(12,4)", "NUMERIC(12,6)", "DECIMAL(12,8)",
		"TIME", "TIME",
		"DATE", "DATE",
		"DATETIME", "DATETIME",
		"TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"}
	_testSqlDataTye(t, name, sqlc, sqlColTypes)
}
