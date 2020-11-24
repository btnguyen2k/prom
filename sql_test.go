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

const (
	timezoneSql = "Asia/Ho_Chi_Minh"
)

func TestNewSqlConnect(t *testing.T) {
	name := "TestNewSqlConnect"
	driver := "mysql"
	dsn := "test:test@tcp(localhost:3306)/test?charset=utf8mb4,utf8&parseTime=false&loc="
	dsn += strings.ReplaceAll(timezoneSql, "/", "%2f")
	sqlc, err := NewSqlConnect(driver, dsn, 10000, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
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
		"oracle": {driver: "godror", dsn: "test/test@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=localhost)(PORT=1521)))(CONNECT_DATA=(SID=ORCLCDB)))", dbFlavor: FlavorOracle},
		"pgsql":  {driver: "pgx", dsn: "postgres://test:test@localhost:5432/test?sslmode=disable&client_encoding=UTF-8&application_name=prom", dbFlavor: FlavorPgSql},
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
		case "pgsql":
			sqlc, err = newSqlConnectPgsql(info.driver, info.dsn, timezoneSql, -1, nil)
		default:
			t.Fatalf("%s failed: unknown database type [%s]", name, k)
		}
		if err != nil {
			t.Fatalf("%s failed: error [%e]", name, err)
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
		case "pgsql":
			sqlc, err = newSqlConnectPgsql(info.driver, info.url, timezoneSql, 10000, nil)
		default:
			t.Fatalf("%s failed: unknown database type [%s]", name, k)
		}
		if err != nil {
			t.Fatalf("%s failed: error [%e]", name+"/"+k, err)
		} else if sqlc == nil {
			t.Fatalf("%s failed: nil", name+"/"+k)
		}
		if sqlc.GetDB() == nil {
			t.Fatalf("%s failed: GetDB returns nil", name+"/"+k)
		}
		if err = sqlc.Ping(nil); err != nil {
			t.Fatalf("%s failed: %e", name+"/Ping/"+k, err)
		}
		if !sqlc.IsConnected() {
			t.Fatalf("%s failed: not connected", name+"/"+k)
		}
		conn, err := sqlc.Conn(nil)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", name+"/Conn/"+k, err)
		} else if conn.Close() != nil {
			t.Fatalf("%s failed: error [%e]", name+"/Conn.Close/"+k, err)
		}
		if err = sqlc.Close(); err != nil {
			t.Fatalf("%s failed: %e", name+"/Close/"+k, err)
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

func sqlInitTable(sqlc *SqlConnect, table, dbtype string) error {
	sqlc.GetDB().Exec(fmt.Sprintf("DROP TABLE %s", table))
	sqlCreate := "CREATE TABLE %s (%s, PRIMARY KEY (%s))"
	sqlInsert := "INSERT INTO %s (%s) VALUES (%s)"
	partCreateCols := ""
	partInsertCols := ""
	partInsertValues := ""
	pkName := sqlTableColNames[0]
	switch dbtype {
	case "sqlite", "sqlite3":
		for i, n := 1, len(sqlTableColNames); i < n; i++ {
			partCreateCols += sqlTableColNames[i] + " " + sqlTableColTypes[dbtype][i-1]
			partInsertCols += sqlTableColNames[i]
			partInsertValues += "?"
			if i < n-1 {
				partCreateCols += ","
				partInsertCols += ","
				partInsertValues += ","
			}
		}
	case "mssql":
		for i, n := 1, len(sqlTableColNames); i < n; i++ {
			partCreateCols += sqlTableColNames[i] + " " + sqlTableColTypes[dbtype][i-1]
			partInsertCols += sqlTableColNames[i]
			partInsertValues += "@p" + strconv.Itoa(i)
			if i < n-1 {
				partCreateCols += ","
				partInsertCols += ","
				partInsertValues += ","
			}
		}
	case "mysql":
		for i, n := 1, len(sqlTableColNames); i < n; i++ {
			partCreateCols += sqlTableColNames[i] + " " + sqlTableColTypes[dbtype][i-1]
			partInsertCols += sqlTableColNames[i]
			partInsertValues += "?"
			if i < n-1 {
				partCreateCols += ","
				partInsertCols += ","
				partInsertValues += ","
			}
		}
	case "oracle":
		for i, n := 1, len(sqlTableColNames); i < n; i++ {
			partCreateCols += sqlTableColNames[i] + " " + sqlTableColTypes[dbtype][i-1]
			partInsertCols += sqlTableColNames[i]
			partInsertValues += ":" + strconv.Itoa(i)
			if i < n-1 {
				partCreateCols += ","
				partInsertCols += ","
				partInsertValues += ","
			}
		}
	case "pgsql":
		for i, n := 1, len(sqlTableColNames); i < n; i++ {
			partCreateCols += sqlTableColNames[i] + " " + sqlTableColTypes[dbtype][i-1]
			partInsertCols += sqlTableColNames[i]
			partInsertValues += "$" + strconv.Itoa(i)
			if i < n-1 {
				partCreateCols += ","
				partInsertCols += ","
				partInsertValues += ","
			}
		}
	default:
		return fmt.Errorf("unknown database type %s", dbtype)
	}
	sqlCreate = fmt.Sprintf(sqlCreate, table, partCreateCols, pkName)
	if _, err := sqlc.GetDB().Exec(sqlCreate); err != nil {
		return err
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
		if _, err := sqlc.GetDB().Exec(sqlInsert, uid, uname, isActived, valInt, valReal, valTime, valTime, valTime, valTime); err != nil {
			return err
		}
	}
	return nil
}

func TestSqlConnect_Unicode(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	name := "TestSqlConnect_Unicode"
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
			t.Fatalf("%s failed: error [%e]", name+"/"+k, err)
		} else if sqlc == nil {
			t.Fatalf("%s failed: nil", name+"/"+k)
		}
		err = sqlInitTable(sqlc, testSqlTableName, k)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", name+"/sqlInitTable/"+k, err)
		}
		sqlc.GetDB().Exec("DELETE FROM " + testSqlTableName)
		strs := []string{"Xin chào, đây là thư viện prom", "您好", "مرحبا", "हैलो", "こんにちは", "សួស្តី", "여보세요", "ສະບາຍດີ", "สวัสดี"}
		for i, str := range strs {
			sql := "INSERT INTO %s (%s, %s) VALUES (%s)"
			placeholders := "?,?"
			switch k {
			case "mssql":
				placeholders = "@P1,@P2"
			case "pgsql":
				placeholders = "$1,$2"
			case "oracle":
				placeholders = ":1,:2"
			}
			sql = fmt.Sprintf(sql, testSqlTableName, sqlTableColNames[1], sqlTableColNames[2], placeholders)
			_, err := sqlc.GetDB().Exec(sql, strconv.Itoa(i), str)
			if err != nil {
				t.Fatalf("%s failed: error [%e]", name+"/insert/"+k, err)
			}

			sqlSelect := "SELECT %s FROM %s WHERE %s='%s'"
			dbRow := sqlc.GetDB().QueryRow(fmt.Sprintf(sqlSelect, sqlTableColNames[2], testSqlTableName, sqlTableColNames[0], strconv.Itoa(i)))
			dataRow, err := sqlc.FetchRow(dbRow, 1)
			if err != nil {
				t.Fatalf("%s failed: error [%e]", name+"/FetchRow/"+k, err)
			} else if dataRow == nil {
				t.Fatalf("%s failed: nil", name+"/FetchRow/"+k)
			}
			val := dataRow[0]
			if _, ok := val.([]byte); ok {
				val = string(val.([]byte))
			}
			if val != str {
				t.Fatalf("%s failed: expected %#v but received %#v", name+"/FetchRow/"+k, str, dataRow[0])
			}
		}

		sqlSelect := "SELECT %s FROM %s ORDER BY %s"
		dbRows, err := sqlc.GetDB().Query(fmt.Sprintf(sqlSelect, sqlTableColNames[2], testSqlTableName, sqlTableColNames[0]))
		if err != nil {
			t.Fatalf("%s failed: error [%e]", name+"/Query/"+k, err)
		} else if dbRows == nil {
			t.Fatalf("%s failed: nil", name+"/Query/"+k)
		}
		dataRows, err := sqlc.FetchRows(dbRows)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", name+"/FetchRows/"+k, err)
		} else if dataRows == nil {
			t.Fatalf("%s failed: nil", name+"/FetchRows/"+k)
		}
		for i, row := range dataRows {
			for col, val := range row {
				row[strings.ToLower(col)] = val
			}
			if row[sqlTableColNames[2]] != strs[i] {
				t.Fatalf("%s failed: expected %#v but received %#v", name+"/FetchRow/"+k, strs[i], row)
			}
		}
	}
}

func TestSqlConnect_FetchRow(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	name := "TestSqlConnect_FetchRow"
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
			t.Fatalf("%s failed: error [%e]", name+"/"+k, err)
		} else if sqlc == nil {
			t.Fatalf("%s failed: nil", name+"/"+k)
		}
		err = sqlInitTable(sqlc, testSqlTableName, k)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", name+"/sqlInitTable/"+k, err)
		}
		sqlSelect := "SELECT * FROM %s WHERE userid='%s'"
		id := strconv.Itoa(rand.Intn(10))
		dbRow := sqlc.GetDB().QueryRow(fmt.Sprintf(sqlSelect, testSqlTableName, id))
		dataRow, err := sqlc.FetchRow(dbRow, len(sqlTableColNames)-1)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", name+"/FetchRow/"+k, err)
		} else if dataRow == nil {
			t.Fatalf("%s failed: nil", name+"/FetchRow/"+k)
		} else if len(dataRow) != len(sqlTableColNames)-1 {
			t.Fatalf("%s failed: expected %d fields but received %d", name+"/FetchRow/"+k, len(sqlTableColNames)-1, len(dataRow))
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
			sqlc, err = newSqlConnectMssql(info.driver, info.url, timezoneSql, 10000, nil)
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
			t.Fatalf("%s failed: error [%e]", name+"/"+k, err)
		} else if sqlc == nil {
			t.Fatalf("%s failed: nil", name+"/"+k)
		}
		err = sqlInitTable(sqlc, testSqlTableName, k)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", name+"/sqlInitTable/"+k, err)
		}
		sqlSelect := "SELECT * FROM %s WHERE userid < '%s'"
		i := rand.Intn(10)
		id := strconv.Itoa(i)
		dbRows, err := sqlc.GetDB().Query(fmt.Sprintf(sqlSelect, testSqlTableName, id))
		if err != nil {
			t.Fatalf("%s failed: error [%e]", name+"/Query/"+k, err)
		} else if dbRows == nil {
			t.Fatalf("%s failed: nil", name+"/Query/"+k)
		}
		dataRows, err := sqlc.FetchRows(dbRows)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", name+"/FetchRows/"+k, err)
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
			sqlc, err = newSqlConnectMssql(info.driver, info.url, timezoneSql, 10000, nil)
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
			t.Fatalf("%s failed: error [%e]", name+"/"+k, err)
		} else if sqlc == nil {
			t.Fatalf("%s failed: nil", name+"/"+k)
		}
		err = sqlInitTable(sqlc, testSqlTableName, k)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", name+"/sqlInitTable/"+k, err)
		}
		sqlSelect := "SELECT * FROM %s WHERE userid < '%s'"
		i := rand.Intn(10)
		id := strconv.Itoa(i)
		dbRows, err := sqlc.GetDB().Query(fmt.Sprintf(sqlSelect, testSqlTableName, id))
		if err != nil {
			t.Fatalf("%s failed: error [%e]", name+"/Query/"+k, err)
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
