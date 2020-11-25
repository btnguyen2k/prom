package prom

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// DbFlavor specifies the flavor or database server/vendor.
//
// Available: since v0.1.0
type DbFlavor int

// Predefined db flavors.
//
// Available: since v0.1.0
const (
	FlavorDefault DbFlavor = iota
	FlavorMySql
	FlavorPgSql
	FlavorMsSql
	FlavorOracle
	FlavorSqlite
)

// SqlPoolOptions configures database connection pooling options.
type SqlPoolOptions struct {
	// Maximum amount of time a connection may be reused, default is 1 hour,
	// see https://golang.org/pkg/database/sql/#DB.SetConnMaxLifetime
	ConnMaxLifetime time.Duration

	// Maximum number of idle connections in the pool, default is 1,
	// see https://golang.org/pkg/database/sql/#DB.SetMaxIdleConns
	MaxIdleConns int

	// Maximum number of open connections to the database, default is 2,
	// see https://golang.org/pkg/database/sql/#DB.SetMaxOpenConns
	MaxOpenConns int
}

var defaultSqlPoolOptions = &SqlPoolOptions{ConnMaxLifetime: 1 * time.Hour, MaxIdleConns: 1, MaxOpenConns: 2}

// SqlConnect holds a database/sql DB instance (https://golang.org/pkg/database/sql/#DB) that can be shared within the application.
type SqlConnect struct {
	driver, dsn string          // driver and data source name (DSN)
	poolOptions *SqlPoolOptions // connection pool options
	timeoutMs   int             // default timeout for db operations, in milliseconds
	flavor      DbFlavor        // database flavor
	db          *sql.DB         // database instance
	loc         *time.Location  // timezone location to parse date/time data, new since v0.1.2
}

// NewSqlConnect constructs a new SqlConnect instance.
//
// Parameters: see #NewSqlConnectWithFlavor.
func NewSqlConnect(driver, dsn string, defaultTimeoutMs int, poolOptions *SqlPoolOptions) (*SqlConnect, error) {
	return NewSqlConnectWithFlavor(driver, dsn, defaultTimeoutMs, poolOptions, FlavorDefault)
}

// NewSqlConnectWithFlavor constructs a new SqlConnect instance.
//
// Parameters:
// 	 - driver          : database driver name
// 	 - dsn             : data source name (sample format [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN])
// 	 - defaultTimeoutMs: default timeout for db operations, in milliseconds
// 	 - poolOptions     : connection pool options. If nil, default value is used
// 	 - flavor          : database flavor associated with the SqlConnect instance.
//
// Return: the SqlConnect instance and error (if any). Note:
//   - In case of connection error: this function returns the SqlConnect instance and the error.
//   - Other error: this function returns (nil, error)
//
// Available: since v0.1.0
func NewSqlConnectWithFlavor(driver, dsn string, defaultTimeoutMs int, poolOptions *SqlPoolOptions, flavor DbFlavor) (*SqlConnect, error) {
	if defaultTimeoutMs < 0 {
		defaultTimeoutMs = 0
	}
	if poolOptions == nil {
		poolOptions = defaultSqlPoolOptions
	}
	sc := &SqlConnect{
		driver:      driver,
		dsn:         dsn,
		poolOptions: poolOptions,
		timeoutMs:   defaultTimeoutMs,
		flavor:      flavor,
		loc:         time.UTC,
	}
	return sc, sc.Init()
}

// Init should be called to initialize the SqlConnect instance before use.
//
// Available since v0.2.8
func (sc *SqlConnect) Init() error {
	if sc.db != nil {
		return nil
	}
	db, err := sql.Open(sc.driver, sc.dsn)
	if err != nil {
		return err
	}
	if sc.poolOptions != nil {
		if sc.poolOptions.MaxOpenConns > 0 {
			db.SetMaxOpenConns(sc.poolOptions.MaxOpenConns)
		}
		if sc.poolOptions.MaxIdleConns > 0 {
			db.SetMaxIdleConns(sc.poolOptions.MaxIdleConns)
		}
		if sc.poolOptions.ConnMaxLifetime > 0 {
			db.SetConnMaxLifetime(sc.poolOptions.ConnMaxLifetime)
		}
	}
	sc.db = db
	return err
}

// GetDriver returns the database driver setting.
//
// Available: since v0.2.8
func (sc *SqlConnect) GetDriver() string {
	return sc.driver
}

// SetDriver sets the database driver setting.
// Note: the change does not take effect if called after Init has been called.
//
// Available: since v0.2.8
func (sc *SqlConnect) SetDriver(driver string) *SqlConnect {
	sc.driver = driver
	return sc
}

// GetDsn returns the database dsn setting.
//
// Available: since v0.2.8
func (sc *SqlConnect) GetDsn() string {
	return sc.dsn
}

// SetDsn sets the database dsn setting.
// Note: the change does not take effect if called after Init has been called.
//
// Available: since v0.2.8
func (sc *SqlConnect) SetDsn(dsn string) *SqlConnect {
	sc.dsn = dsn
	return sc
}

// GetTimeoutMs returns default timeout value (in milliseconds).
//
// Available since v0.2.8
func (sc *SqlConnect) GetTimeoutMs() int {
	return sc.timeoutMs
}

// SetTimeoutMs sets default timeout value (in milliseconds).
//
// Available since v0.2.8
func (sc *SqlConnect) SetTimeoutMs(timeoutMs int) *SqlConnect {
	sc.timeoutMs = timeoutMs
	return sc
}

// GetSqlPoolOptions returns the database connection pool configurations.
//
// Available: since v0.2.8
func (sc *SqlConnect) GetSqlPoolOptions() *SqlPoolOptions {
	return sc.poolOptions
}

// SetSqlPoolOptions sets the database connection pool configurations.
// Note: the change does not take effect if called after Init has been called.
//
// Available: since v0.2.8
func (sc *SqlConnect) SetSqlPoolOptions(poolOptions *SqlPoolOptions) *SqlConnect {
	sc.poolOptions = poolOptions
	return sc
}

// GetDbFlavor returns the current database flavor associated with this SqlConnect.
//
// Available: since v0.1.0
func (sc *SqlConnect) GetDbFlavor() DbFlavor {
	return sc.flavor
}

// SetDbFlavor associates a database flavor with this SqlConnect.
//
// Available: since v0.1.0
func (sc *SqlConnect) SetDbFlavor(flavor DbFlavor) *SqlConnect {
	sc.flavor = flavor
	return sc
}

// GetLocation returns the timezone location associated with this SqlConnect.
//
// Timezone/Location rules:
//   - If the database's underlying data type does not support timezone: read date/time is treated as "in-location".
//   - If the database's underlying data type support timezone: read date/time is converted to the target timezone/location.
//   - In any case, the returned date/time is attached with the specified timezone/location.
//
// Available: since v0.1.2
func (sc *SqlConnect) GetLocation() *time.Location {
	return sc.loc
}

// SetLocation associates a timezone location with this SqlConnect, used when parsing date/time data. Default value is time.UTC.
//
// Timezone/Location rules:
//   - If the database's underlying data type does not support timezone: read date/time is treated as "in-location".
//   - If the database's underlying data type support timezone: read date/time is converted to the target timezone/location.
//   - In any case, the returned date/time is attached with the specified timezone/location.
//
// Available: since v0.1.2
func (sc *SqlConnect) SetLocation(loc *time.Location) *SqlConnect {
	if loc == nil {
		sc.loc = time.UTC
	} else {
		sc.loc = loc
	}
	return sc
}

func (sc *SqlConnect) ensureLocation() *time.Location {
	if sc.loc == nil {
		sc.loc = time.UTC
	}
	return sc.loc
}

// NewContext creates a new context with specified timeout in milliseconds.
// If there is no specified timeout, or timeout value is less than or equal to 0, the default timeout is used.
//
// Available: since v0.2.0
// (since v0.2.8) this function return only context.Context. Use NewContextWithCancel if context.CancelFunc is needed.
func (sc *SqlConnect) NewContext(timeoutMs ...int) context.Context {
	ctx, _ := sc.NewContextWithCancel(timeoutMs...)
	return ctx
}

// NewContext creates a new context with specified timeout in milliseconds if the supplied ctx is nil. Otherwise,
// ctx is returned as-is.
//
// If there is no specified timeout, or timeout value is less than or equal to 0, the default timeout is used.
//
// Available: since v0.2.8
func (sc *SqlConnect) NewContextIfNil(ctx context.Context, timeoutMs ...int) context.Context {
	if ctx == nil {
		ctx = sc.NewContext(timeoutMs...)
	}
	return ctx
}

// NewContextWithCancel is similar to NewContext, but it returns a pair (context.Context, context.CancelFunc).
//
// Available: since v0.2.8
func (sc *SqlConnect) NewContextWithCancel(timeoutMs ...int) (context.Context, context.CancelFunc) {
	d := sc.timeoutMs
	if len(timeoutMs) > 0 && timeoutMs[0] > 0 {
		d = timeoutMs[0]
	}
	return context.WithTimeout(context.Background(), time.Duration(d)*time.Millisecond)
}

// GetDB returns the underlying 'sql.DB' instance.
func (sc *SqlConnect) GetDB() *sql.DB {
	return sc.db
}

// Close closes the underlying 'sql.DB' instance.
func (sc *SqlConnect) Close() error {
	return sc.db.Close()
}

// Ping verifies a connection to the database is still alive, establishing a connection if necessary.
func (sc *SqlConnect) Ping(ctx context.Context) error {
	return sc.db.PingContext(sc.NewContextIfNil(ctx))
}

// IsConnected returns true if the connection to the database is alive.
func (sc *SqlConnect) IsConnected() bool {
	return sc.Ping(nil) == nil
}

// Conn returns a single connection by either opening a new connection or returning an existing connection from the connection pool.
// Conn will block until either a connection is returned or ctx is canceled/timed-out.
//
// Every leased connection must be returned to the pool after use by calling sql.Conn.Close
func (sc *SqlConnect) Conn(ctx context.Context) (*sql.Conn, error) {
	return sc.db.Conn(sc.NewContextIfNil(ctx))
}

// FetchRow copies the columns from the matched row into a slice and return it.
//
// If more than one row matches the query, FetchRow uses only the first row and discards the rest.
// If no row matches the query, FetchRow returns (nil,nil).
func (sc *SqlConnect) FetchRow(row *sql.Row, numCols int) ([]interface{}, error) {
	if numCols <= 0 {
		return nil, errors.New("number of columns must be larger than 0")
	}
	vals := make([]interface{}, numCols)
	scanVals := make([]interface{}, numCols)
	for i := 0; i < numCols; i++ {
		scanVals[i] = &vals[i]
	}
	err := row.Scan(scanVals...)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return vals, err
}

var rawBytesType = reflect.TypeOf(sql.RawBytes{})
var bytesArrType = reflect.TypeOf([]byte{})
var uint8ArrType = reflect.TypeOf([]uint8{})
var timeType = reflect.TypeOf(time.Time{})
var sqlNulTime = reflect.TypeOf(sql.NullTime{})
var dbIntTypes = map[string]map[DbFlavor]bool{
	"TINYINT":   {FlavorDefault: true, FlavorMySql: true, FlavorMsSql: true},
	"SMALLINT":  {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true},
	"MEDIUMINT": {FlavorDefault: true, FlavorMySql: true},
	"INT":       {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true},
	"INTEGER":   {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true, FlavorSqlite: true},
	"BIGINT":    {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true},
	"INT2":      {FlavorDefault: true, FlavorPgSql: true},
	"INT4":      {FlavorDefault: true, FlavorPgSql: true},
	"INT8":      {FlavorDefault: true, FlavorPgSql: true},
}
var dbFloatTypes = map[string]map[DbFlavor]bool{
	"FLOAT":            {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true},
	"REAL":             {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true, FlavorSqlite: true},
	"NUMERIC":          {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true, FlavorSqlite: true},
	"DECIMAL":          {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true},
	"DOUBLE":           {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true},
	"DOUBLE PRECISION": {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true},
	"BINARY_FLOAT":     {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true},
	"BINARY_DOUBLE":    {FlavorDefault: true, FlavorOracle: true},
}
var dbStringTypes = map[string]map[DbFlavor]bool{
	"CHAR":              {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true},
	"VARCHAR":           {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true},
	"TEXT":              {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorSqlite: true},
	"CHARACTER":         {FlavorDefault: true, FlavorPgSql: true, FlavorOracle: true},
	"CHARACTER VARYING": {FlavorDefault: true, FlavorPgSql: true, FlavorOracle: true},
	"NCHAR":             {FlavorDefault: true, FlavorMsSql: true, FlavorOracle: true},
	"NVARCHAR":          {FlavorDefault: true, FlavorMsSql: true},
	"NTEXT":             {FlavorDefault: true, FlavorMsSql: true},
	"VARCHAR2":          {FlavorDefault: true, FlavorOracle: true},
	"NVARCHAR2":         {FlavorDefault: true, FlavorOracle: true},
	"CLOB":              {FlavorDefault: true, FlavorOracle: true},
	"NCLOB":             {FlavorDefault: true, FlavorOracle: true},
	"LONG":              {FlavorDefault: true, FlavorOracle: true},
	"BPCHAR":            {FlavorDefault: true, FlavorPgSql: true},
}
var dbDateTimeTypes = map[string]map[DbFlavor]bool{
	"1266":                           {FlavorDefault: true, FlavorPgSql: true},
	"TIME":                           {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorSqlite: true},
	"DATE":                           {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true},
	"YEAR":                           {FlavorDefault: true, FlavorMySql: true},
	"DATETIME":                       {FlavorDefault: true, FlavorMySql: true, FlavorMsSql: true},
	"DATETIME2":                      {FlavorDefault: true, FlavorMsSql: true},
	"DATETIMEOFFSET":                 {FlavorDefault: true, FlavorMsSql: true},
	"SMALLDATETIME":                  {FlavorDefault: true, FlavorMsSql: true},
	"TIMESTAMP":                      {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorOracle: true},
	"TIMESTAMP WITH TIME ZONE":       {FlavorDefault: true, FlavorOracle: true},
	"TIMESTAMP WITH LOCAL TIME ZONE": {FlavorDefault: true, FlavorOracle: true},
}

func (sc *SqlConnect) isIntType(col *sql.ColumnType) bool {
	name := strings.ToUpper(col.DatabaseTypeName())
	m, ok := dbIntTypes[name]
	if !ok {
		return false
	}
	_, ok = m[sc.flavor]
	return ok
}

func (sc *SqlConnect) isFloatType(col *sql.ColumnType) bool {
	name := strings.ToUpper(col.DatabaseTypeName())
	m, ok := dbFloatTypes[name]
	if !ok {
		return false
	}
	_, ok = m[sc.flavor]
	return ok
}

func (sc *SqlConnect) isStringType(col *sql.ColumnType) bool {
	name := strings.ToUpper(col.DatabaseTypeName())
	m, ok := dbStringTypes[name]
	if !ok {
		return false
	}
	_, ok = m[sc.flavor]
	return ok
}

func (sc *SqlConnect) isDateTimeType(col *sql.ColumnType) bool {
	name := strings.ToUpper(col.DatabaseTypeName())
	m, ok := dbDateTimeTypes[name]
	if !ok {
		return false
	}
	_, ok = m[sc.flavor]
	return ok
}

func isRawBytesType(v interface{}) bool {
	t := reflect.TypeOf(v)
	return t == rawBytesType || t == bytesArrType || t == uint8ArrType
}

const (
	dtlayout     = "2006-01-02 15:04:05"
	dtlayoutTz   = "2006-01-02 15:04:05-07"
	dtlayoutTzz  = "2006-01-02 15:04:05-07:00"
	dtlayoutNano = "2006-01-02 15:04:05.999999999"
)

func (sc *SqlConnect) _scanMysqlDateTimeFromRawBytes(result map[string]interface{}, v *sql.ColumnType, val interface{}) error {
	// MySQL's date/time types do not support timezone, treat the value as "in-location"
	loc := sc.ensureLocation()
	var err error
	switch strings.ToUpper(v.DatabaseTypeName()) {
	case "TIME":
		result[v.Name()], err = time.ParseInLocation(dtlayout, fmt.Sprintf("2006-01-02 %s", val), loc)
	case "DATE":
		result[v.Name()], err = time.ParseInLocation("2006-01-02", string(val.([]byte)), loc)
	case "DATETIME":
		result[v.Name()], err = time.ParseInLocation(dtlayout, string(val.([]byte)), loc)
	case "TIMESTAMP":
		result[v.Name()], err = time.ParseInLocation(dtlayout, string(val.([]byte)), loc)
	default:
		err = errors.New("unknown date/time column type " + v.DatabaseTypeName())
	}
	return err
}

func (sc *SqlConnect) _scanPgsqlDateTimeFromString(result map[string]interface{}, v *sql.ColumnType, val interface{}) error {
	loc := sc.ensureLocation()
	var err error
	switch strings.ToUpper(v.DatabaseTypeName()) {
	case "TIME":
		// TIME does not support timezone, treated the value as "in-location"
		result[v.Name()], err = time.ParseInLocation(dtlayout, fmt.Sprintf("2006-01-02 %s", val), loc)
	case "1266":
		// 1266 is TIME WITH TIMEZONE, convert to the target timezone/location
		result[v.Name()], err = time.Parse(dtlayoutTz, fmt.Sprintf("2006-01-02 %s", val))
		result[v.Name()] = result[v.Name()].(time.Time).In(loc)
	default:
		err = errors.New("unknown date/time column type " + v.DatabaseTypeName())
	}
	return err
}

func (sc *SqlConnect) _transformPgsqlDateTime(result map[string]interface{}, v *sql.ColumnType, val interface{}) error {
	loc := sc.ensureLocation()
	result[v.Name()] = val.(time.Time)
	var err error
	switch strings.ToUpper(v.DatabaseTypeName()) {
	case "DATE", "TIMESTAMP":
		// DATE/TIMESTAMP does not support timezone, treated the value as "in-location"
		result[v.Name()], err = time.ParseInLocation(dtlayoutNano, val.(time.Time).Format(dtlayoutNano), loc)
	default:
		// assume other types support timezone,convert to the target timezone/location
		result[v.Name()] = val.(time.Time).In(loc)
	}
	return err
}

func (sc *SqlConnect) _transformMssqlDateTime(result map[string]interface{}, v *sql.ColumnType, val interface{}) error {
	loc := sc.ensureLocation()
	result[v.Name()] = val.(time.Time)
	var err error
	switch strings.ToUpper(v.DatabaseTypeName()) {
	case "TIME":
		// TIME does not support timezone, treated the value as "in-location"
		temp := val.(time.Time).Format("15:04:05")
		result[v.Name()], err = time.ParseInLocation(dtlayout, fmt.Sprintf("2006-01-02 %s", temp), loc)
	case "DATE", "DATETIME", "DATETIME2":
		// DATE/DATETIME/DATETIME2 does not support timezone, treated the value as "in-location"
		result[v.Name()], err = time.ParseInLocation(dtlayoutNano, val.(time.Time).Format(dtlayoutNano), loc)
	default:
		// assume other types support timezone,convert to the target timezone/location
		result[v.Name()] = val.(time.Time).In(loc)
	}
	return err
}

func (sc *SqlConnect) _scanSqliteDateTimeFromString(result map[string]interface{}, v *sql.ColumnType, val interface{}) error {
	// SQLite store date/time as string, with timezone info
	loc := sc.ensureLocation()
	var err error
	result[v.Name()], err = time.Parse(dtlayoutTzz, val.(string))
	result[v.Name()] = result[v.Name()].(time.Time).In(loc)
	return err
}

func (sc *SqlConnect) _transformOracleDateTime(result map[string]interface{}, v *sql.ColumnType, val interface{}) error {
	loc := sc.ensureLocation()
	result[v.Name()] = val.(time.Time)
	var err error
	switch strings.ToUpper(v.DatabaseTypeName()) {
	case "DATE":
		// FIXME: not sure if it's behavior of Oracle or godror but this seems wrong!
		// DATE does not support timezone, but Oracle converts DATE to UTC before storing.
		// Hence, we just need to convert it back to correct timezone/location
		result[v.Name()] = val.(time.Time).In(loc)
	default:
		// FIXME: not sure if it's behavior of Oracle or godror but this seems wrong!
		// first "parse in UTC" and then convert to the target timezone/location
		// assume other types support timezone,convert to the target timezone/location
		result[v.Name()], err = time.Parse(dtlayoutNano, val.(time.Time).Format(dtlayoutNano))
		result[v.Name()] = result[v.Name()].(time.Time).In(loc)
	}
	return err
}

func (sc *SqlConnect) fetchOneRow(rows *sql.Rows, colsAndTypes []*sql.ColumnType) (map[string]interface{}, error) {
	numCols := len(colsAndTypes)
	vals := make([]interface{}, numCols)
	scanVals := make([]interface{}, numCols)
	for i := 0; i < numCols; i++ {
		scanVals[i] = &vals[i]
	}
	if err := rows.Scan(scanVals...); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	result := map[string]interface{}{}
	for i, v := range colsAndTypes {
		// if strings.ToLower(v.Name()) == "data_time" {
		// 	fmt.Printf("%s/%s/%s/%v - %s\n", v.Name(), v.DatabaseTypeName(), v.ScanType(), vals[i], vals[i])
		// 	// t, e := time.ParseInLocation("15:04:05", fmt.Sprintf("%s", vals[i]), loc)
		// 	// fmt.Println(t, e)
		// }
		if sc.isIntType(v) && isRawBytesType(vals[i]) {
			// when number is loaded as []byte
			result[v.Name()], _ = strconv.ParseInt(string(vals[i].([]byte)), 10, 64)
		} else if sc.isFloatType(v) && isRawBytesType(vals[i]) {
			// when number is loaded as []byte
			result[v.Name()], _ = strconv.ParseFloat(string(vals[i].([]byte)), 64)
		} else if sc.isStringType(v) && isRawBytesType(vals[i]) {
			// when string is loaded as []byte
			result[v.Name()] = string(vals[i].([]byte))
		} else if sc.flavor == FlavorSqlite && sc.isDateTimeType(v) {
			// special care for SQLite's date/time types
			if err := sc._scanSqliteDateTimeFromString(result, v, vals[i]); err != nil {
				return nil, err
			}
		} else if sc.flavor == FlavorMySql && sc.isDateTimeType(v) && isRawBytesType(vals[i]) {
			// MySQL's TIME/DATE/DATETIME/TIMESTAMP is loaded as []byte
			if err := sc._scanMysqlDateTimeFromRawBytes(result, v, vals[i]); err != nil {
				return nil, err
			}
		} else if sc.flavor == FlavorPgSql && sc.isDateTimeType(v) && v.ScanType().Kind() == reflect.String {
			// PostgreSQL's TIME is loaded as string
			if err := sc._scanPgsqlDateTimeFromString(result, v, vals[i]); err != nil {
				return nil, err
			}
		} else if sc.flavor == FlavorPgSql && v.ScanType().Kind() == timeType.Kind() {
			// special care for PostgreSQL's date/time types
			if err := sc._transformPgsqlDateTime(result, v, vals[i]); err != nil {
				return nil, err
			}
		} else if sc.flavor == FlavorMsSql && v.ScanType().Kind() == timeType.Kind() {
			// special care for MSSQL's date/time types
			if err := sc._transformMssqlDateTime(result, v, vals[i]); err != nil {
				return nil, err
			}
		} else if sc.flavor == FlavorOracle && v.ScanType().Kind() == sqlNulTime.Kind() {
			// special care for Oracle's date/time types
			if err := sc._transformOracleDateTime(result, v, vals[i]); err != nil {
				return nil, err
			}
		} else if sc.flavor == FlavorPgSql && v.ScanType().Kind() == reflect.Interface && sc.isStringType(v) {
			// PostgreSQL's CHAR(1) is loaded as []byte old driver version
			_v, ok := vals[i].([]byte)
			if ok {
				result[v.Name()] = string(_v)
			} else {
				result[v.Name()] = vals[i].(string)
			}
		} else {
			result[v.Name()] = vals[i]
		}
	}
	return result, nil
}

// FetchRows loads rows from database and transform to a slice of 'map[string]interface{}' where each column's name & value is a map entry.
// If no row matches the query, FetchRow returns (<empty slice>, nil).
//
// Note: FetchRows does NOT call 'rows.close()' when done!
func (sc *SqlConnect) FetchRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	result := make([]map[string]interface{}, 0)
	for rows.Next() {
		rowData, err := sc.fetchOneRow(rows, colTypes)
		if err != nil {
			return nil, err
		}
		result = append(result, rowData)
	}
	return result, rows.Err()
}

// FetchRowsCallback loads rows from database. For each row, FetchRowsCallback transforms it to 'map[string]interface{}', where each column's name & value is a map entry, and passes the map to the callback function.
// FetchRowsCallback stops the loop when there is no more row to load or 'callback' function returns 'false'.
//
// Note: FetchRowsCallback does NOT call 'rows.close()' when done!
func (sc *SqlConnect) FetchRowsCallback(rows *sql.Rows, callback func(row map[string]interface{}, err error) bool) error {
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	var next = true
	for next && rows.Next() {
		if rowData, err := sc.fetchOneRow(rows, colTypes); err != nil {
			next = callback(nil, err)
			// if !next {
			// 	return err
			// }
		} else {
			next = callback(rowData, nil)
		}
	}
	return rows.Err()
}
