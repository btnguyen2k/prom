// Package sql provides database/sql specific implementation of btnguyen2k/prom and other utilities.
package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/btnguyen2k/prom"
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
	FlavorCosmosDb
)

// DurationToOracleYearToMonth converts a time.Duration value to Oracle's INTERVAL YEAR TO MONTH literals (e.g. "YY-MM").
//
// Note: a month is assumed to have 30 days; and a year is 12 months. Hence, the conversion is not accurate as a year is only 360 days.
//
// @Available since <<VERSION>>
func DurationToOracleYearToMonth(v time.Duration) string {
	months := int(v.Truncate(24*time.Hour).Hours()) / 24 / 30
	years := months / 12
	months -= years * 12
	return fmt.Sprintf("%d-%d", years, months)
}

// ParseOracleIntervalYearToMonth parses an Oracle's INTERVAL YEAR TO MONTH literal (e.g. "+YY-MM") to time.Duration.
//
// Note: a month is assumed to have 30 days; and a year is 12 months. Hence, the conversion is not accurate as a year is only 360 days.
//
// @Available since <<VERSION>>
func ParseOracleIntervalYearToMonth(v string) (time.Duration, error) {
	re := regexp.MustCompile(`^\+?(\d+)-(\d+)$`)
	matches := re.FindStringSubmatch(v)
	if matches == nil {
		return 0, fmt.Errorf("cannot parse [%s] as Oracle's INTERVAL YEAR TO MONTH", v)
	}
	years, _ := strconv.Atoi(matches[1])
	months, _ := strconv.Atoi(matches[2])
	return time.Duration(years*12+months) * 30 * 24 * time.Hour, nil
}

// DurationToOracleDayToSecond converts a time.Duration value to Oracle's INTERVAL DAY TO SECOND literals (e.g. "d HH:mm:ss.SSSSSSSSS").
//
// @Available since <<VERSION>>
func DurationToOracleDayToSecond(v time.Duration, precision int) string {
	z := time.Unix(0, 0).UTC()
	hms := z.Add(v).Format("15:04:05")
	days := int(v.Truncate(24*time.Hour).Hours() / 24)
	result := fmt.Sprintf("%d %s", days, hms)
	if precision > 0 {
		if precision > 9 {
			precision = 9
		}
		v = v.Round(time.Duration(math.Pow10(9-precision)) * time.Nanosecond)
		layout := strings.Repeat("9", precision)
		trailing := z.Add(v).Format("." + layout)
		for len(trailing) < precision+1 {
			trailing += "0"
		}
		result += trailing
	}
	return result
}

// ParseOracleIntervalDayToSecond parses an Oracle's INTERVAL DAY TO SECOND literal (e.g. "+6 13:44:50.123457") to time.Duration .
//
// @Available since <<VERSION>>
func ParseOracleIntervalDayToSecond(v string) (time.Duration, error) {
	re := regexp.MustCompile(`^\+?(\d+)\s(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?$`)
	matches := re.FindStringSubmatch(v)
	if matches == nil {
		return 0, fmt.Errorf("cannot parse [%s] as Oracle's INTERVAL DAY TO SECOND", v)
	}
	days, _ := strconv.Atoi(matches[1])
	hours, _ := strconv.Atoi(matches[2])
	minutes, _ := strconv.Atoi(matches[3])
	seconds, _ := strconv.Atoi(matches[4])
	nanos := 0
	if len(matches) > 5 {
		for len(matches[5]) < 9 {
			matches[5] += "0"
		}
		nanos, _ = strconv.Atoi(matches[5])
	}
	return time.Duration(days)*24*time.Hour + time.Duration(hours)*time.Hour + time.Duration(minutes)*time.Minute +
		time.Duration(seconds)*time.Second + time.Duration(nanos)*time.Nanosecond, nil
}

// PoolOpts configures database connection pooling options.
//
// See:
//   - Connection's max lifetime: https://golang.org/pkg/database/sql/#DB.SetConnMaxLifetime
//   - Max idle connections: https://golang.org/pkg/database/sql/#DB.SetMaxIdleConns
//   - Max open connections: https://golang.org/pkg/database/sql/#DB.SetMaxOpenConns
//
// @Available since <<VERSION>>
type PoolOpts struct {
	prom.BasePoolOpts
}

var defaultPoolOpts = &PoolOpts{prom.BasePoolOpts{ConnLifetime: 1 * time.Hour, MinPoolSize: 1, MaxPoolSize: 2}}

// SqlConnect holds a database/sql DB instance (https://golang.org/pkg/database/sql/#DB) that can be shared within the application.
type SqlConnect struct {
	*prom.BaseConnection
	driver, dsn    string         // driver and data source name (DSN)
	timeoutMs      int            // default timeout for db operations, in milliseconds
	flavor         DbFlavor       // database flavor
	db             *sql.DB        // database instance
	dbProxy        *DBProxy       // (since v0.3.0) wrapper around the real sql.DB instance
	loc            *time.Location // timezone location to parse date/time data, new since v0.1.2
	mysqlParseTime bool           // set to 'true' if specifying parseTime=true in MySQL connection string, new since v0.2.12
}

// NewSqlConnectWithFlavor constructs a new SqlConnect instance.
//
// Parameters:
//   - driver          : database driver name
//   - dsn             : data source name (sample format [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN])
//   - defaultTimeoutMs: default timeout for db operations, in milliseconds
//   - poolOpts        : connection pool options. If nil, default value is used
//   - flavor          : database flavor associated with the SqlConnect instance. See DbFlavor for details.
//
// Return: the SqlConnect instance and error (if any). Note:
//   - In case of connection error: this function returns the SqlConnect instance and the error.
//   - Other error: this function returns (nil, error)
//
// Available: since v0.1.0
func NewSqlConnectWithFlavor(driver, dsn string, defaultTimeoutMs int, poolOpts *PoolOpts, flavor DbFlavor) (*SqlConnect, error) {
	if defaultTimeoutMs < 0 {
		defaultTimeoutMs = 0
	}
	if poolOpts == nil {
		poolOpts = defaultPoolOpts
	}
	baseConn := &prom.BaseConnection{}
	baseConn.SetPoolOpts(poolOpts).RegisterMetricsLogger(prom.NewMemoryStoreMetricsLogger(1028))
	sc := &SqlConnect{
		BaseConnection: baseConn,
		driver:         driver,
		dsn:            dsn,
		timeoutMs:      defaultTimeoutMs,
		flavor:         flavor,
		loc:            time.UTC,
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
	if poolOpts := sc.PoolOpts(); poolOpts != nil {
		if poolOpts.MaxPoolSize > 0 {
			db.SetMaxOpenConns(poolOpts.MaxPoolSize)
		}
		if poolOpts.MinPoolSize > 0 {
			db.SetMaxIdleConns(poolOpts.MinPoolSize)
		}
		if poolOpts.ConnLifetime > 0 {
			db.SetConnMaxLifetime(poolOpts.ConnLifetime)
		}
	}
	if sc.MetricsLogger() == nil {
		sc.RegisterMetricsLogger(prom.NewMemoryStoreMetricsLogger(1028))
	}
	sc.db = db
	sc.dbProxy = &DBProxy{DB: db, sqlc: sc}
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

// PoolOpts overrides prom.BaseConnection/PoolOpts.
func (sc *SqlConnect) PoolOpts() *PoolOpts {
	poolOpts := sc.BaseConnection.PoolOpts()
	if sqlPoolOpts, ok := poolOpts.(*PoolOpts); ok {
		return sqlPoolOpts
	}
	return nil
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
func (sc *SqlConnect) SetLocation(inLocation *time.Location) *SqlConnect {
	if inLocation == nil {
		sc.loc = time.UTC
	} else {
		sc.loc = inLocation
	}
	return sc
}

func (sc *SqlConnect) ensureLocation() *time.Location {
	if sc.loc == nil {
		sc.loc = time.UTC
	}
	return sc.loc
}

// GetMysqlParseTime returns the flag mysqlParseTime's value.
//
// Flag 'mysqlParseTime' is 'true' if MySQL connection string contains parseTime=true.
//
// Available: since v0.2.12
func (sc *SqlConnect) GetMysqlParseTime() bool {
	return sc.mysqlParseTime
}

// SetMysqlParseTime sets the flag mysqlParseTime's value.
//
// Flag 'mysqlParseTime' is 'true' if MySQL connection string contains parseTime=true.
//
// Available: since v0.2.12
func (sc *SqlConnect) SetMysqlParseTime(value bool) *SqlConnect {
	sc.mysqlParseTime = value
	return sc
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

// NewContextIfNil creates a new context with specified timeout in milliseconds if the supplied ctx is nil. Otherwise,
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

// GetDBProxy is similar to GetDB, but returns a proxy that can be used as a replacement.
//
// Available since v0.3.0
func (sc *SqlConnect) GetDBProxy() *DBProxy {
	if sc.dbProxy == nil {
		sc.dbProxy = &DBProxy{DB: sc.db, sqlc: sc}
	}
	return sc.dbProxy
}

// Close closes the underlying 'sql.DB' instance.
func (sc *SqlConnect) Close() error {
	return sc.db.Close()
}

// Ping verifies a connection to the database is still alive, establishing a connection if necessary.
func (sc *SqlConnect) Ping(ctx context.Context) error {
	return sc.GetDBProxy().PingContext(sc.NewContextIfNil(ctx))
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
	return sc.GetDB().Conn(sc.NewContextIfNil(ctx))
}

// ConnProxy is similar to Conn, but returns a proxy that can be used as a replacement.
//
// Available since v0.3.0
func (sc *SqlConnect) ConnProxy(ctx context.Context) (*ConnProxy, error) {
	conn, err := sc.Conn(ctx)
	return &ConnProxy{Conn: conn, sqlc: sc}, err
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
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return vals, err
}

var rawBytesType = reflect.TypeOf(sql.RawBytes{})
var bytesArrType = reflect.TypeOf([]byte{})
var uint8ArrType = reflect.TypeOf([]uint8{})
var timeType = reflect.TypeOf(time.Time{})
var sqlNullTime = reflect.TypeOf(sql.NullTime{})

// var sqlNullInt32 = reflect.TypeOf(sql.NullInt32{})
// var sqlNullInt64 = reflect.TypeOf(sql.NullInt64{})
// var sqlNullFloat64 = reflect.TypeOf(sql.NullFloat64{})

var dbIntTypes = map[string]map[DbFlavor]bool{
	"TINYINT":   {FlavorDefault: true, FlavorMySql: true, FlavorMsSql: true, FlavorSqlite: true},
	"SMALLINT":  {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorSqlite: true},
	"MEDIUMINT": {FlavorDefault: true, FlavorMySql: true, FlavorSqlite: true},
	"INT":       {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true, FlavorSqlite: true},
	"INTEGER":   {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true, FlavorSqlite: true},
	"BIGINT":    {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true, FlavorSqlite: true},
	"INT2":      {FlavorDefault: true, FlavorPgSql: true, FlavorSqlite: true},
	"INT4":      {FlavorDefault: true, FlavorPgSql: true},
	"INT8":      {FlavorDefault: true, FlavorPgSql: true, FlavorSqlite: true},
}
var dbFloatTypes = map[string]map[DbFlavor]bool{
	"FLOAT":            {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true, FlavorSqlite: true},
	"FLOAT4":           {FlavorDefault: true, FlavorPgSql: true},
	"FLOAT8":           {FlavorDefault: true, FlavorPgSql: true},
	"REAL":             {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true, FlavorSqlite: true},
	"NUMBER":           {FlavorDefault: true, FlavorSqlite: true},
	"NUMERIC":          {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true, FlavorSqlite: true},
	"DECIMAL":          {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true, FlavorSqlite: true},
	"DOUBLE":           {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true, FlavorSqlite: true},
	"DOUBLE PRECISION": {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true},
	"BINARY_FLOAT":     {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true},
	"BINARY_DOUBLE":    {FlavorDefault: true, FlavorOracle: true},
	"MONEY":            {FlavorDefault: true, FlavorMsSql: true, FlavorPgSql: true},
	"SMALLMONEY":       {FlavorDefault: true, FlavorMsSql: true},
	"790":              {FlavorPgSql: true},
}
var dbStringTypes = map[string]map[DbFlavor]bool{
	"CHAR":              {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true},
	"VARCHAR":           {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true, FlavorSqlite: true},
	"TEXT":              {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorSqlite: true},
	"CHARACTER":         {FlavorDefault: true, FlavorPgSql: true, FlavorOracle: true, FlavorSqlite: true},
	"CHARACTER VARYING": {FlavorDefault: true, FlavorPgSql: true, FlavorOracle: true},
	"NCHAR":             {FlavorDefault: true, FlavorMsSql: true, FlavorOracle: true, FlavorSqlite: true},
	"NVARCHAR":          {FlavorDefault: true, FlavorMsSql: true, FlavorSqlite: true},
	"NTEXT":             {FlavorDefault: true, FlavorMsSql: true},
	"VARCHAR2":          {FlavorDefault: true, FlavorOracle: true},
	"NVARCHAR2":         {FlavorDefault: true, FlavorOracle: true},
	"CLOB":              {FlavorDefault: true, FlavorOracle: true, FlavorSqlite: true},
	"NCLOB":             {FlavorDefault: true, FlavorOracle: true},
	"LONG":              {FlavorDefault: true, FlavorOracle: true},
	"BPCHAR":            {FlavorDefault: true, FlavorPgSql: true},
}
var dbDateTimeTypes = map[string]map[DbFlavor]bool{
	"1266":                           {FlavorDefault: true, FlavorPgSql: true},
	"TIME":                           {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorSqlite: true},
	"DATE":                           {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true, FlavorSqlite: true},
	"YEAR":                           {FlavorDefault: true, FlavorMySql: true},
	"DATETIME":                       {FlavorDefault: true, FlavorMySql: true, FlavorMsSql: true, FlavorSqlite: true},
	"DATETIME2":                      {FlavorDefault: true, FlavorMsSql: true},
	"DATETIMEOFFSET":                 {FlavorDefault: true, FlavorMsSql: true},
	"SMALLDATETIME":                  {FlavorDefault: true, FlavorMsSql: true},
	"TIMESTAMP":                      {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorOracle: true, FlavorSqlite: true},
	"TIMESTAMP WITH TIME ZONE":       {FlavorDefault: true, FlavorOracle: true},
	"TIMESTAMP WITH LOCAL TIME ZONE": {FlavorDefault: true, FlavorOracle: true},
}

var dbDurationTypes = map[string]map[DbFlavor]bool{
	"INTERVALDS_DTY": {FlavorDefault: true, FlavorOracle: true},
	"INTERVALYM_DTY": {FlavorDefault: true, FlavorOracle: true},
}

var reDbTypeName = regexp.MustCompile(`^(?i)(.*?)\(.*$`)

func _normalizeDbTypeName(ct *sql.ColumnType) string {
	rawDbTypeName := strings.ToUpper(ct.DatabaseTypeName())
	if matches := reDbTypeName.FindStringSubmatch(rawDbTypeName); len(matches) > 1 {
		return strings.TrimSpace(matches[1])
	}
	return rawDbTypeName
}

func (sc *SqlConnect) isNumberType(col *sql.ColumnType) bool {
	dbTypeName := _normalizeDbTypeName(col)
	if sc.flavor == FlavorOracle && dbTypeName == "NUMBER" {
		// special case for Oracle
		return true
	}
	m, ok := dbIntTypes[dbTypeName]
	if !ok {
		m, ok = dbFloatTypes[dbTypeName]
	}
	if !ok {
		return false
	}
	_, ok = m[sc.flavor]
	return ok
}

func (sc *SqlConnect) isIntType(col *sql.ColumnType) bool {
	dbTypeName := _normalizeDbTypeName(col)
	if sc.flavor == FlavorOracle && dbTypeName == "NUMBER" {
		// special case for Oracle
		_, scale, _ := col.DecimalSize()
		return scale == 0
	}
	m, ok := dbIntTypes[dbTypeName]
	if !ok {
		return false
	}
	_, ok = m[sc.flavor]
	return ok
}

func (sc *SqlConnect) isFloatType(col *sql.ColumnType) bool {
	dbTypeName := _normalizeDbTypeName(col)
	if sc.flavor == FlavorOracle && dbTypeName == "NUMBER" {
		// special case for Oracle
		_, scale, _ := col.DecimalSize()
		return scale != 0
	}
	m, ok := dbFloatTypes[dbTypeName]
	if !ok {
		return false
	}
	_, ok = m[sc.flavor]
	return ok
}

func (sc *SqlConnect) isStringType(col *sql.ColumnType) bool {
	dbTypeName := _normalizeDbTypeName(col)
	m, ok := dbStringTypes[dbTypeName]
	if !ok {
		return false
	}
	_, ok = m[sc.flavor]
	return ok
}

func (sc *SqlConnect) isDateTimeType(col *sql.ColumnType) bool {
	dbTypeName := _normalizeDbTypeName(col)
	m, ok := dbDateTimeTypes[dbTypeName]
	if !ok {
		return false
	}
	_, ok = m[sc.flavor]
	return ok
}

func (sc *SqlConnect) isDurationType(col *sql.ColumnType) bool {
	dbTypeName := _normalizeDbTypeName(col)
	m, ok := dbDurationTypes[dbTypeName]
	if !ok {
		return false
	}
	_, ok = m[sc.flavor]
	return ok
}

func isValueTypeRawBytes(v interface{}) bool {
	if v == nil {
		return false
	}
	t := reflect.TypeOf(v)
	return t == rawBytesType || t == bytesArrType || t == uint8ArrType
}

// toIntIfValidInteger converts the input to int64 if:
//   - the input is an integer/unsigned integer
//   - a string or []byte representing an integer/unsigned integer
func toIntIfValidInteger(v interface{}) (int64, error) {
	if v == nil {
		return 0, errors.New("input is nil")
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(rv.Uint()), nil
	case reflect.String:
		return strconv.ParseInt(rv.String(), 10, 64)
	case rawBytesType.Kind(), bytesArrType.Kind(), uint8ArrType.Kind():
		return strconv.ParseInt(string(rv.Bytes()), 10, 64)
	default:
		return 0, errors.New("input is not a valid integer")
	}
}

// toFloatIfValidReal converts the input to float64 if:
//   - the input is a floating point/real number or integer
//   - a string or []byte representing a floating point/real number
func toFloatIfValidReal(v interface{}) (float64, error) {
	if v == nil {
		return 0.0, errors.New("input is nil")
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return rv.Float(), nil
	case reflect.String:
		return strconv.ParseFloat(rv.String(), 64)
	case rawBytesType.Kind(), bytesArrType.Kind(), uint8ArrType.Kind():
		return strconv.ParseFloat(string(rv.Bytes()), 64)
	default:
		return 0, errors.New("input is not a valid floating point number")
	}
}

const (
	dtlayout           = "2006-01-02 15:04:05"
	dtlayoutTz         = "2006-01-02 15:04:05-07"
	dtlayoutTzz        = "2006-01-02 15:04:05-07:00"
	dtlayoutNanoTzzTzz = "2006-01-02 15:04:05.999999999 -0700 -0700"
	dtlayoutNano       = "2006-01-02 15:04:05.999999999"
)

func (sc *SqlConnect) _scanMysqlDateTimeFromRawBytes(result map[string]interface{}, v *sql.ColumnType, val interface{}) error {
	loc := sc.ensureLocation()
	var err error
	dbTypeName := _normalizeDbTypeName(v)
	switch dbTypeName {
	case "TIME":
		// MySQL's TIME type do not support timezone, treat the value as "in-location"
		result[v.Name()], err = time.ParseInLocation(dtlayout, fmt.Sprintf("2006-01-02 %s", val), loc)
	case "DATE":
		// MySQL's DATE type do not support timezone, treat the value as "in-location"
		result[v.Name()], err = time.ParseInLocation("2006-01-02", string(val.([]byte)), loc)
	case "DATETIME":
		// MySQL's DATETIME type do not support timezone, treat the value as "in-location"
		result[v.Name()], err = time.ParseInLocation(dtlayout, string(val.([]byte)), loc)
	case "TIMESTAMP":
		// MySQL's TIMESTAMP is converted and stored as UTC
		// since this code is reached only when parseTime=false, time timestamp value is not
		// automatically converted to connection's timezone/location.
		result[v.Name()], err = time.ParseInLocation(dtlayout, string(val.([]byte)), loc)
	default:
		err = errors.New("unknown date/time column type " + v.DatabaseTypeName())
	}
	return err
}

func (sc *SqlConnect) _scanPgsqlDateTimeFromString(result map[string]interface{}, v *sql.ColumnType, val interface{}) error {
	loc := sc.ensureLocation()
	var err error
	dbTypeName := _normalizeDbTypeName(v)
	switch dbTypeName {
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
	dbTypeName := _normalizeDbTypeName(v)
	switch dbTypeName {
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
	var err error
	dbTypeName := _normalizeDbTypeName(v)
	switch dbTypeName {
	case "TIME":
		// TIME does not support timezone, treated the value as "in-location"
		temp := val.(time.Time).Format("15:04:05")
		result[v.Name()], err = time.ParseInLocation(dtlayout, fmt.Sprintf("2006-01-02 %s", temp), loc)
	case "DATE", "DATETIME", "DATETIME2":
		// DATE/DATETIME/DATETIME2 does not support timezone, treated the value as "in-location"
		result[v.Name()], err = time.ParseInLocation(dtlayoutNano, val.(time.Time).Format(dtlayoutNano), loc)
	default:
		// assume other types support timezone, convert to the target timezone/location
		result[v.Name()] = val.(time.Time).In(loc)
	}
	return err
}

// handle date/time types with drivers github.com/mattn/go-sqlite3 and modernc.org/sqlite
func (sc *SqlConnect) _scanSqliteDateTime(result map[string]interface{}, v *sql.ColumnType, val interface{}) error {
	loc := sc.ensureLocation()
	str, ok := val.(string)
	if !ok {
		var bytes []byte
		bytes, ok = val.([]byte)
		if ok {
			str = string(bytes)
		}
	}
	if ok {
		// date/time is fetched as string/[]byte, with timezone info
		var err error
		for _, dtLayout := range []string{dtlayoutTzz, dtlayoutNanoTzzTzz} {
			// datetime layouts used by github.com/mattn/go-sqlite3 and modernc.org/sqlite
			result[v.Name()], err = time.Parse(dtLayout, str)
			if err == nil {
				result[v.Name()] = result[v.Name()].(time.Time).In(loc)
				break
			}
		}
		return err
	}

	vTime, ok := val.(time.Time)
	if ok {
		// date/time is fetched as time.Time, with timezone info
		result[v.Name()] = vTime.In(loc)
		return nil
	}

	return fmt.Errorf("sqlite: cannot convert value %#v to time.Time", val)
}

func (sc *SqlConnect) _transformOracleDateTime(result map[string]interface{}, v *sql.ColumnType, val interface{}) error {
	loc := sc.ensureLocation()
	var err error
	dbTypeName := _normalizeDbTypeName(v)

	//fmt.Printf("[DEBUG] %s/%s - %s - %s\n", v.Name(), v.DatabaseTypeName()+":"+dbTypeName, val.(time.Time).Location(), val.(time.Time).Format(time.RFC3339))

	switch dbTypeName {
	case "DATE", "TIMESTAMP", "TIMESTAMPDTY":
		// Oracle's DATE/TIMESTAMP/TIMESTAMPDTY types are non-timezone, Oracle returns value as UTC.
		// Hence, we need to convert it back to the configured timezone/location.
		valT := val.(time.Time)
		if valT.Location().String() == "UTC" {
			valT, _ = time.ParseInLocation(dtlayoutNano, valT.Format(dtlayoutNano), loc)
		}
		result[v.Name()] = valT

		//TODO: smoke test with godror driver
		//result[v.Name()] = val.(time.Time).In(loc)

	default:
		// assume other date/time types support timezone, convert to the configured timezone/location
		valT := val.(time.Time)
		result[v.Name()] = valT.In(loc)

		// TODO: smoke test with godror driver
		//// first "parse in UTC" and then convert to the target timezone/location
		//// assume other types support timezone,convert to the target timezone/location
		//result[v.Name()], err = time.ParseInLocation(dtlayoutNano, val.(time.Time).Format(dtlayoutNano), time.UTC)
		//result[v.Name()] = result[v.Name()].(time.Time).In(loc)
	}
	return err
}

func (sc *SqlConnect) _transformOracleDuration(result map[string]interface{}, v *sql.ColumnType, val interface{}) error {
	dbTypeName := _normalizeDbTypeName(v)
	var err error = fmt.Errorf("unknown duration column type %s", dbTypeName)

	//fmt.Printf("[DEBUG-_transformOracleDuration] %s/%s - %T/%s\n", v.Name(), v.DatabaseTypeName()+":"+dbTypeName, val, val)

	switch dbTypeName {
	case "INTERVALDS_DTY":
		result[v.Name()], err = ParseOracleIntervalDayToSecond(val.(string))
	case "INTERVALYM_DTY":
		result[v.Name()], err = ParseOracleIntervalYearToMonth(val.(string))
	}
	return err
}

func (sc *SqlConnect) _scanNilValue(result map[string]interface{}, v *sql.ColumnType) error {
	switch {
	case sc.isIntType(v):
		result[v.Name()] = (*int64)(nil)
	case sc.isFloatType(v):
		result[v.Name()] = (*float64)(nil)
	case sc.isStringType(v):
		result[v.Name()] = (*string)(nil)
	case sc.isDateTimeType(v):
		result[v.Name()] = (*time.Time)(nil)
	}
	return nil
}

func (sc *SqlConnect) fetchOneRow(rows *sql.Rows, colsAndTypes []*sql.ColumnType) (map[string]interface{}, error) {
	numCols := len(colsAndTypes)
	vals := make([]interface{}, numCols)
	scanVals := make([]interface{}, numCols)
	for i := 0; i < numCols; i++ {
		scanVals[i] = &vals[i]
	}
	if err := rows.Scan(scanVals...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	result := map[string]interface{}{}
	for i, v := range colsAndTypes {
		//dbTypeName := _normalizeDbTypeName(v)
		//fmt.Printf("[DEBUG] %s/%s - %T/%s\n", v.Name(), v.DatabaseTypeName()+":"+dbTypeName, vals[i], vals[i])

		switch {
		case vals[i] == nil:
			// special care for nil value
			if err := sc._scanNilValue(result, v); err != nil {
				return nil, err
			}
		case sc.flavor == FlavorOracle && sc.isStringType(v) && vals[i] == "":
			// special care for Oracle's empty string
			if err := sc._scanNilValue(result, v); err != nil {
				return nil, err
			}
		case sc.flavor == FlavorSqlite && sc.isNumberType(v):
			var err error
			if sc.isFloatType(v) {
				result[v.Name()], err = toFloatIfValidReal(vals[i])
			} else {
				result[v.Name()], err = toIntIfValidInteger(vals[i])
			}
			if err != nil {
				return nil, err
			}
		case (sc.flavor == FlavorMsSql || sc.flavor == FlavorMySql || sc.flavor == FlavorPgSql) && sc.isNumberType(v):
			isRealNumber := v.ScanType() != nil && (v.ScanType().Name() == "float32" || v.ScanType().Name() == "float64")
			dbTypeName := strings.ToUpper(v.DatabaseTypeName())
			isRealNumber = isRealNumber || dbTypeName == "MONEY" || dbTypeName == "SMALLMONEY"
			if sc.flavor == FlavorPgSql && dbTypeName == "790" {
				// PostgreSQL type 790 is type "MONEY"
				isRealNumber = true
				v := fmt.Sprintf("%s", vals[i])
				v = regexp.MustCompile(`[^\d\.]+`).ReplaceAllString(v, "")
				vals[i] = v
			}
			_, scale, _ := v.DecimalSize()
			isRealNumber = isRealNumber || scale != 0
			var err error
			if isRealNumber {
				result[v.Name()], err = toFloatIfValidReal(vals[i])
			} else {
				result[v.Name()], err = toIntIfValidInteger(vals[i])
			}
			if err != nil {
				return nil, err
			}
		case isValueTypeRawBytes(vals[i]) && sc.isStringType(v):
			// when string is loaded as []byte
			result[v.Name()] = string(vals[i].([]byte))
		case sc.flavor == FlavorSqlite && sc.isDateTimeType(v):
			// special care for SQLite's date/time types
			if err := sc._scanSqliteDateTime(result, v, vals[i]); err != nil {
				return nil, err
			}
		case sc.flavor == FlavorMySql && sc.isDateTimeType(v) && isValueTypeRawBytes(vals[i]):
			// MySQL's TIME/DATE/DATETIME/TIMESTAMP is loaded as []byte
			if err := sc._scanMysqlDateTimeFromRawBytes(result, v, vals[i]); err != nil {
				return nil, err
			}
		case sc.flavor == FlavorPgSql && sc.isDateTimeType(v) && v.ScanType().Kind() == reflect.String:
			// PostgreSQL's TIME is loaded as string
			if err := sc._scanPgsqlDateTimeFromString(result, v, vals[i]); err != nil {
				return nil, err
			}
		case sc.flavor == FlavorPgSql && v.ScanType().Kind() == timeType.Kind():
			// special care for PostgreSQL's date/time types
			if err := sc._transformPgsqlDateTime(result, v, vals[i]); err != nil {
				return nil, err
			}
		case sc.flavor == FlavorMsSql && v.ScanType().Kind() == timeType.Kind():
			// special care for MSSQL's date/time types
			if err := sc._transformMssqlDateTime(result, v, vals[i]); err != nil {
				return nil, err
			}
		case sc.flavor == FlavorOracle && v.ScanType().Kind() == sqlNullTime.Kind():
			// special care for Oracle's date/time types
			if err := sc._transformOracleDateTime(result, v, vals[i]); err != nil {
				return nil, err
			}
		case sc.flavor == FlavorOracle && sc.isDurationType(v):
			// special care for Oracle's duration types
			if err := sc._transformOracleDuration(result, v, vals[i]); err != nil {
				return nil, err
			}
		case sc.flavor == FlavorOracle && sc.isNumberType(v):
			// special care for Oracle's number types
			var err error
			if sc.isIntType(v) {
				result[v.Name()], err = toIntIfValidInteger(vals[i])
			} else {
				result[v.Name()], err = toFloatIfValidReal(vals[i])
			}
			if err != nil {
				return nil, err
			}
		// case sc.flavor == FlavorOracle && strings.ToUpper(v.DatabaseTypeName()) == "NUMBER" && isValueTypeString(vals[i]):
		// 	// special care for Oracle's NUMBER type
		// 	_, scale, _ := v.DecimalSize()
		// 	if scale == 0 {
		// 		result[v.Name()], _ = strconv.ParseInt(vals[i].(string), 10, 64)
		// 	} else {
		// 		result[v.Name()], _ = strconv.ParseFloat(vals[i].(string), 64)
		// 	}
		case sc.flavor == FlavorPgSql && v.ScanType().Kind() == reflect.Interface && sc.isStringType(v):
			// PostgreSQL's CHAR(1) is loaded as []byte old driver version
			_v, ok := vals[i].([]byte)
			if ok {
				result[v.Name()] = string(_v)
			} else {
				result[v.Name()] = vals[i].(string)
			}
		default:
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
	var rowData map[string]interface{}
	for next && rows.Next() {
		if rowData, err = sc.fetchOneRow(rows, colTypes); err != nil {
			next = callback(nil, err)
		} else {
			next = callback(rowData, nil)
		}
	}
	if err != nil {
		return err
	}
	return rows.Err()
}
