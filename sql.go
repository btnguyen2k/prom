package prom

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"time"
)

/*
DbFlavor specifies the flavor or database server/vendor.

Available since v0.1.0
*/
type DbFlavor int

/*
Predefined db flavors.

Available since v0.1.0
*/
const (
	FlavorDefault DbFlavor = iota
	FlavorMySql
	FlavorPgSql
	FlavorMsSql
	FlavorOracle
)

/*
SqlPoolOptions configures database connection pooling options.
*/
type SqlPoolOptions struct {
	/*
		Maximum amount of time a connection may be reused, default is 1 hour,
		see https://golang.org/pkg/database/sql/#DB.SetConnMaxLifetime
	*/
	ConnMaxLifetime time.Duration

	/*
		Maximum number of idle connections in the pool, default is 1,
		see https://golang.org/pkg/database/sql/#DB.SetMaxIdleConns
	*/
	MaxIdleConns int

	/*
		Maximum number of open connections to the database, default is 2,
		see https://golang.org/pkg/database/sql/#DB.SetMaxOpenConns
	*/
	MaxOpenConns int
}

var defaultSqlPoolOptions = SqlPoolOptions{ConnMaxLifetime: 3600 * time.Second, MaxIdleConns: 1, MaxOpenConns: 2}

/*
SqlConnect holds a database/sql DB instance (https://golang.org/pkg/database/sql/#DB) that can be shared within the application.
*/
type SqlConnect struct {
	driver, dsn string          // driver and data source name (DSN)
	poolOptions *SqlPoolOptions // connection pool options
	timeoutMs   int             // default timeout for db operations, in milliseconds
	flavor      DbFlavor        // database flavor
	db          *sql.DB         // database instance
	loc         *time.Location  // timezone location to parse date/time data, new since v0.1.2
}

/*
NewSqlConnect constructs a new SqlConnect instance.

Parameters: see #NewSqlConnectWithFlavor.
*/
func NewSqlConnect(driver, dsn string, defaultTimeoutMs int, poolOptions *SqlPoolOptions) (*SqlConnect, error) {
	return NewSqlConnectWithFlavor(driver, dsn, defaultTimeoutMs, poolOptions, FlavorDefault)
}

/*
NewSqlConnectWithFlavor constructs a new SqlConnect instance.

Available since v0.1.0

Parameters:

	- driver          : database driver name
	- dsn             : data source name (format [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN])
	- defaultTimeoutMs: default timeout for db operations, in milliseconds
	- poolOptions     : connection pool options. If nil, default value is used
	- flavor          : database flavor associated with the SqlConnect instance.

Return: the SqlConnect instance and error (if any). Note:

  - In case of connection error: this function returns the SqlConnect instance and the error.
  - Other error: this function returns (nil, error)
*/
func NewSqlConnectWithFlavor(driver, dsn string, defaultTimeoutMs int, poolOptions *SqlPoolOptions, flavor DbFlavor) (*SqlConnect, error) {
	if defaultTimeoutMs < 0 {
		defaultTimeoutMs = 0
	}
	if poolOptions == nil {
		poolOptions = &defaultSqlPoolOptions
	}
	sc := &SqlConnect{
		driver:      driver,
		dsn:         dsn,
		poolOptions: poolOptions,
		timeoutMs:   defaultTimeoutMs,
		flavor:      flavor,
		loc:         time.UTC,
	}
	db, err := sql.Open(driver, dsn)
	if poolOptions != nil {
		db.SetConnMaxLifetime(poolOptions.ConnMaxLifetime)
		db.SetMaxIdleConns(poolOptions.MaxIdleConns)
		db.SetMaxOpenConns(poolOptions.MaxOpenConns)
	}
	sc.db = db
	return sc, err
}

/*
GetDbFlavor returns the current database flavor associated with this SqlConnect.

Available since v0.1.0
*/
func (sc *SqlConnect) GetDbFlavor() DbFlavor {
	return sc.flavor
}

/*
SetDbFlavor associates a database flavor with this SqlConnect.

Available since v0.1.0
*/
func (sc *SqlConnect) SetDbFlavor(flavor DbFlavor) *SqlConnect {
	sc.flavor = flavor
	return sc
}

/*
GetLocation returns the timezone location associated with this SqlConnect.

Available since v0.1.2
*/
func (sc *SqlConnect) GetLocation() *time.Location {
	return sc.loc
}

/*
SetLocation associates a timezone location with this SqlConnect, used when parsing date/time data. Default value is time.UTC.

Available since v0.1.2
*/
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

/*
NewBackgroundContext creates a new background context with specified timeout in milliseconds.
If there is no specified timeout, or timeout value is less than or equal to 0, the default timeout is used.
*/
func (sc *SqlConnect) NewBackgroundContext(timeoutMs ...int) (context.Context, context.CancelFunc) {
	d := sc.timeoutMs
	if len(timeoutMs) > 0 && timeoutMs[0] > 0 {
		d = timeoutMs[0]
	}
	return context.WithTimeout(context.Background(), time.Duration(d)*time.Millisecond)
}

/*
GetDB returns the underlying 'sql.DB' instance.
*/
func (sc *SqlConnect) GetDB() *sql.DB {
	return sc.db
}

/*
Close closes the underlying 'sql.DB' instance.
*/
func (sc *SqlConnect) Close() error {
	return sc.db.Close()
}

/*
Ping verifies a connection to the database is still alive, establishing a connection if necessary.
*/
func (sc *SqlConnect) Ping(ctx context.Context) error {
	if ctx == nil {
		ctx, _ = sc.NewBackgroundContext()
	}
	return sc.db.PingContext(ctx)
}

/*
IsConnected returns true if the connection to the database is alive.
*/
func (sc *SqlConnect) IsConnected() bool {
	return sc.Ping(nil) == nil
}

/*
Conn returns a single connection by either opening a new connection or returning an existing connection from the connection pool.
Conn will block until either a connection is returned or ctx is canceled/timed-out.

Every 'Conn' must be returned to the database pool after use by calling 'Conn.Close'.
*/
func (sc *SqlConnect) Conn(ctx context.Context) (*sql.Conn, error) {
	if ctx == nil {
		ctx, _ = sc.NewBackgroundContext()
	}
	return sc.db.Conn(ctx)
}

/*
FetchRow copies the columns from the matched row into a slice and return it.
If more than one row matches the query, FetchRow uses only the first row and discards the rest.
If no row matches the query, FetchRow returns (nil,nil).
*/
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
var dbStringTypes = map[string]map[DbFlavor]bool{
	"CHAR":              {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true},
	"VARCHAR":           {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true},
	"TEXT":              {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true},
	"CHARACTER":         {FlavorDefault: true, FlavorPgSql: true},
	"CHARACTER VARYING": {FlavorDefault: true, FlavorPgSql: true},
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
	"TIME":           {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true},
	"DATE":           {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorMsSql: true, FlavorOracle: true},
	"YEAR":           {FlavorDefault: true, FlavorMySql: true},
	"DATETIME":       {FlavorDefault: true, FlavorMySql: true, FlavorMsSql: true},
	"DATETIME2":      {FlavorDefault: true, FlavorMsSql: true},
	"DATETIMEOFFSET": {FlavorDefault: true, FlavorMsSql: true},
	"SMALLDATETIME":  {FlavorDefault: true, FlavorMsSql: true},
	"TIMESTAMP":      {FlavorDefault: true, FlavorMySql: true, FlavorPgSql: true, FlavorOracle: true},
}

func (sc *SqlConnect) isStringType(col *sql.ColumnType) bool {
	name := strings.ToUpper(col.DatabaseTypeName())
	m, e := dbStringTypes[name]
	if !e {
		return false
	}
	_, e = m[sc.flavor]
	return e
}

func (sc *SqlConnect) isDateTimeType(col *sql.ColumnType) bool {
	name := strings.ToUpper(col.DatabaseTypeName())
	m, e := dbDateTimeTypes[name]
	if !e {
		return false
	}
	_, e = m[sc.flavor]
	return e
}

func isRawBytesType(v interface{}) bool {
	t := reflect.TypeOf(v)
	return t == rawBytesType || t == bytesArrType || t == uint8ArrType
}

func (sc *SqlConnect) fetchOneRow(rows *sql.Rows, colsAndTypes []*sql.ColumnType) (map[string]interface{}, error) {
	numCols := len(colsAndTypes)
	vals := make([]interface{}, numCols)
	scanVals := make([]interface{}, numCols)
	for i := 0; i < numCols; i++ {
		scanVals[i] = &vals[i]
	}
	err := rows.Scan(scanVals...)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	result := map[string]interface{}{}
	for k, v := range colsAndTypes {
		if sc.isStringType(v) && isRawBytesType(vals[k]) {
			// when string is loaded as []byte
			result[v.Name()] = string(vals[k].([]byte))
		} else if sc.flavor == FlavorMySql && sc.isDateTimeType(v) && isRawBytesType(vals[k]) {
			// Mysql's TIME/DATE/DATETIME/TIMESTAMP is loaded as []byte
			loc := sc.ensureLocation()
			var err error
			switch strings.ToUpper(v.DatabaseTypeName()) {
			case "TIME":
				result[v.Name()], err = time.ParseInLocation("15:04:05", string(vals[k].([]byte)), loc)
			case "DATE":
				result[v.Name()], err = time.ParseInLocation("2006-01-02", string(vals[k].([]byte)), loc)
			case "DATETIME":
				result[v.Name()], err = time.ParseInLocation("2006-01-02 15:04:05", string(vals[k].([]byte)), loc)
			case "TIMESTAMP":
				result[v.Name()], err = time.ParseInLocation("2006-01-02 15:04:05", string(vals[k].([]byte)), loc)
			default:
				return nil, err
			}
			if err != nil {
				return nil, err
			}
		} else if sc.flavor == FlavorPgSql && v.ScanType().Kind() == reflect.Interface && sc.isStringType(v) {
			// Postgresql's CHAR(1) is loaded as []byte
			result[v.Name()] = string(vals[k].([]byte))
		} else {
			result[v.Name()] = vals[k]
		}
	}
	return result, err
}

/*
FetchRows loads rows from database and transform to a slice of 'map[string]interface{}' where each column's name & value is a map entry.
If no row matches the query, FetchRow returns (<empty slice>,nil).

Note: FetchRows does NOT call 'rows.close()' when done!
*/
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

/*
FetchRowsCallback loads rows from database. For each row, FetchRowsCallback transforms it to 'map[string]interface{}', where each column's name & value is a map entry, and passes the map to the callback function.
FetchRowsCallback stops the loop when there is no more row to load or 'callback' function returns 'false'.

Note: FetchRowsCallback does NOT call 'rows.close()' when done!
*/
func (sc *SqlConnect) FetchRowsCallback(rows *sql.Rows, callback func(row map[string]interface{}, err error) bool) error {
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	for rows.Next() {
		var next bool
		rowData, err := sc.fetchOneRow(rows, colTypes)
		if err != nil {
			next = callback(nil, err)
		} else {
			next = callback(rowData, nil)
		}
		if !next {
			break
		}
	}
	return rows.Err()
}
