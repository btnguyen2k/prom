package prom

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

/*
SqlConnect configures database connection pooling options.
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
	db          *sql.DB         // database instance
}

/*
NewSqlConnect constructs a new SqlConnect instance.

Parameters:

  - driver          : database driver name
  - dsn             : data source name (format [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN])
  - defaultTimeoutMs: default timeout for db operations, in milliseconds
  - poolOptions     : connection pool options. If nil, default value is used

Return: the SqlConnect instance and error (if any). Note:

  - In case of connection error: this function returns the SqlConnect instance and the error.
  - Other error: this function returns (nil, error)
*/
func NewSqlConnect(driver, dsn string, defaultTimeoutMs int, poolOptions *SqlPoolOptions) (*SqlConnect, error) {
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
NewBackgroundContext creates a new background context with specified timeout in milliseconds.
If there is no specified timeout, or timeout value is less than or equal to 0, the default timeout is used.
*/
func (m *SqlConnect) NewBackgroundContext(timeoutMs ...int) (context.Context, context.CancelFunc) {
	d := m.timeoutMs
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

func (sc *SqlConnect) fetchOneRow(rows *sql.Rows, cols []string) (map[string]interface{}, error) {
	numCols := len(cols)
	vals := make([]interface{}, numCols)
	scanVals := make([]interface{}, numCols)
	for i := 0; i < numCols; i++ {
		scanVals[i] = &vals[i]
	}
	err := rows.Scan(scanVals...)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	result := map[string]interface{}{}
	for k, v := range cols {
		result[v] = vals[k]
	}
	return result, err
}

/*
FetchRows loads rows from database and transform to a slice of 'map[string]interface{}' where each column's name & value is a map entry.
If no row matches the query, FetchRow returns (<empty slice>,nil).

Note: FetchRows does NOT call 'rows.close()' when done!
*/
func (sc *SqlConnect) FetchRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	result := make([]map[string]interface{}, 0)
	for rows.Next() {
		rowData, err := sc.fetchOneRow(rows, cols)
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
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	for rows.Next() {
		var next bool
		rowData, err := sc.fetchOneRow(rows, cols)
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
