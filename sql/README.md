**'Prom' for the `database/sql` (https://pkg.go.dev/database/sql)**

[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/prom/sql)](https://pkg.go.dev/github.com/btnguyen2k/prom/sql)

This package helps with managing shared `database/sql` connections and handling niche cases with various drivers and database types.

## Sample usage (PostgreSQL):

```go
package main

import (
	"fmt"
	promsql "github.com/btnguyen2k/prom/sql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"math/rand"
	"time"
)

func main() {
	driver := "pgx"
	dsn := "postgres://username:password@localhost:5432/test?sslmode=disable&client_encoding=UTF-8&application_name=myapp"
	timeoutMs := 10000
	sqlConnect, err := promsql.NewSqlConnectWithFlavor(driver, dsn, timeoutMs, nil, promsql.FlavorPgSql)
	if sqlConnect == nil || err != nil {
		panic("error creating SqlConnect instance")
	}

	// from now on, one SqlConnect instance can be shared & used by all goroutines within the application
	defer sqlConnect.Close()

	// execute SQL statement to drop & create table
	sqlStm := "DROP TABLE IF EXISTS tbl_demo"
	_, err = sqlConnect.GetDB().Exec(sqlStm)
	if err != nil {
		fmt.Printf("Error while executing query [%s]: %e\n", sqlStm, err)
	} else {
		fmt.Println("Dropped table [tbl_demo]")
		sqlStm = "CREATE TABLE tbl_demo (id INT, name TEXT, PRIMARY KEY(id))"
		_, err = sqlConnect.GetDB().Exec(sqlStm)
		if err != nil {
			fmt.Printf("Error while executing query [%s]: %e\n", sqlStm, err)
		} else {
			fmt.Println("Created table [tbl_demo]")
		}
	}

	// insert some rows
	sqlStm = "INSERT INTO tbl_demo (id, name) VALUES (?, ?)"
	n := 100
	fmt.Printf("Inserting %d rows to table [tbl_demo]\n", n)
	for i := 1; i <= n; i++ {
		id := i
		name := time.Unix(int64(rand.Int31()), rand.Int63())
		sqlConnect.GetDB().Exec(sqlStm, id, name.String())
	}
}
```

See more:
- [examples](../examples)
- [database/sql](https://golang.org/pkg/database/sql/)
- [SQL database drivers](https://github.com/golang/go/wiki/SQLDrivers) for Go

## Features

**Shared `database/sql` connection pool with additional utilities.**

`SqlConnect` is `database/sql` connection pool that can be shared by all goroutines within the application.
Create a new instance of `SqlConnect` with `NewSqlConnectWithFlavor()`.

See [examples](../examples/PromBasic.go) for more details.

**Utility functions to help fetching rows resulted from SQL queries and mapping to Go data types.**

See [examples](../examples/PromFetchRows.go) for more details.

**Easy date/time/duration handling.**

- Date/time values are automatically converted to/from `time.Time` with timezone configured via `SqlConnect.SetLocation()`.
- `github.com/go-sql-driver/mysql`'s `parseTime` parameter is automatically handled.
- Oracle's `INTERVAL DAY TO SECOND` and `INTERVAL YEAR TO MONTH` are automatically converted to `time.Duration`.
  - A month is assumed to have 30 days, and a year is 12 months. Hence, the conversion from/to `INTERVAL YEAR TO MONTH` to/from `time.Duration` is _approximated_ only!

See [examples](../examples/PromDatetime.go) for more details.

**Proxy to log executed queries and/or measure execution time.**

Query execution is automatically logged and measured for execution time if executed
via proxy version of `sql.DB` (obtained from `SqlConnect.GetDBProxy()`) or `sql.Conn`
(obtained from `SqlConnect.ConnProxy()`).

See [examples](../examples/PromLogAndMetrics.go) for more details.

**Others**

- Database's `NULL` values are converted to corresponding Go's `nil` points:
  - `NULL` integer is converted to `(*int64)(nil)`
  - `NULL` float is converted to `(*float64)(nil)`
  - `NULL` string is converted to `(*string)(nil)`
  - `NULL` date/time is converted to `(*time.Time)(nil)`
- `SQLite`'s numbers are converted to correct Go data types: `int64` for integers, `float64` for floats.
- In the case a string is loaded as `[]byte`, it is mapped to Go `string` type automatically.

## Supported/Tested databases and drivers:

- SQLite:
  - Drivers:
    - [github.com/mattn/go-sqlite3](https://github.com/mattn/go-sqlite3): CGO
    - [modernc.org/sqlite](https://modernc.org/sqlite): pure Go
- PostgreSQL:
  - PostgreSQL versions: 11, 12, 13, 14, 15, 16
  - Drivers:
    - [github.com/jackc/pgx/v5](https://github.com/jackc/pgx): pure Go
- MySQL:
  - MySQL versions: 5.7, 8.0, 8.2
  - Drivers:
    - [github.com/go-sql-driver/mysql](github.com/go-sql-driver/mysql): pure Go
- MSSQL:
  - MSSQL versions: 2017-latest, 2019-latest, 2022-latest
  - Drivers:
    - [github.com/microsoft/go-mssqldb](https://github.com/microsoft/go-mssqldb): pure Go
    - [github.com/denisenkom/go-mssqldb](https://github.com/denisenkom/go-mssqldb): pure Go (deprecated soon, use Microsoft's driver)
- Oracle:
  - Oracle database versions: 18.4.0-xe, 21.3.0-xe
  - Drivers:
    - [github.com/sijms/go-ora/v2](https://github.com/sijms/go-ora): pure Go
    - [github.com/godror/godror](https://github.com/godror/godror): CGO
