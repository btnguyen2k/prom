**'Prom' for the `database/sql` package (https://pkg.go.dev/database/sql)**

[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/prom/sql)](https://pkg.go.dev/github.com/btnguyen2k/prom/sql)

This package helps with managing shared `database/sql` connections and handling niche cases with various drivers and database types.

Supported/Tested databases and drivers:
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



Sample usage (MySQL):

```go
import (
	"github.com/btnguyen2k/prom/sql"
	_ "github.com/go-sql-driver/mysql"
)

driver := "mysql"
dsn := "user:passwd@tcp(localhost:3306)/dbname"
timeoutMs := 10000
sqlConnect, err := sql.NewSqlConnectWithFlavor(driver, dsn, timeoutMs, nil, sql.FlavorMySql)
if sqlConnect == nil || err != nil {
	panic("error creating SqlConnect instance")
}

// from now on, one SqlConnect instance can be shared & used by all goroutines within the application

// execute SQL statement to drop & create table
sql := "DROP TABLE IF EXISTS tbl_demo"
_, err := sqlConnect.GetDB().Exec(sql)
if err != nil {
    fmt.Printf("Error while executing query [%s]: %e\n", sql, err)
} else {
    fmt.Println("Dropped table [tbl_demo]")
    sql = "CREATE TABLE tbl_demo (id INT, name TEXT, PRIMARY KEY(id))"
    _, err = sqlConnect.GetDB().Exec(sql)
	if err != nil {
	    fmt.Printf("Error while executing query [%s]: %e\n", sql, err)
    } else {
	    fmt.Println("Created table [tbl_demo]")
	}
}

// insert some rows
sql = "INSERT INTO tbl_demo (id, name) VALUES (?, ?)"
n := 100
fmt.Printf("Inserting %d rows to table [tbl_demo]\n", n)
for i := 1; i <= n; i++ {
    id := i
	name := time.Unix(int64(rand.Int31()), rand.Int63())
	sqlConnect.GetDB().Exec(sql, id, name.String())
}
```

See more:
- [examples](../examples/sql/)
- [database/sql](https://golang.org/pkg/database/sql/)
- [SQL database drivers](https://github.com/golang/go/wiki/SQLDrivers) for Go
