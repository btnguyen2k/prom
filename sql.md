**'Prom' for the `database/sql` package (https://golang.org/pkg/database/sql/)**

[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/prom)](https://pkg.go.dev/github.com/btnguyen2k/prom#SqlConnect)

Prom for `database/sql` has been tested and supports with the following drivers:

- [x] Azure Cosmos DB: [github.com/btnguyen2k/gocosmos](https://github.com/btnguyen2k/gocosmos).
- [x] MSSQL: [github.com/denisenkom/go-mssqldb](https://github.com/denisenkom/go-mssqldb).
- [x] MySQL: [github.com/go-sql-driver/mysql](https://github.com/go-sql-driver/mysql).
- [x] Oracle: [github.com/godror/godror](https://github.com/go-goracle/goracle).
- [x] PostgreSQL:[github.com/jackc/pgx](https://github.com/jackc/pgx).
- [x] SQLite3: [github.com/mattn/go-sqlite3](https://github.com/mattn/go-sqlite3).

Sample usage (MySQL):

```go
import (
	"github.com/btnguyen2k/prom"
	_ "github.com/go-sql-driver/mysql"
	...
)

driver := "mysql"
dsn := "user:passwd@tcp(localhost:3306)/dbname"
timeoutMs := 10000
sqlConnect, err := prom.NewSqlConnectWithFlavor(driver, dsn, timeoutMs, nil, prom.FlavorMySql)
if sqlConnect == nil || err != nil {
    if err != nil {
	    fmt.Println("Error:", err)
	}
	if sqlConnect == nil {
		panic("error creating [prom.SqlConnect] instance")
	}
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
- [examples](examples/)
- Package [database/sql](https://golang.org/pkg/database/sql/)
- [SQL database drivers](https://github.com/golang/go/wiki/SQLDrivers) for Go
