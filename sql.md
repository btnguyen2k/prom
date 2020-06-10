**'Prom' for the `database/sql` package (https://golang.org/pkg/database/sql/)**

Drivers:
- [x] MySQL: [github.com/go-sql-driver/mysql](https://github.com/go-sql-driver/mysql).
- [x] PostgreSQL: [github.com/lib/pq](https://github.com/lib/pq).
- [x] Oracle: [github.com/godror/godror](https://github.com/go-goracle/goracle).
- [ ] MSSQL: [github.com/denisenkom/go-mssqldb](https://github.com/denisenkom/go-mssqldb) (_experimental_).

Sample usage (MySQL):

```golang
import (
	"github.com/btnguyen2k/prom"
	_ "github.com/go-sql-driver/mysql"
	...
)

driver := "mysql"
dsn := "test:test@tcp(localhost:3306)/test"
sqlConnect, err := prom.NewSqlConnect(driver, dsn, 10000, nil)
if sqlConnect == nil || err != nil {
    if err != nil {
	    fmt.Println("Error:", err)
	}
	if sqlConnect == nil {
		panic("error creating [prom.SqlConnect] instance")
	}
}

// from now on, one SqlConnect instance can be shared & used by all goroutines within the application

//execute SQL statement to drop & create table
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

//insert some rows
sql = "INSERT INTO tbl_demo (id, name) VALUES (?, ?)"
n := 100
fmt.Printf("Inserting %d rows to table [tbl_demo]\n", n)
for i := 1; i <= n; i++ {
    id := i
	name := time.Unix(int64(rand.Int31()), rand.Int63())
	sqlConnect.GetDB().Exec(sql, id, name.String())
}
```

See usage examples in [examples directory](examples/). Documentation at [![GoDoc](https://godoc.org/github.com/btnguyen2k/prom?status.svg)](https://godoc.org/github.com/btnguyen2k/prom#SqlConnect)

See also [database/sql](https://golang.org/pkg/database/sql/) package.
