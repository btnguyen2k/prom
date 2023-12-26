package main

import (
	"database/sql"
	"fmt"
	promsql "github.com/btnguyen2k/prom/sql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"os"
)

func newSqlConnect() *promsql.SqlConnect {
	driver := "pgx"
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://test:test@localhost:5432/test?sslmode=disable&client_encoding=UTF-8&application_name=myapp"
	}
	sqlConnect, err := promsql.NewSqlConnectWithFlavor(driver, dsn, 10000, nil, promsql.FlavorPgSql)
	if sqlConnect == nil || err != nil {
		panic(fmt.Errorf("error creating SqlConnect instance: %s", err))
	}
	return sqlConnect
}

func executeSql(sqlC *promsql.SqlConnect, sql string, params ...interface{}) sql.Result {
	result, err := sqlC.GetDB().Exec(sql, params...)
	if err != nil {
		panic(err)
	}
	return result
}

func executeQuery(sqlC *promsql.SqlConnect, sql string, params ...interface{}) *sql.Rows {
	rows, err := sqlC.GetDB().Query(sql, params...)
	if err != nil {
		panic(err)
	}
	return rows
}
