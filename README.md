# prom

[![Go Report Card](https://goreportcard.com/badge/github.com/btnguyen2k/prom)](https://goreportcard.com/report/github.com/btnguyen2k/prom)
[![GoDoc](https://godoc.org/github.com/btnguyen2k/prom?status.svg)](https://godoc.org/github.com/btnguyen2k/prom)

Utility library to manage shared connection in Golang.

## Documentation

- ['Prom' for go-redis](go-redis.md).
- ['Prom' for the official Go driver for MongoDB](mongo.md).


## History

### 2019-03-19 - v0.0.4

- ['Prom' for database/sql](sql.md):
  - Usage examples: [MySQL](examples/example_mysql.go), [PostgreSQL](examples/example_pgsql.go)


### 2019-03-08 - v0.0.3.1

- ['Prom' for the official Go driver for MongoDB](mongo.md): add 2 functions
  - `DecodeSingleResultRaw(*mongo.SingleResult) (string, error)`
  - `DecodeResultCallbackRaw(context.Context, *mongo.Cursor, func(docNum int, doc string, err error))`
- Bug fixes & refactoring.


### 2019-03-05 - v0.0.3

- ['Prom' for the official Go driver for MongoDB](mongo.md):
  - Bug fixes, enhancements & refactoring
  - Add [usage examples](examples/example_mongo.go)
- New ['Prom' for go-redis](go-redis.md):  
  - [Usage examples](examples/example_go-redis.go)

### 2019-03-04 - v0.0.2

- Function `DecodeSingleResult` and `DecodeResultCallback` are now attached to `*MongoConnect`
- Change Mongo's timeout parameter from `int64` to `int`

### 2019-02-28 - v0.0.1

- 'Prom' for the official Go driver for MongoDB (https://github.com/mongodb/mongo-go-driver)

## License

MIT - see [LICENSE.md](LICENSE.md).
