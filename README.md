# prom

[![Go Report Card](https://goreportcard.com/badge/github.com/btnguyen2k/prom)](https://goreportcard.com/report/github.com/btnguyen2k/prom)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/prom)](https://pkg.go.dev/github.com/btnguyen2k/prom)
[![Actions Status](https://github.com/btnguyen2k/prom/workflows/prom/badge.svg)](https://github.com/btnguyen2k/prom/actions)
[![codecov](https://codecov.io/gh/btnguyen2k/prom/branch/master/graph/badge.svg?token=EBTGTZMSUV)](https://codecov.io/gh/btnguyen2k/prom)
[![Release](https://img.shields.io/github/release/btnguyen2k/prom.svg?style=flat-square)](RELEASE-NOTES.md)

Utility library to manage shared connections in Go.

## Documentations

- ['Prom' for AWS DyamoDB](dynamodb/).
- ['Prom' for the official Go driver for MongoDB](mongo/).
- ['Prom' for go-redis](goredis/).
- ['Prom' for database/sql](sql/).

## Examples

- ['Prom' for AWS DyamoDB](./examples/dynamodb/).
- ['Prom' for the official Go driver for MongoDB](./examples/mongo/).
- ['Prom' for go-redis](./examples/goredis/).
- ['Prom' for database/sql](./examples/sql/).

## Supported 3rd party libraries/drivers

`prom` is supporting and has been tested against following libraries/drivers+version:

- Redis: `github.com/go-redis/redis/v8 v8.11.5`
- MongoDB: `go.mongodb.org/mongo-driver v1.9.1`
- (database/sql driver) Azure Cosmos DB: `github.com/btnguyen2k/gocosmos v0.1.6`
- (database/sql driver) MSSQL: `github.com/denisenkom/go-mssqldb v0.12.2`
- (database/sql driver) MySQL: `github.com/go-sql-driver/mysql v1.6.0`
- (database/sql driver) Oracle: `github.com/godror/godror v0.33.3`
- (database/sql driver) PostgreSQL: `github.com/jackc/pgx/v4 v4.16.1`
- (database/sql driver) SQLite3: `github.com/mattn/go-sqlite3 v1.14.14`

## License

MIT - see [LICENSE.md](LICENSE.md).
