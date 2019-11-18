# prom

[![Go Report Card](https://goreportcard.com/badge/github.com/btnguyen2k/prom)](https://goreportcard.com/report/github.com/btnguyen2k/prom)
[![GoDoc](https://godoc.org/github.com/btnguyen2k/prom?status.svg)](https://godoc.org/github.com/btnguyen2k/prom)

Utility library to manage shared connection in Golang.

## Documentation

- ['Prom' for go-redis](go-redis.md).
- ['Prom' for the official Go driver for MongoDB](mongo.md).
- ['Prom' for database/sql](sql.md).
- ['Prom' for AWS DyamoDB](aws-dynamodb.md).

Go version, direct and tested dependencies: see [go.mod](go.mod).

## History

### 2019-11-17 - v0.2.5

- `AwsDynamodbConnect`:
  - Add new function `IsAwsError(err error, code string) bool`
  - No longer ignore certain AWS errors, lets caller decide to call `AwsIgnoreErrorIfMatched` if needed.
- Update dependency libs.


### 2019-11-14 - v0.2.4

- `AwsDynamodbConnect`:
  - New constants `AwsAttrTypeString`, `AwsAttrTypeNumber` and `AwsAttrTypeBinary`.
  - New constants `AwsKeyTypePartition` and `AwsKeyTypeSort`.
  - Add transaction-supported functions.
- Update dependency libs.
- Other fixes & enhancements.


### 2019-10-30 - v0.2.3

- `MongoConnect`: fixed a bug that incorrectly creates collection index when passing `mongo.IndexModel` as parameter.


### 2019-10-25 - v0.2.2

- Change `MongoConnect.Ping(timeoutMs ...int)` to `MongoConnect.Ping(ctx context.Context)`.
- Bump Go version to `1.12` and update dependencies.
- Other fixes & enhancements.


### 2019-10-14 - v0.2.1

- ['Prom' for the official Go driver for MongoDB](mongo.md):
  - Deprecate function `CreateIndexes`, replaced with `CreateCollectionIndexes`.
- Add tests.


### 2019-10-12 - v0.2.0

- New ['Prom' for AWS DyamoDB](aws-dynamodb.md):
  - [Usage examples](examples/example_aws-dynamodb_base.go).
- For API consistency:
  - New function `MongoConnect.Close(context.Context) error` to replace `Disconnect(ctx context.Context) error`.
  - New function `GoRedisConnect.Close() error`.
- ['Prom' for go-redis](go-redis.md):
  - Upgrade to [github.com/go-redis/redis](https://github.com/go-redis/redis) `v6.15.6`.
- ['Prom' for the official Go driver for MongoDB](mongo.md):
  - Upgrade to [go.mongodb.org/mongo-driver](https://godoc.org/go.mongodb.org/mongo-driver/) `v1.1.2`.
- ['Prom' for database/sql](sql.md):
  - Upgrade to [gopkg.in/goracle.v2](https://github.com/go-goracle/goracle) `v2.21.4`.


### 2019-09-14 - v0.1.3

- ['Prom' for go-redis](go-redis.md):
  - Upgrade to [github.com/go-redis/redis](https://github.com/go-redis/redis) `v6.15.5`.
- ['Prom' for the official Go driver for MongoDB](mongo.md):
  - Upgrade to [go.mongodb.org/mongo-driver](https://godoc.org/go.mongodb.org/mongo-driver/) `v1.1.1`.
  - Fixed bug `"no documents in result"`
- ['Prom' for database/sql](sql.md):
  - Upgrade to [github.com/lib/pq](https://github.com/lib/pq) `v1.2.0`.
  - Upgrade to [gopkg.in/goracle.v2](https://github.com/go-goracle/goracle) `v2.20.1`.


### 2019-04-03 - v0.1.2

- ['Prom' for database/sql](sql.md):
  - Add timezone location attribute to `SqlConnect` struct.
  - Correctly parse date/time data from db using timezone location attribute.


### 2019-04-01 - v0.1.1

- ['Prom' for database/sql](sql.md): solve the case when Mysql's `TIME` is loaded as `[]byte`.


### 2019-03-27 - v0.1.0

- Migrated Go modular design.
- Add `DbFlavor`:
  - New method `NewSqlConnectWithFlavor`
- `SqlConnect.fetchOneRow` fetches correct column's data type instead of `[]byte` for MySQL.
  This makes results of `SqlConnect.FetchRows` and `SqlConnect.FetchRowsCallback` are correctly typed, too.
- Update examples for [MongoDB](examples/example_mongo.go), [MySQL](examples/example_mysql.go) and [PostgreSQL](examples/example_pgsql.go).
- Add examples for [MSSQL](examples/example_mssql.go) and [Oracle DB](examples/example_oracle.go).


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
