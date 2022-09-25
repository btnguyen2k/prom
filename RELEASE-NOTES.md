# prom release notes

## 2022-10-24 - v0.3.0

Support simple logging and metrics:
- New struct `CmdExecInfo`: information around an executing command.
- New interface `IMetricsLogger`: APIs to log command executions and retrieve metrics.
- Dependency libs updated/upgraded:
  - AWS DynamoDB: `github.com/aws/aws-sdk-go v1.44.44`
  - Redis: `github.com/go-redis/redis/v8 v8.11.5`
  - MongoDB: `go.mongodb.org/mongo-driver v1.9.1`
  - (database/sql driver) Azure Cosmos DB: `github.com/btnguyen2k/gocosmos v0.1.6`
  - (database/sql driver) MSSQL: `github.com/denisenkom/go-mssqldb v0.12.2`
  - (database/sql driver) MySQL: `github.com/go-sql-driver/mysql v1.6.0`
  - (database/sql driver) Oracle: `github.com/godror/godror v0.33.3`
  - (database/sql driver) PostgreSQL: `github.com/jackc/pgx/v4 v4.16.1`
  - (database/sql driver) SQLite3: `github.com/mattn/go-sqlite3 v1.14.14`

## 2021-10-10 - v0.2.15

- ['Prom' for AWS DyamoDB](dynamodb/aws-dynamodb.md):
  - New struct `AwsQueryOpt`: to supply additional options to `AwsDynamodbConnect.QueryItems` and `AwsDynamodbConnect.QueryItemsWithCallback`.
  - `AwsDynamodbConnect.QueryItems` and `AwsDynamodbConnect.QueryItemsWithCallback` now support `ScanIndexBackward`.

## 2021-09-22 - v0.2.14

- ['Prom' for AWS DyamoDB](dynamodb/aws-dynamodb.md): new helper functions:
  - `AwsDynamodbWaitForGsiStatus`: periodically check if table's GSI status reaches a desired value, or timeout.
  - `AwsDynamodbWaitForTableStatus`: periodically check if table's status reaches a desired value, or timeout.

## 2021-09-06 - v0.2.13

- ['Prom' for the official Go driver for MongoDB](mongo.md):
  - New exported variables: `MongoPoolOptsLongDistance`, `MongoPoolOptsGeneral` and MongoPoolOptsFailFast`.
  - Default pool options are now `MongoPoolOptsGeneral`.

## 2021-08-30 - v0.2.12

- ['Prom' for database/sql](sql.md): bug fixes & enhancements with date/time types.

## 2021-03-10 - v0.2.11

- ['Prom' for database/sql](sql.md): bug fixes, enhancements and unit test rewritten.

## 2021-02-19 - v0.2.10

- ['Prom' for database/sql](sql.md):
  - Quick fix for Oracle's `NUMBER` data type.

## 2020-12-26 - v0.2.9

- ['Prom' for database/sql](sql.md):
  - Add `FlavorCosmosDb` && [Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db/) support.
- More unit tests.
- Update dependencies.
- Other fixes & enhancements.

## 2020-10-25 - v0.2.8

- ['Prom' for go-redis](go-redis.md):
  - Add `RedisPoolOpts` struct.
  - `GoRedisConnect.GetClient`, `GoRedisConnect.GetFailoverClient` and `GoRedisConnect.GetClusterClient` adhere to Redis connection pool settings.
- ['Prom' for the official Go driver for MongoDB](mongo.md):
  - Add `MongoPoolOpts` struct.
  - New getter/setter & `MongoConnect.Init` functions.
  - `MongoConnect.NewContext` now returns single `context.Context` result.
  - New functions `MongoConnect.NewContextIfNil` and `MongoConnect.NewContextWithCancel`.
- ['Prom' for database/sql](sql.md):
  - New getter/setter & `SqlConnect.Init` functions
  - New db flavor for SQLite
  - `SqlConnect.NewContext` now returns single `context.Context` result
  - New functions `SqlConnect.NewContextIfNil` and `SqlConnect.NewContextWithCancel`
- More unit tests.
- Update dependencies.
- Other fixes & enhancements.

## 2020-06-10 - v0.2.7

- ['Prom' for AWS DyamoDB](dynamodb/aws-dynamodb.md):
  - `AwsDynamodbConnect.PutItemIfNotExist` returns `(nil, nil)` if the item being put already existed.
- ['Prom' for the official Go driver for MongoDB](mongo.md):
  - Clean up deprecated functions.
- ['Prom' for database/sql](sql.md):
  - Clean up deprecated functions.
  - Migrate Oracle driver to `github.com/godror/godror` due to naming (trademark) issues.
  - Migrate PostgreSQL driver to `github.com/jackc/pgx/v4/stdlib`.
- More unit tests.
- Update dependencies.
- Other fixes & enhancements.

## 2019-11-19 - v0.2.6

- ['Prom' for AWS DyamoDB](dynamodb/aws-dynamodb.md):
  - New functions `AwsDynamodbConnect.BuildxxxInput` and `AwsDynamodbConnect.xxxWithInput`.
  - Doc fixes and updates.

## 2019-11-17 - v0.2.5

- ['Prom' for AWS DyamoDB](dynamodb/aws-dynamodb.md):
  - Add new function `IsAwsError(err error, code string) bool`
  - No longer ignore certain AWS errors, lets caller decide to call `AwsIgnoreErrorIfMatched` if needed.
- Update dependencies.
- Other fixes & enhancements.

## 2019-11-14 - v0.2.4

- ['Prom' for AWS DyamoDB](dynamodb/aws-dynamodb.md):
  - New constants `AwsAttrTypeString`, `AwsAttrTypeNumber` and `AwsAttrTypeBinary`.
  - New constants `AwsKeyTypePartition` and `AwsKeyTypeSort`.
  - Add transaction-supported functions.
- Update dependencies.
- Other fixes & enhancements.

## 2019-10-30 - v0.2.3

- ['Prom' for the official Go driver for MongoDB](mongo.md):
  - `MongoConnect`: fixed a bug that incorrectly creates collection index when passing `mongo.IndexModel` as parameter.

## 2019-10-25 - v0.2.2

- ['Prom' for the official Go driver for MongoDB](mongo.md):
  - Change `MongoConnect.Ping(timeoutMs ...int)` to `MongoConnect.Ping(ctx context.Context)`.
- Bump Go version to `1.12` and update dependencies.
- Other fixes & enhancements.

## 2019-10-14 - v0.2.1

- ['Prom' for the official Go driver for MongoDB](mongo.md):
  - Deprecate function `CreateIndexes`, replaced with `CreateCollectionIndexes`.
- Add tests.

## 2019-10-12 - v0.2.0

- New ['Prom' for AWS DyamoDB](dynamodb/aws-dynamodb.md):
  - AWS SDK for Go: https://github.com/aws/aws-sdk-go
  - Type: `AwsDynamodbConnect`.
  - [Usage examples](examples/example_aws-dynamodb_base.go).
- For API consistency:
  - New function `MongoConnect.Close(context.Context) error` to replace `Disconnect(ctx context.Context) error`.
  - New function `GoRedisConnect.Close() error`.
- Libs upgraded:
  - Upgrade to [github.com/go-redis/redis](https://github.com/go-redis/redis) `v6.15.6`.
  - Upgrade to [go.mongodb.org/mongo-driver](https://godoc.org/go.mongodb.org/mongo-driver/) `v1.1.2`.
  - Upgrade to [gopkg.in/goracle.v2](https://github.com/go-goracle/goracle) `v2.21.4`.

## 2019-09-14 - v0.1.3

- ['Prom' for the official Go driver for MongoDB](mongo.md):
  - Fixed bug `"no documents in result"`
- Libs upgraded:
  - Upgrade to [github.com/go-redis/redis](https://github.com/go-redis/redis) `v6.15.5`.
  - Upgrade to [go.mongodb.org/mongo-driver](https://godoc.org/go.mongodb.org/mongo-driver/) `v1.1.1`.
  - Upgrade to [github.com/lib/pq](https://github.com/lib/pq) `v1.2.0`.
  - Upgrade to [gopkg.in/goracle.v2](https://github.com/go-goracle/goracle) `v2.20.1`.

## 2019-04-03 - v0.1.2

- ['Prom' for database/sql](sql.md):
  - Add timezone location attribute to `SqlConnect` struct.
  - Correctly parse date/time data from db using timezone location attribute.

## 2019-04-01 - v0.1.1

- ['Prom' for database/sql](sql.md): solve the case when Mysql's `TIME` is loaded as `[]byte`.

## 2019-03-27 - v0.1.0

- Migrated Go modular design.
- Add `DbFlavor`:
  - New method `NewSqlConnectWithFlavor`
- `SqlConnect.fetchOneRow` fetches correct column's data type instead of `[]byte` for MySQL.
  This makes results of `SqlConnect.FetchRows` and `SqlConnect.FetchRowsCallback` are correctly typed, too.
- Update examples for [MongoDB](examples/example_mongo.go), [MySQL](examples/example_mysql.go) and [PostgreSQL](examples/example_pgsql.go).
- Add examples for [MSSQL](examples/example_mssql.go) and [Oracle DB](examples/example_oracle.go).

## 2019-03-19 - v0.0.4

- New ['Prom' for database/sql](sql.md):
  - Go's database/sql package: https://pkg.go.dev/database/sql
  - Type: `SqlConnect`.
  - Usage examples: [MySQL](examples/example_mysql.go), [PostgreSQL](examples/example_pgsql.go)

## 2019-03-08 - v0.0.3.1

- ['Prom' for the official Go driver for MongoDB](mongo.md): add 2 functions
  - `DecodeSingleResultRaw(*mongo.SingleResult) (string, error)`
  - `DecodeResultCallbackRaw(context.Context, *mongo.Cursor, func(docNum int, doc string, err error))`
- Bug fixes & refactoring.

## 2019-03-05 - v0.0.3

- ['Prom' for the official Go driver for MongoDB](mongo.md):
  - Bug fixes, enhancements & refactoring
  - Add [usage examples](examples/example_mongo.go)
- New ['Prom' for go-redis](go-redis.md):
  - go-redis: https://github.com/go-redis/redis
  - Type: `GoRedisConnect`.
  - [Usage examples](examples/example_go-redis.go)

## 2019-03-04 - v0.0.2

- Function `DecodeSingleResult` and `DecodeResultCallback` are now attached to `*MongoConnect`
- Change Mongo's timeout parameter from `int64` to `int`

## 2019-02-28 - v0.0.1

- New ['Prom' for the official Go driver for MongoDB](mongo.md):
  - Go driver for MongoDB: https://github.com/mongodb/mongo-go-driver
  - Type: `MongoConnect`.
