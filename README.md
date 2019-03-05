# prom

[![Go Report Card](https://goreportcard.com/badge/github.com/btnguyen2k/prom)](https://goreportcard.com/report/github.com/btnguyen2k/prom)
[![GoDoc](https://godoc.org/github.com/btnguyen2k/prom?status.svg)](https://godoc.org/github.com/btnguyen2k/prom)

Utility library to manage shared connection in Golang.

## Documentation

- ['Prom' for the official Go driver for MongoDB](mongo.md).

## History

### 2019-03-04 - v0.0.2

- Function `DecodeSingleResult` and `DecodeResultCallback` are now attached to `*MongoConnect`
- Change Mongo's timeout parameter from `int64` to `int`

### 2019-02-28 - v0.0.1

- 'Prom' for the official Go driver for MongoDB (https://github.com/mongodb/mongo-go-driver)

## License

MIT - see [LICENSE.md](LICENSE.md).
