# prom

[![Go Report Card](https://goreportcard.com/badge/github.com/btnguyen2k/prom)](https://goreportcard.com/report/github.com/btnguyen2k/prom)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/prom)](https://pkg.go.dev/github.com/btnguyen2k/prom)
[![Actions Status](https://github.com/btnguyen2k/prom/workflows/ci/badge.svg)](https://github.com/btnguyen2k/prom/actions)
[![codecov](https://codecov.io/gh/btnguyen2k/prom/branch/master/graph/badge.svg)](https://codecov.io/gh/btnguyen2k/prom)
[![Release](https://img.shields.io/github/release/btnguyen2k/prom.svg?style=flat-square)](RELEASE-NOTES.md)

Utility library to manage shared connections in Go.

## Usage

`prom` itself does not provide functionality for direct use. Instead, use its sub-packages/modules:

- ['Prom' for database/sql](./sql/): (maintained as sub-package) help with managing shared `database/sql` connections and handling niche cases with various drivers and database types.

<!--
- ['Prom' for AWS DyamoDB](dynamodb/)
- ['Prom' for the official Go driver for MongoDB](mongo/)
- ['Prom' for go-redis](goredis/)

## Examples

- [AWS DyamoDB](./examples/dynamodb/)
- [MongoDB](./examples/mongo/)
- [Redis](./examples/goredis/)
- [database/sql](./examples/sql/)
-->

## Contributing

Feel free to create [pull requests](https://github.com/btnguyen2k/prom/pulls) or [issues](https://github.com/btnguyen2k/prom/issues) to report bugs or suggest new features. If you find this project useful, please start it.

If you develop a cool sub-package for `prom`, let me know and I will add it to the list above.

## License

MIT - see [LICENSE.md](LICENSE.md).
