module sql_test

go 1.21

toolchain go1.21.4

require (
	github.com/btnguyen2k/gocosmos v0.3.0
	github.com/btnguyen2k/prom v0.4.1
	github.com/denisenkom/go-mssqldb v0.12.3
	github.com/go-sql-driver/mysql v1.7.1
	github.com/godror/godror v0.40.4
	github.com/jackc/pgx/v4 v4.18.1
	github.com/mattn/go-sqlite3 v1.14.18
)

replace github.com/btnguyen2k/prom => ../

replace github.com/btnguyen2k/prom/sql => ../sql/

require (
	github.com/btnguyen2k/consu/checksum v0.1.2 // indirect
	github.com/btnguyen2k/consu/gjrc v0.1.1 // indirect
	github.com/btnguyen2k/consu/olaf v0.1.3 // indirect
	github.com/btnguyen2k/consu/reddo v0.1.7 // indirect
	github.com/btnguyen2k/consu/semita v0.1.5 // indirect
	github.com/go-logfmt/logfmt v0.6.0 // indirect
	github.com/godror/knownpb v0.1.1 // indirect
	github.com/golang-sql/civil v0.0.0-20190719163853-cb61b32ac6fe // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.14.0 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.3.2 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/pgtype v1.14.0 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	golang.org/x/crypto v0.6.0 // indirect
	golang.org/x/exp v0.0.0-20230905200255-921286631fa9 // indirect
	golang.org/x/text v0.7.0 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
)
