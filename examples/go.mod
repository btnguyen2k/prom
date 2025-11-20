module main

go 1.24.0

require (
	github.com/btnguyen2k/prom v0.4.1
	github.com/jackc/pgx/v5 v5.5.4
)

replace github.com/btnguyen2k/prom => ../

require (
	github.com/btnguyen2k/consu/olaf v0.1.3 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	golang.org/x/crypto v0.45.0 // indirect
	golang.org/x/sync v0.18.0 // indirect
	golang.org/x/text v0.31.0 // indirect
)
