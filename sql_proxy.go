package prom

import (
	"context"
	"database/sql"
	"regexp"
	"strings"
)

// DBProxy is a proxy that can be used as replacement for sql.DB.
//
// This proxy overrides some functions from sql.DB and automatically logs the execution metrics.
//
// Available since v0.3.0
type DBProxy struct {
	*sql.DB
	sqlc *SqlConnect
}

// Ping overrides sql.DB.Ping to log execution metrics.
func (dbp *DBProxy) Ping() error {
	return dbp.PingContext(context.Background())
}

// PingContext overrides sql.DB.PingContext to log execution metrics.
func (dbp *DBProxy) PingContext(ctx context.Context) error {
	cmd := dbp.sqlc.NewCmdExecInfo()
	defer func() {
		dbp.sqlc.LogMetrics(MetricsCatAll, cmd)
		dbp.sqlc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName = "ping"
	err := dbp.DB.PingContext(ctx)
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return err
}

// Close overrides sql.DB.Close to log execution metrics.
func (dbp *DBProxy) Close() error {
	cmd := dbp.sqlc.NewCmdExecInfo()
	defer func() {
		dbp.sqlc.LogMetrics(MetricsCatAll, cmd)
		dbp.sqlc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName = "close"
	err := dbp.DB.Close()
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return err
}

// Prepare overrides sql.DB.Prepare to log execution metrics.
func (dbp *DBProxy) Prepare(query string) (*sql.Stmt, error) {
	return dbp.PrepareContext(context.Background(), query)
}

// PrepareContext overrides sql.DB.PrepareContext to log execution metrics.
func (dbp *DBProxy) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	cmd := dbp.sqlc.NewCmdExecInfo()
	defer func() {
		dbp.sqlc.LogMetrics(MetricsCatAll, cmd)
		dbp.sqlc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "prepare", m{"query": query}
	result, err := dbp.DB.PrepareContext(ctx, query)
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return result, err
}

var firstWordRegEx = regexp.MustCompile(`^\s*(\w+)`)
var sqlDMLCmds = m{"INSERT": true, "DELETE": true, "UPDATE": true, "UPSERT": true}
var sqlDDLCmds = m{"ALTER": true, "CREATE": true, "DROP": true}
var sqlDQLCmds = m{"SELECT": true}

// Exec overrides sql.DB.Exec to log execution metrics.
func (dbp *DBProxy) Exec(query string, args ...interface{}) (sql.Result, error) {
	return dbp.ExecContext(context.Background(), query, args...)
}

// ExecContext overrides sql.DB.ExecContext to log execution metrics.
func (dbp *DBProxy) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	cmd := dbp.sqlc.NewCmdExecInfo()
	firstWord := strings.ToUpper(firstWordRegEx.FindString(query))
	defer func() {
		dbp.sqlc.LogMetrics(MetricsCatAll, cmd)
		if v, ok := sqlDMLCmds[firstWord]; ok && v.(bool) {
			dbp.sqlc.LogMetrics(MetricsCatDML, cmd)
		} else if v, ok := sqlDMLCmds[firstWord]; ok && v.(bool) {
			dbp.sqlc.LogMetrics(MetricsCatDDL, cmd)
		} else {
			dbp.sqlc.LogMetrics(MetricsCatOther, cmd)
		}
	}()
	cmd.CmdName, cmd.CmdRequest = firstWord, m{"query": query, "params": args}
	result, err := dbp.DB.ExecContext(ctx, query, args...)
	if err == nil {
		lastInsertId, _ := result.LastInsertId()
		rowsAffected, _ := result.RowsAffected()
		cmd.CmdResponse = m{"lastInsertId": lastInsertId, "rowsAffected": rowsAffected}
	}
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return result, err
}

// Query overrides sql.DB.Query to log execution metrics.
func (dbp *DBProxy) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return dbp.QueryContext(context.Background(), query, args...)
}

// QueryContext overrides sql.DB.QueryContext to log execution metrics.
func (dbp *DBProxy) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	cmd := dbp.sqlc.NewCmdExecInfo()
	firstWord := strings.ToUpper(firstWordRegEx.FindString(query))
	defer func() {
		dbp.sqlc.LogMetrics(MetricsCatAll, cmd)
		if v, ok := sqlDQLCmds[firstWord]; ok && v.(bool) {
			dbp.sqlc.LogMetrics(MetricsCatDQL, cmd)
		} else {
			dbp.sqlc.LogMetrics(MetricsCatOther, cmd)
		}
	}()
	cmd.CmdName, cmd.CmdRequest = firstWord, m{"query": query, "params": args}
	result, err := dbp.DB.QueryContext(ctx, query, args...)
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return result, err
}

// QueryRow overrides sql.DB.QueryRow to log execution metrics.
func (dbp *DBProxy) QueryRow(query string, args ...interface{}) *sql.Row {
	return dbp.QueryRowContext(context.Background(), query, args...)
}

// QueryRowContext overrides sql.DB.QueryRowContext to log execution metrics.
func (dbp *DBProxy) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	cmd := dbp.sqlc.NewCmdExecInfo()
	firstWord := strings.ToUpper(firstWordRegEx.FindString(query))
	defer func() {
		dbp.sqlc.LogMetrics(MetricsCatAll, cmd)
		if v, ok := sqlDQLCmds[firstWord]; ok && v.(bool) {
			dbp.sqlc.LogMetrics(MetricsCatDQL, cmd)
		} else {
			dbp.sqlc.LogMetrics(MetricsCatOther, cmd)
		}
	}()
	cmd.CmdName, cmd.CmdRequest = firstWord, m{"query": query, "params": args}
	result := dbp.DB.QueryRowContext(ctx, query, args...)
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, result.Err())
	return result
}
