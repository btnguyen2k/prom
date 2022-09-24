package prom

import (
	"context"
	"database/sql"
	"regexp"
	"strings"
)

var firstWordRegEx = regexp.MustCompile(`^\s*(\w+)`)
var sqlDMLCmds = m{"INSERT": true, "DELETE": true, "UPDATE": true, "UPSERT": true}
var sqlDDLCmds = m{"ALTER": true, "CREATE": true, "DROP": true}
var sqlDQLCmds = m{"SELECT": true}

// DBProxy is a proxy that can be used as replacement for sql.DB.
//
// This proxy overrides some functions from sql.DB and automatically logs the execution metrics.
//
// Available since v0.3.0
type DBProxy struct {
	*sql.DB
	sqlc *SqlConnect
}

// BeginProxy is similar to sql.DB.Begin, but returns a proxy that can be used as a replacement.
//
// See TxProxy.
func (dbp *DBProxy) BeginProxy() (*TxProxy, error) {
	tx, err := dbp.DB.Begin()
	return &TxProxy{Tx: tx, sqlc: dbp.sqlc}, err
}

// BeginTxProxy is similar to sql.DB.BeginTx, but returns a proxy that can be used as a replacement.
//
// See TxProxy.
func (dbp *DBProxy) BeginTxProxy(ctx context.Context, opts *sql.TxOptions) (*TxProxy, error) {
	tx, err := dbp.DB.BeginTx(ctx, opts)
	return &TxProxy{Tx: tx, sqlc: dbp.sqlc}, err
}

// ConnProxy is similar to sql.DB.Conn, but returns a proxy that can be used as a replacement.
//
// See ConnProxy.
func (dbp *DBProxy) ConnProxy(ctx context.Context) (*ConnProxy, error) {
	conn, err := dbp.DB.Conn(ctx)
	return &ConnProxy{Conn: conn, sqlc: dbp.sqlc}, err
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

/*----------------------------------------------------------------------*/

// ConnProxy is a proxy that can be used as replacement for sql.Conn.
//
// This proxy overrides some functions from sql.Conn and automatically logs the execution metrics.
//
// Available since v0.3.0
type ConnProxy struct {
	*sql.Conn
	sqlc *SqlConnect
}

// BeginTxProxy is similar to sql.Conn.BeginTx, but returns a proxy that can be used as a replacement.
//
// See TxProxy.
func (cp *ConnProxy) BeginTxProxy(ctx context.Context, opts *sql.TxOptions) (*TxProxy, error) {
	tx, err := cp.Conn.BeginTx(ctx, opts)
	return &TxProxy{Tx: tx, sqlc: cp.sqlc}, err
}

// PingContext overrides sql.Conn.PingContext to log execution metrics.
func (cp *ConnProxy) PingContext(ctx context.Context) error {
	cmd := cp.sqlc.NewCmdExecInfo()
	defer func() {
		cp.sqlc.LogMetrics(MetricsCatAll, cmd)
		cp.sqlc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName = "ping"
	err := cp.Conn.PingContext(ctx)
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return err
}

// Close overrides sql.Conn.Close to log execution metrics.
func (cp *ConnProxy) Close() error {
	cmd := cp.sqlc.NewCmdExecInfo()
	defer func() {
		cp.sqlc.LogMetrics(MetricsCatAll, cmd)
		cp.sqlc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName = "close"
	err := cp.Conn.Close()
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return err
}

// PrepareContext overrides sql.Conn.PrepareContext to log execution metrics.
func (cp *ConnProxy) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	cmd := cp.sqlc.NewCmdExecInfo()
	defer func() {
		cp.sqlc.LogMetrics(MetricsCatAll, cmd)
		cp.sqlc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "prepare", m{"query": query}
	result, err := cp.Conn.PrepareContext(ctx, query)
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return result, err
}

// ExecContext overrides sql.Conn.ExecContext to log execution metrics.
func (cp *ConnProxy) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	cmd := cp.sqlc.NewCmdExecInfo()
	firstWord := strings.ToUpper(firstWordRegEx.FindString(query))
	defer func() {
		cp.sqlc.LogMetrics(MetricsCatAll, cmd)
		if v, ok := sqlDMLCmds[firstWord]; ok && v.(bool) {
			cp.sqlc.LogMetrics(MetricsCatDML, cmd)
		} else if v, ok := sqlDMLCmds[firstWord]; ok && v.(bool) {
			cp.sqlc.LogMetrics(MetricsCatDDL, cmd)
		} else {
			cp.sqlc.LogMetrics(MetricsCatOther, cmd)
		}
	}()
	cmd.CmdName, cmd.CmdRequest = firstWord, m{"query": query, "params": args}
	result, err := cp.Conn.ExecContext(ctx, query, args...)
	if err == nil {
		lastInsertId, _ := result.LastInsertId()
		rowsAffected, _ := result.RowsAffected()
		cmd.CmdResponse = m{"lastInsertId": lastInsertId, "rowsAffected": rowsAffected}
	}
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return result, err
}

// QueryContext overrides sql.Conn.QueryContext to log execution metrics.
func (cp *ConnProxy) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	cmd := cp.sqlc.NewCmdExecInfo()
	firstWord := strings.ToUpper(firstWordRegEx.FindString(query))
	defer func() {
		cp.sqlc.LogMetrics(MetricsCatAll, cmd)
		if v, ok := sqlDQLCmds[firstWord]; ok && v.(bool) {
			cp.sqlc.LogMetrics(MetricsCatDQL, cmd)
		} else {
			cp.sqlc.LogMetrics(MetricsCatOther, cmd)
		}
	}()
	cmd.CmdName, cmd.CmdRequest = firstWord, m{"query": query, "params": args}
	result, err := cp.Conn.QueryContext(ctx, query, args...)
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return result, err
}

// QueryRowContext overrides sql.Conn.QueryRowContext to log execution metrics.
func (cp *ConnProxy) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	cmd := cp.sqlc.NewCmdExecInfo()
	firstWord := strings.ToUpper(firstWordRegEx.FindString(query))
	defer func() {
		cp.sqlc.LogMetrics(MetricsCatAll, cmd)
		if v, ok := sqlDQLCmds[firstWord]; ok && v.(bool) {
			cp.sqlc.LogMetrics(MetricsCatDQL, cmd)
		} else {
			cp.sqlc.LogMetrics(MetricsCatOther, cmd)
		}
	}()
	cmd.CmdName, cmd.CmdRequest = firstWord, m{"query": query, "params": args}
	result := cp.Conn.QueryRowContext(ctx, query, args...)
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, result.Err())
	return result
}

/*----------------------------------------------------------------------*/

// TxProxy is a proxy that can be used as replacement for sql.Tx.
//
// This proxy overrides some functions from sql.Tx and automatically logs the execution metrics.
//
// Available since v0.3.0
type TxProxy struct {
	*sql.Tx
	sqlc *SqlConnect
}

// Commit overrides sql.Tx.Commit to log execution metrics.
func (tp *TxProxy) Commit() error {
	cmd := tp.sqlc.NewCmdExecInfo()
	defer func() {
		tp.sqlc.LogMetrics(MetricsCatAll, cmd)
		tp.sqlc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName = "commit"
	err := tp.Tx.Commit()
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return err
}

// Rollback overrides sql.Tx.Rollback to log execution metrics.
func (tp *TxProxy) Rollback() error {
	cmd := tp.sqlc.NewCmdExecInfo()
	defer func() {
		tp.sqlc.LogMetrics(MetricsCatAll, cmd)
		tp.sqlc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName = "rollback"
	err := tp.Tx.Rollback()
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return err
}

// Prepare overrides sql.Tx.Prepare to log execution metrics.
func (tp *TxProxy) Prepare(query string) (*sql.Stmt, error) {
	return tp.PrepareContext(context.Background(), query)
}

// PrepareContext overrides sql.Tx.PrepareContext to log execution metrics.
func (tp *TxProxy) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	cmd := tp.sqlc.NewCmdExecInfo()
	defer func() {
		tp.sqlc.LogMetrics(MetricsCatAll, cmd)
		tp.sqlc.LogMetrics(MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "prepare", m{"query": query}
	result, err := tp.Tx.PrepareContext(ctx, query)
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return result, err
}

// Exec overrides sql.Tx.Exec to log execution metrics.
func (tp *TxProxy) Exec(query string, args ...interface{}) (sql.Result, error) {
	return tp.ExecContext(context.Background(), query, args...)
}

// ExecContext overrides sql.Tx.ExecContext to log execution metrics.
func (tp *TxProxy) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	cmd := tp.sqlc.NewCmdExecInfo()
	firstWord := strings.ToUpper(firstWordRegEx.FindString(query))
	defer func() {
		tp.sqlc.LogMetrics(MetricsCatAll, cmd)
		if v, ok := sqlDMLCmds[firstWord]; ok && v.(bool) {
			tp.sqlc.LogMetrics(MetricsCatDML, cmd)
		} else if v, ok := sqlDMLCmds[firstWord]; ok && v.(bool) {
			tp.sqlc.LogMetrics(MetricsCatDDL, cmd)
		} else {
			tp.sqlc.LogMetrics(MetricsCatOther, cmd)
		}
	}()
	cmd.CmdName, cmd.CmdRequest = firstWord, m{"query": query, "params": args}
	result, err := tp.Tx.ExecContext(ctx, query, args...)
	if err == nil {
		lastInsertId, _ := result.LastInsertId()
		rowsAffected, _ := result.RowsAffected()
		cmd.CmdResponse = m{"lastInsertId": lastInsertId, "rowsAffected": rowsAffected}
	}
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return result, err
}

// Query overrides sql.Tx.Query to log execution metrics.
func (tp *TxProxy) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return tp.QueryContext(context.Background(), query, args...)
}

// QueryContext overrides sql.Tx.QueryContext to log execution metrics.
func (tp *TxProxy) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	cmd := tp.sqlc.NewCmdExecInfo()
	firstWord := strings.ToUpper(firstWordRegEx.FindString(query))
	defer func() {
		tp.sqlc.LogMetrics(MetricsCatAll, cmd)
		if v, ok := sqlDQLCmds[firstWord]; ok && v.(bool) {
			tp.sqlc.LogMetrics(MetricsCatDQL, cmd)
		} else {
			tp.sqlc.LogMetrics(MetricsCatOther, cmd)
		}
	}()
	cmd.CmdName, cmd.CmdRequest = firstWord, m{"query": query, "params": args}
	result, err := tp.Tx.QueryContext(ctx, query, args...)
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, err)
	return result, err
}

// QueryRow overrides sql.Tx.QueryRow to log execution metrics.
func (tp *TxProxy) QueryRow(query string, args ...interface{}) *sql.Row {
	return tp.QueryRowContext(context.Background(), query, args...)
}

// QueryRowContext overrides sql.Tx.QueryRowContext to log execution metrics.
func (tp *TxProxy) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	cmd := tp.sqlc.NewCmdExecInfo()
	firstWord := strings.ToUpper(firstWordRegEx.FindString(query))
	defer func() {
		tp.sqlc.LogMetrics(MetricsCatAll, cmd)
		if v, ok := sqlDQLCmds[firstWord]; ok && v.(bool) {
			tp.sqlc.LogMetrics(MetricsCatDQL, cmd)
		} else {
			tp.sqlc.LogMetrics(MetricsCatOther, cmd)
		}
	}()
	cmd.CmdName, cmd.CmdRequest = firstWord, m{"query": query, "params": args}
	result := tp.Tx.QueryRowContext(ctx, query, args...)
	cmd.EndWithCostAsExecutionTime(CmdResultOk, CmdResultError, result.Err())
	return result
}
