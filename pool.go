package prom

import "time"

// IBasePoolOpts is the base interface to define configurations for a connection pool.
//
// @Available since <<VERSION>>
type IBasePoolOpts interface {
}

// BasePoolOpts is an abstract implementation of IBasePoolOpts.
//
// @Available since <<VERSION>>
type BasePoolOpts struct {
	// Maximum number of connections.
	// Set to zero or negative value to use default value.
	MaxPoolSize int `json:"max_size"`

	// Minimum number of idle connections. Default value is 1.
	MinPoolSize int `json:"min_size"`

	// Maximum amount of time a connection may be reused.
	// Set to zero or negative value to use default value.
	ConnLifetime time.Duration `json:"conn_lifetime"`
}

// IBaseConnection is the base interface to define a connection.
//
// @Available since <<VERSION>>
type IBaseConnection interface {
	// PoolOpts returns the pooling options attached to this connection.
	PoolOpts() IBasePoolOpts

	// MetricsLogger returns the metrics logger attached to this connection.
	MetricsLogger() IMetricsLogger

	// RegisterMetricsLogger attaches a metrics logger to this connection.
	// This function returns the connection itself for chaining.
	RegisterMetricsLogger(logger IMetricsLogger) IBaseConnection

	// NewCmdExecInfo is the convenient function to create a new CmdExecInfo instance.
	// The returned CmdExecInfo should have  its 'id' and 'begin-time' fields initialized.
	NewCmdExecInfo() *CmdExecInfo

	// LogMetrics is the convenient function to put the CmdExecInfo to the metrics log.
	// This function is silently no-op if the input iis nil or there is no associated metrics logger.
	LogMetrics(category string, cmd *CmdExecInfo) error

	// Metrics is the convenient function to capture the snapshot of command execution metrics.
	// This function is silently no-op if there is no associated metrics logger.
	Metrics(category string, opts ...MetricsOpts) (*Metrics, error)
}

// BaseConnection is an abstract implementation of IBaseConnection.
//
// @Available since <<VERSION>>
type BaseConnection struct {
	poolOpts      IBasePoolOpts
	metricsLogger IMetricsLogger
}

// PoolOpts implements IBaseConnection.PoolOpts.
func (c *BaseConnection) PoolOpts() IBasePoolOpts {
	return c.poolOpts
}

// MetricsLogger implements IBaseConnection.MetricsLogger.
func (c *BaseConnection) MetricsLogger() IMetricsLogger {
	return c.metricsLogger
}

// RegisterMetricsLogger implements IBaseConnection.RegisterMetricsLogger.
func (c *BaseConnection) RegisterMetricsLogger(logger IMetricsLogger) IBaseConnection {
	c.metricsLogger = logger
	return c
}

// NewCmdExecInfo implements IBaseConnection.NewCmdExecInfo.
func (c *BaseConnection) NewCmdExecInfo() *CmdExecInfo {
	return &CmdExecInfo{
		Id:        NewId(),
		BeginTime: time.Now(),
		Cost:      -1,
	}
}

// LogMetrics implements IBaseConnection.LogMetrics.
func (c *BaseConnection) LogMetrics(category string, cmd *CmdExecInfo) error {
	if cmd != nil && c.metricsLogger != nil {
		return c.metricsLogger.Put(category, cmd)
	}
	return nil
}

// Metrics implements IBaseConnection.Metrics.
func (c *BaseConnection) Metrics(category string, opts ...MetricsOpts) (*Metrics, error) {
	if c.metricsLogger != nil {
		return c.metricsLogger.Metrics(category, opts...)
	}
	return nil, nil
}
