package prom

import "testing"

func TestBaseConnection_GetSetPoolOpts(t *testing.T) {
	testName := "TestBaseConnection_GetSetPoolOpts"
	conn := BaseConnection{}

	poolOpts := &BasePoolOpts{}
	conn.SetPoolOpts(poolOpts)
	if conn.PoolOpts() != poolOpts {
		t.Fatalf("%s failed: poolOpts mismatched", testName)
	}
	conn.SetPoolOpts(nil)
	if conn.PoolOpts() != nil {
		t.Fatalf("%s failed: expect sqlPoolOptions to be nil", testName)
	}
}

func TestBaseConnection_GetSetMetricsLogger(t *testing.T) {
	testName := "TestBaseConnection_GetSetMetricsLogger"
	conn := BaseConnection{}

	metricsLogger := NewMemoryStoreMetricsLogger(1234)
	if conn.MetricsLogger() == metricsLogger {
		t.Fatalf("%s failed: expect a different metricsLogger", testName)
	}
	conn.RegisterMetricsLogger(metricsLogger)
	if conn.MetricsLogger() != metricsLogger {
		t.Fatalf("%s failed: expect metricsLogger to be set correctly", testName)
	}
}

func TestBaseConnection_NewCmdExecInfo(t *testing.T) {
	testName := "TestBaseConnection_NewCmdExecInfo"
	conn := BaseConnection{}
	cmd := conn.NewCmdExecInfo()
	if cmd == nil {
		t.Fatalf("%s failed: nil", testName)
	}
}

func TestBaseConnection_Metrics(t *testing.T) {
	testName := "TestBaseConnection_Metrics"
	conn := BaseConnection{}
	conn.RegisterMetricsLogger(NewMemoryStoreMetricsLogger(1234))

	cmd := conn.NewCmdExecInfo()
	cmd.CmdName, cmd.CmdRequest = "SELECT", map[string]interface{}{"query": "SELECT * FROM table_name", "params": 1.2}
	cmd.EndWithCost(12.34, "success", "failed", nil)
	if err := conn.LogMetrics("test", cmd); err != nil {
		t.Fatalf("%s failed: %s", testName+"/LogMetrics", err)
	}

	metrics, err := conn.Metrics("test", MetricsOpts{ReturnLatestCommands: 1})
	if err != nil {
		t.Fatalf("%s failed: %s", testName+"/Metrics", err)
	}
	if metrics == nil {
		t.Fatalf("%s failed: nil", testName+"/Metrics")
	}
	if len(metrics.LastNCmds) != 1 {
		t.Fatalf("%s failed: expected %#v metrics returned, but received %#v", testName+"/Metrics", 1, len(metrics.LastNCmds))
	}
}
