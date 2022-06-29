package prom

import (
	"errors"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestCmdExecInfo_EndWithCost(t *testing.T) {
	testName := "TestCmdExecInfo_EndWithCost"
	cmd := &CmdExecInfo{
		Id:        "1",
		BeginTime: time.Now(),
		CmdName:   "dummy",
		Cost:      0,
	}
	successResult, failedResult := "successful", "failed"

	cmd.EndWithCost(12.34, successResult, failedResult, nil)
	if cmd.EndTime.IsZero() {
		t.Fatalf("%s failed: field [EndTime] is zero", testName)
	}
	if cmd.Error != nil {
		t.Fatalf("%s failed: field [Error] is non-nil", testName)
	}
	if cmd.Result != successResult {
		t.Fatalf("%s failed: expected [Result] to be %#v but received %#v", testName, successResult, cmd.Result)
	}
	if cmd.Cost != 12.34 {
		t.Fatalf("%s failed: expected [Cost] to be %#v but received %#v", testName, 12.34, cmd.Cost)
	}

	err := errors.New("dummy")
	cmd.EndWithCost(56.78, successResult, failedResult, err)
	if cmd.EndTime.IsZero() {
		t.Fatalf("%s failed: field [EndTime] is zero", testName)
	}
	if cmd.Error != err {
		t.Fatalf("%s failed: expected [Error] to be %e but received %e", testName, err, cmd.Error)
	}
	if cmd.Result != failedResult {
		t.Fatalf("%s failed: expected [Result] to be %#v but received %#v", testName, failedResult, cmd.Result)
	}
	if cmd.Cost != 12.34+56.78 {
		t.Fatalf("%s failed: expected [Cost] to be %#v but received %#v", testName, 12.34+56.78, cmd.Cost)
	}
}

func TestCmdExecInfo_EndWithCostAsExecutionTime(t *testing.T) {
	testName := "TestCmdExecInfo_EndWithCostAsExecutionTime"
	cmd := &CmdExecInfo{
		Id:        "1",
		BeginTime: time.Now(),
		CmdName:   "dummy",
		Cost:      0,
	}
	rand.Seed(time.Now().UnixNano())
	successResult, failedResult := "successful", "failed"

	time.Sleep(time.Duration(1+rand.Intn(1024)) * time.Millisecond)
	cmd.EndWithCostAsExecutionTime(successResult, failedResult, nil)
	if cmd.EndTime.IsZero() {
		t.Fatalf("%s failed: field [EndTime] is zero", testName)
	}
	if cmd.Error != nil {
		t.Fatalf("%s failed: field [Error] is non-nil", testName)
	}
	if cmd.Result != successResult {
		t.Fatalf("%s failed: expected [Result] to be %#v but received %#v", testName, successResult, cmd.Result)
	}
	if e := cmd.EndTime.Sub(cmd.BeginTime).Microseconds(); e != int64(cmd.Cost) {
		t.Fatalf("%s failed: expected [Cost] to be %#v but received %#v", testName, e, cmd.Cost)
	}

	time.Sleep(time.Duration(1+rand.Intn(1024)) * time.Millisecond)
	err := errors.New("dummy")
	oldEndTime := cmd.EndTime
	cmd.EndWithCostAsExecutionTime(successResult, failedResult, err)
	if cmd.EndTime.IsZero() {
		t.Fatalf("%s failed: field [EndTime] is zero", testName)
	}
	if cmd.Error != err {
		t.Fatalf("%s failed: expected [Error] to be %e but received %e", testName, err, cmd.Error)
	}
	if cmd.Result != failedResult {
		t.Fatalf("%s failed: expected [Result] to be %#v but received %#v", testName, failedResult, cmd.Result)
	}
	if !cmd.EndTime.After(oldEndTime) {
		t.Fatalf("%s failed: [EndTime] not updated", testName)
	}
	if e := cmd.EndTime.Sub(cmd.BeginTime).Microseconds(); e != int64(cmd.Cost) {
		t.Fatalf("%s failed: expected [Cost] to be %#v but received %#v", testName, e, cmd.Cost)
	}
}

func TestNewMemoryStoreMetricsLogger(t *testing.T) {
	testName := "TestNewMemoryStoreMetricsLogger"
	logger := NewMemoryStoreMetricsLogger(1337)
	if logger == nil {
		t.Fatalf("%s failed: nil", testName)
	}
}

func TestMemoryStoreMetricsLogger_Capacity(t *testing.T) {
	testName := "TestMemoryStoreMetricsLogger_Capacity"
	rand.Seed(time.Now().UnixNano())
	capacity := 1 + rand.Intn(1024)
	logger := &MemoryStoreMetricsLogger{capacity: capacity}
	if logger.Capacity() != capacity {
		t.Fatalf("%s failed: expected capacity to be %#v but received %#v", testName, capacity, logger.Capacity())
	}
}

func TestMemoryStoreMetricsLogger_Put(t *testing.T) {
	testName := "TestMemoryStoreMetricsLogger_Put"
	capacity := 10
	logger := &MemoryStoreMetricsLogger{capacity: capacity}
	for i := 0; i < capacity+3; i++ {
		if err := logger.Put("*", &CmdExecInfo{Id: strconv.Itoa(i + 1), Cost: float64(i + 1)}); err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		}
		if l, e := logger.getStore("*").size(), int(math.Min(float64(i+1), float64(capacity))); l != e {
			t.Fatalf("%s failed: expected size to be %#v but received %#v", testName, e, l)
		}
	}
}

func TestMemoryStoreMetricsLogger_Metrics(t *testing.T) {
	testName := "TestMemoryStoreMetricsLogger_Metrics"
	capacity := 10
	logger := &MemoryStoreMetricsLogger{capacity: capacity}
	for i := 0; i < capacity+3; i++ {
		logger.Put("*", &CmdExecInfo{Id: strconv.Itoa(i + 1), Cost: float64(i + 1)})
		m, err := logger.Metrics("*", MetricsOpts{ReturnLatestCommands: capacity + 10})
		if err != nil {
			t.Fatalf("%s failed: %s", testName, err)
		}
		if m.Category != "*" {
			t.Fatalf("%s failed: expected Category to be %#v but received %#v", testName, "*", m.Category)
		}
		if m.TotalNumCmds != int64(i+1) {
			t.Fatalf("%s failed: expected TotalNumCmds to be %#v but received %#v", testName, i+1, m.TotalNumCmds)
		}
		if v, e := m.ReservoirNumCmds, int64(math.Min(float64(i+1), float64(capacity))); v != e {
			t.Fatalf("%s failed: expected ReservoirNumCmds to be %#v but received %#v", testName, e, v)
		}
		if v, e := len(m.LastNCmds), int(math.Min(float64(i+1), float64(capacity))); v != e {
			t.Fatalf("%s failed: expected len(LastNCmds) to be %#v but received %#v", testName, e, v)
		}
		for j, cmd := range m.LastNCmds {
			if v, e := cmd.Id, strconv.Itoa(i+1-j); v != e {
				t.Fatalf("%s failed: expected LastNCmds[%d] to be %#v but received %#v", testName, j, e, v)
			}
		}
	}
}
