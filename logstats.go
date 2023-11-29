package prom

import (
	"errors"
	"sync"
	"time"

	"github.com/rcrowley/go-metrics"
)

const (
	CmdResultOk    = "OK"
	CmdResultError = "ERROR"
)

const (
	// MetricsCatAll is a common metrics category for "all commands".
	MetricsCatAll = "all"

	// MetricsCatDDL is a common metrics category for "DDL commands".
	MetricsCatDDL = "ddl"

	// MetricsCatDML is a common metrics category for "DML commands".
	MetricsCatDML = "dml"

	// MetricsCatDQL is a common metrics category for "DQL commands".
	MetricsCatDQL = "dql"

	// MetricsCatOther is a common metrics category for "other commands".
	MetricsCatOther = "other"
)

// CmdExecInfo captures information around an executing command.
//
// Available since v0.3.0
type CmdExecInfo struct {
	// Id is the command's unique id.
	Id string `json:"id"`

	// BeginTime is the timestamp when the command started execution.
	BeginTime time.Time `json:"tbegin"`

	// EndTime is the timestamp when the command finished execution.
	EndTime time.Time `json:"tend"`

	// CmdName is name of the command.
	CmdName string `json:"cname"`

	// CmdRequest captures the command's arguments/input.
	CmdRequest interface{} `json:"creq"`

	// CmdResponse captures the command's output.
	CmdResponse interface{} `json:"cres"`

	// CmdMeta captures other metadata of the command.
	CmdMeta interface{} `json:"meta"`

	// Result is the output status upon command execution.
	Result interface{} `json:"result"`

	// Cost is the associated execution cost.
	Cost float64 `json:"cost"`

	// Error captures the execution error, if any.
	Error error `json:"error"`
}

// EndWithCostAsExecutionTime is convenient function to "close" the command execution and calculate the execution cost as the total microseconds taken.
func (cmd *CmdExecInfo) EndWithCostAsExecutionTime(successResult, failedResult interface{}, err error) {
	cmd.EndTime = time.Now()
	cmd.Cost = float64(cmd.EndTime.Sub(cmd.BeginTime).Microseconds())
	if err != nil {
		cmd.Result = failedResult
		cmd.Error = err
	} else {
		cmd.Result = successResult
	}
}

// EndWithCost is convenient function to "close" the command execution.
func (cmd *CmdExecInfo) EndWithCost(cost float64, successResult, failedResult interface{}, err error) {
	cmd.EndTime = time.Now()
	if cmd.Cost < 0 {
		cmd.Cost = 0.0
	}
	cmd.Cost += cost
	if err != nil {
		cmd.Result = failedResult
		cmd.Error = err
	} else {
		cmd.Result = successResult
	}
}

// Metrics is the snapshot of command execution metrics.
//
// Available since v0.3.0
type Metrics struct {
	// Name of the category the metrics belong to.
	Category string `json:"cat"`

	// Total number of executed commands since the last reset.
	TotalNumCmds int64 `json:"total"`

	// It might not be practical to store all executed commands to calculate metrics with 100% accuracy. Most of the
	// time, stats of latest executed command is more important. This is the number of last executed commands used
	// to calculate the metrics.
	ReservoirNumCmds int64 `json:"window"`

	// The statistics minimum value of command execution cost.
	MinCost float64 `json:"min"`

	// The statistics maximum value of command execution cost.
	MaxCost float64 `json:"max"`

	// The statistics mean value of command execution cost.
	MeanCost float64 `json:"avg"`

	// The statistics p99 value of command execution cost.
	P99Cost float64 `json:"p99"`

	// The statistics p95 value of command execution cost.
	P95Cost float64 `json:"p95"`

	// The statistics p90 value of command execution cost.
	P90Cost float64 `json:"p90"`

	// The statistics p75 value of command execution cost.
	P75Cost float64 `json:"p75"`

	// The statistics p50 value of command execution cost.
	P50Cost float64 `json:"p50"`

	// Last N executed commands.
	LastNCmds []*CmdExecInfo `json:"last"`
}

// MetricsOpts is argument used by function IMetricsLogger.Metrics.
//
// Available since v0.3.0
type MetricsOpts struct {
	// This option specifies the number of last commands to be returned along with the metrics.
	ReturnLatestCommands int
}

/*----------------------------------------------------------------------*/

// IMetricsLogger defines APIs to log command executions and retrieve metrics.
//
// Available since v0.3.0
type IMetricsLogger interface {
	// Put stores a CmdExecInfo instance.
	Put(category string, cmd *CmdExecInfo) error

	// Metrics returns the snapshot of command execution metrics.
	Metrics(category string, opts ...MetricsOpts) (*Metrics, error)
}

// NewMemoryStoreMetricsLogger creates a new MemoryStoreMetricsLogger instance.
//   - capacity: max number of items MemoryStoreMetricsLogger can hold.
//
// Available since v0.3.0
func NewMemoryStoreMetricsLogger(capacity int) IMetricsLogger {
	return &MemoryStoreMetricsLogger{capacity: capacity}
}

// MemoryStoreMetricsLogger is an in-memory bound storage implementation of IMetricsLogger.
//
// Available since v0.3.0
type MemoryStoreMetricsLogger struct {
	capacity int
	lock     sync.Mutex
	storage  map[string]*boundMemoryStackStore
}

// Capacity returns max number of items can be hold by this logger.
func (logger *MemoryStoreMetricsLogger) Capacity() int {
	return logger.capacity
}

// Put implements IMetricsLogger.Put
func (logger *MemoryStoreMetricsLogger) Put(category string, cmd *CmdExecInfo) error {
	return logger.getStore(category).put(cmd)
}

// Metrics implements IMetricsLogger.Metrics
func (logger *MemoryStoreMetricsLogger) Metrics(category string, opts ...MetricsOpts) (*Metrics, error) {
	s := logger.getStore(category)
	s.lock.Lock()
	defer s.lock.Unlock()
	h := s.histogramSnapshot()
	m := &Metrics{
		Category:         category,
		TotalNumCmds:     h.Count(),
		ReservoirNumCmds: int64(s.size()),
		MinCost:          float64(h.Min()),
		MaxCost:          float64(h.Max()),
		MeanCost:         h.Mean(),
		P99Cost:          h.Percentile(0.99),
		P95Cost:          h.Percentile(0.95),
		P90Cost:          h.Percentile(0.90),
		P75Cost:          h.Percentile(0.75),
		P50Cost:          h.Percentile(0.50),
	}
	if len(opts) > 0 && opts[0].ReturnLatestCommands > 0 && len(s.store) > 0 {
		n, l := opts[0].ReturnLatestCommands, len(s.store)
		if n > l {
			n = l
		}
		m.LastNCmds = make([]*CmdExecInfo, n)
		copy(m.LastNCmds, s.store[l-n:l])
		reserve(m.LastNCmds)
	}
	return m, nil
}

func (logger *MemoryStoreMetricsLogger) getStore(category string) *boundMemoryStackStore {
	logger.lock.Lock()
	defer logger.lock.Unlock()
	if logger.storage == nil {
		logger.storage = make(map[string]*boundMemoryStackStore)
	}
	store := logger.storage[category]
	if store == nil {
		store = newBoundMemoryStackStore(logger.capacity)
		logger.storage[category] = store
	}
	return store
}

/*----------------------------------------------------------------------*/

func reserve(input []*CmdExecInfo) {
	const batchSize = 100
	inputMid := len(input) / 2
	var wg sync.WaitGroup
	for i := 0; i < inputMid; i += batchSize {
		wg.Add(1)
		j := i + batchSize
		if j > inputMid {
			j = inputMid
		}
		go reverseBatch(input, i, j, &wg)
	}
	wg.Wait()
}

func reverseBatch(input []*CmdExecInfo, start, end int, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := start; i < end; i++ {
		j := len(input) - i - 1
		input[i], input[j] = input[j], input[i]
	}
}

func newBoundMemoryStackStore(capacity int) *boundMemoryStackStore {
	return &boundMemoryStackStore{
		store:     make([]*CmdExecInfo, 0, capacity),
		histogram: metrics.NewHistogram(metrics.NewExpDecaySample(capacity, 0.015)),
	}
}

type boundMemoryStackStore struct {
	histogram metrics.Histogram
	store     []*CmdExecInfo
	lock      sync.Mutex
}

func (s *boundMemoryStackStore) put(item *CmdExecInfo) error {
	if item == nil {
		return errors.New("nil input")
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	c := cap(s.store)
	l := len(s.store)
	if l >= c {
		copy(s.store, s.store[1:])
		l--
	}
	s.store = s.store[0 : l+1]
	s.store[l] = item
	s.histogram.Update(int64(item.Cost))
	return nil
}

func (s *boundMemoryStackStore) size() int {
	return len(s.store)
}

func (s *boundMemoryStackStore) histogramSnapshot() metrics.Histogram {
	return s.histogram.Snapshot()
}
