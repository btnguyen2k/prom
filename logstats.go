package prom

import (
	"time"
)

// CmdExecInfo captures information around an executing command.
//
// Available since v0.3.0
type CmdExecInfo struct {
	// Id is the command's unique id.
	Id string `json:"id"`

	// BeginTime is the timestamp when the command started execution.
	BeginTime *time.Time `json:"tbegin"`

	// EndTime is the timestamp when the command finished execution.
	EndTime *time.Time `json:"tend"`

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

/*----------------------------------------------------------------------*/

// ILogMetrics defines APIs to log command executions and retrieve metrics.
//
// Available since v0.3.0
type ILogMetrics interface {
	// Put stores a CmdExecInfo instance.
	Put(category string, cmd *CmdExecInfo) error

	// GetN fetches last N CmdExecInfo instances from a category.
	GetN(category string, n int) ([]*CmdExecInfo, error)
}
