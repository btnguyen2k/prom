package prom

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

func _rcVerifyLastCommand(f _testFailedWithMsgFunc, testName string, rc *GoRedisConnect, cmdName string, ignoredErrs []error, cmdCats ...string) {
	for _, cat := range cmdCats {
		m, err := rc.Metrics(cat, MetricsOpts{ReturnLatestCommands: 1})
		if err != nil {
			f(fmt.Sprintf("%s failed: error [%e]", testName+"/Metrics("+cat+")", err))
		}
		if m == nil {
			f(fmt.Sprintf("%s failed: cannot obtain metrics info", testName+"/Metrics("+cat+")"))
		}
		if e, v := 1, len(m.LastNCmds); e != v {
			f(fmt.Sprintf("%s failed: expected %v last command returned but received %v", testName+"/Metrics("+cat+")", e, v))
		}
		cmd := m.LastNCmds[0]
		// cmd.CmdRequest, cmd.CmdResponse, cmd.CmdMeta = nil, nil, nil
		if cmd.Error != nil {
			for _, err := range ignoredErrs {
				if err == cmd.Error {
					return
				}
			}
		}
		if cmd.CmdName != cmdName || cmd.Result != CmdResultOk || cmd.Error != nil || cmd.Cost < 0 {
			f(fmt.Sprintf("%s failed: invalid last command metrics.\nExpected: [Name=%v / Result=%v / Error = %e / Cost = %v]\nReceived: [Name=%v / Result=%v / Error = %s / Cost = %v]",
				testName+"/Metrics("+cat+")",
				cmdName, CmdResultOk, error(nil), ">= 0",
				cmd.CmdName, cmd.Result, cmd.Error, cmd.Cost))
		}
	}
}

func _newGoRedisConnectForRedisClient(t *testing.T, testName string) *GoRedisConnect {
	hostsAndPorts := os.Getenv("REDIS_HOSTS_AND_PORTS")
	hostsAndPorts = strings.ReplaceAll(hostsAndPorts, `"`, "")
	if hostsAndPorts == "" {
		return nil
	}

	// user := os.Getenv("REDIS_USER")
	// user = strings.ReplaceAll(user, `"`, "")

	password := os.Getenv("REDIS_PASSWORD")
	password = strings.ReplaceAll(password, `"`, "")

	rc, err := NewGoRedisConnect(hostsAndPorts, password, 0)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	return rc
}

func _newGoRedisConnectForRedisFailoverClient(t *testing.T, testName string) *GoRedisConnect {
	hostsAndPorts := os.Getenv("REDIS_FAILOVER_HOSTS_AND_PORTS")
	hostsAndPorts = strings.ReplaceAll(hostsAndPorts, `"`, "")
	if hostsAndPorts == "" {
		return nil
	}

	// user := os.Getenv("REDIS_FAILOVER_USER")
	// user = strings.ReplaceAll(user, `"`, "")

	password := os.Getenv("REDIS_FAILOVER_PASSWORD")
	password = strings.ReplaceAll(password, `"`, "")

	rc, err := NewGoRedisConnect(hostsAndPorts, password, 0)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}

	masterName := os.Getenv("REDIS_FAILOVER_MASTER_NAME")
	masterName = strings.ReplaceAll(masterName, `"`, "")
	rc.SetSentinelMasterName(masterName)

	return rc
}

func _newGoRedisConnectForRedisClusterClient(t *testing.T, testName string) *GoRedisConnect {
	hostsAndPorts := os.Getenv("REDIS_CLUSTER_HOSTS_AND_PORTS")
	hostsAndPorts = strings.ReplaceAll(hostsAndPorts, `"`, "")
	if hostsAndPorts == "" {
		return nil
	}

	// user := os.Getenv("REDIS_CLUSTER_USER")
	// user = strings.ReplaceAll(user, `"`, "")

	password := os.Getenv("REDIS_CLUSTER_PASSWORD")
	password = strings.ReplaceAll(password, `"`, "")

	rc, err := NewGoRedisConnect(hostsAndPorts, password, 0)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	return rc
}

var _testList = []string{"Normal", "Failover", "Cluster"}
var _testRcList []*GoRedisConnect
var _testCmdableList []redis.Cmdable

var _setupTestRedisProxy _testSetupOrTeardownFunc = func(t *testing.T, testName string) {
	_testRcList = make([]*GoRedisConnect, len(_testList))
	_testCmdableList = make([]redis.Cmdable, len(_testList))
	for i, testItem := range _testList {
		if strings.ToUpper(testItem) == "FAILOVER" {
			_testRcList[i] = _newGoRedisConnectForRedisFailoverClient(t, testName)
			if _testRcList[i] != nil {
				_testCmdableList[i] = _testRcList[i].GetFailoverClientProxy(0)
				_testRcList[i].GetFailoverClient(0).FlushAll(context.TODO())
			}
		} else if strings.ToUpper(testItem) == "CLUSTER" {
			_testRcList[i] = _newGoRedisConnectForRedisClusterClient(t, testName)
			if _testRcList[i] != nil {
				_testCmdableList[i] = _testRcList[i].GetClusterClientProxy()
				for _, hostAndPort := range _testRcList[i].hostsAndPorts {
					client := _testRcList[i].newClientWithHostAndPort(hostAndPort, 0)
					client.FlushAll(context.TODO())
				}
			}
		} else {
			_testRcList[i] = _newGoRedisConnectForRedisClient(t, testName)
			if _testRcList[i] != nil {
				_testCmdableList[i] = _testRcList[i].GetClientProxy(0)
				_testRcList[i].GetClient(0).FlushAll(context.TODO())
			}
		}
	}
}

var _teardownTestRedisProxy _testSetupOrTeardownFunc = func(t *testing.T, testName string) {
	for _, rc := range _testRcList {
		if rc != nil {
			go rc.Close()
		}
	}
}

/* Redis' bitmap-related commands */

func TestRedisProxy_BitCount(t *testing.T) {
	testName := "TestRedisProxy_BitCount"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.BitCount(context.TODO(), "key", &redis.BitCount{})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "bitCount", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_BitField(t *testing.T) {
	testName := "TestRedisProxy_BitField"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.BitField(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "bitField", nil, MetricsCatAll, MetricsCatOther)
		})
	}
}

func TestRedisProxy_BitOpAnd(t *testing.T) {
	testName := "TestRedisProxy_BitOpAnd"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.BitOpAnd(context.TODO(), "dest{key}", "{key}1", "{key}2")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "bitOp", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_BitOpNot(t *testing.T) {
	testName := "TestRedisProxy_BitOpNot"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.BitOpNot(context.TODO(), "dest{key}", "{key}")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "bitOp", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_BitOpOr(t *testing.T) {
	testName := "TestRedisProxy_BitOpOr"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.BitOpOr(context.TODO(), "dest{key}", "{key}1", "{key}2")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "bitOp", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_BitOpXor(t *testing.T) {
	testName := "TestRedisProxy_BitOpXor"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.BitOpXor(context.TODO(), "dest{key}", "{key}1", "{key}2")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "bitOp", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_BitPos(t *testing.T) {
	testName := "TestRedisProxy_BitPos"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.BitPos(context.TODO(), "dest", 1, 2, 4)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "bitPos", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_GetBit(t *testing.T) {
	testName := "TestRedisProxy_GetBit"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.GetBit(context.TODO(), "key", 10)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "getBit", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SetBit(t *testing.T) {
	testName := "TestRedisProxy_SetBit"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.SetBit(context.TODO(), "key", 10, 1)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "setBit", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

/* Redis' cluster-related commands */

func TestRedisProxy_ReadOnly(t *testing.T) {
	testName := "TestRedisProxy_ReadOnly"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) != "CLUSTER" {
				t.SkipNow()
			}
			c.ReadOnly(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "readOnly", nil, MetricsCatAll, MetricsCatOther)
		})
	}
}

func TestRedisProxy_ReadWrite(t *testing.T) {
	testName := "TestRedisProxy_ReadWrite"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ReadWrite(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "readWrite", nil, MetricsCatAll, MetricsCatOther)
		})
	}
}

/* Redis' generic commands */

func TestRedisProxy_Copy(t *testing.T) {
	testName := "TestRedisProxy_Copy"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Copy(context.TODO(), "src{Key}", "dest{Key}", 0, true)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "copy", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_Del(t *testing.T) {
	testName := "TestRedisProxy_Del"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Del(context.TODO(), "{key}1", "{key}2", "{key}3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "del", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_Dump(t *testing.T) {
	testName := "TestRedisProxy_Dump"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Dump(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "dump", []error{redis.Nil}, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Exists(t *testing.T) {
	testName := "TestRedisProxy_Exists"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Exists(context.TODO(), "{key}1", "{key}2", "{key}3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "exists", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Expire(t *testing.T) {
	testName := "TestRedisProxy_Expire"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Expire(context.TODO(), "key", 1*time.Second)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "expire", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_ExpireAt(t *testing.T) {
	testName := "TestRedisProxy_ExpireAt"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ExpireAt(context.TODO(), "key", time.Now())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "expireAt", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_Keys(t *testing.T) {
	testName := "TestRedisProxy_Keys"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Keys(context.TODO(), "pattern")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "keys", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Migrate(t *testing.T) {
	testName := "TestRedisProxy_Migrate"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Migrate(context.TODO(), "host", "6379", "key", 1, 1*time.Second)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "migrate", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_Move(t *testing.T) {
	testName := "TestRedisProxy_Move"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" ||
				strings.ToUpper(_testList[i]) == "CLUSTER" {
				// MOVE is not allowed in cluster mode
				t.SkipNow()
			}
			c.Move(context.TODO(), "key", 1)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "move", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_ObjectEncoding(t *testing.T) {
	testName := "TestRedisProxy_ObjectEncoding"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ObjectEncoding(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "objectEncoding", []error{redis.Nil}, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ObjectIdleTime(t *testing.T) {
	testName := "TestRedisProxy_ObjectIdleTime"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ObjectIdleTime(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "objectIdleTime", []error{redis.Nil}, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ObjectRefCount(t *testing.T) {
	testName := "TestRedisProxy_ObjectRefCount"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ObjectRefCount(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "objectRefCount", []error{redis.Nil}, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Persist(t *testing.T) {
	testName := "TestRedisProxy_Persist"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Persist(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "persist", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_PExpire(t *testing.T) {
	testName := "TestRedisProxy_PExpire"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.PExpire(context.TODO(), "key", 1*time.Second)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "pexpire", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_PExpireAt(t *testing.T) {
	testName := "TestRedisProxy_PExpireAt"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.PExpireAt(context.TODO(), "key", time.Now())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "pexpireAt", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_Ping(t *testing.T) {
	testName := "TestRedisProxy_Ping"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Ping(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "ping", nil, MetricsCatAll, MetricsCatOther)
		})
	}
}

func TestRedisProxy_PTTL(t *testing.T) {
	testName := "TestRedisProxy_PTTL"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.PTTL(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "pttl", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_RandomKey(t *testing.T) {
	testName := "TestRedisProxy_RandomKey"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.RandomKey(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "randomKey", []error{redis.Nil}, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Rename(t *testing.T) {
	testName := "TestRedisProxy_Rename"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Set(context.TODO(), "{key}", "value", 0)
			c.Rename(context.TODO(), "{key}", "new{key}")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "rename", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_RenameNX(t *testing.T) {
	testName := "TestRedisProxy_RenameNX"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Set(context.TODO(), "{key}", "value", 0)
			c.RenameNX(context.TODO(), "{key}", "new{key}")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "renamenx", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_Restore(t *testing.T) {
	testName := "TestRedisProxy_Restore"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Set(context.TODO(), "key", "value", 0)
			dumpResult := c.Dump(context.TODO(), "key")
			c.Restore(context.TODO(), "key0", 10*time.Second, dumpResult.Val())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "restore", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_Scan(t *testing.T) {
	testName := "TestRedisProxy_Scan"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Scan(context.TODO(), 1234, "key*", 2)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "scan", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Sort(t *testing.T) {
	testName := "TestRedisProxy_Sort"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Sort(context.TODO(), "key", &redis.Sort{})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "sort", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_Touch(t *testing.T) {
	testName := "TestRedisProxy_Touch"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Touch(context.TODO(), "{key}1", "{key}2", "{key}3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "touch", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_TTL(t *testing.T) {
	testName := "TestRedisProxy_TTL"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.TTL(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "ttl", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Type(t *testing.T) {
	testName := "TestRedisProxy_Type"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Type(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "type", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Unlink(t *testing.T) {
	testName := "TestRedisProxy_Unlink"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Unlink(context.TODO(), "{key}1", "{key}2", "{key}3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "unlink", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_Wait(t *testing.T) {
	testName := "TestRedisProxy_Wait"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, rc := range _testRcList {
		t.Run(_testList[i], func(t *testing.T) {
			if rc == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			switch strings.ToUpper(_testList[i]) {
			case "FAILOVER":
				c := rc.GetFailoverClientProxy(0)
				c.Wait(context.TODO(), 0, 1*time.Second)
			case "CLUSTER":
				c := rc.GetClusterClientProxy()
				c.Wait(context.TODO(), 0, 1*time.Second)
			default:
				c := rc.GetClientProxy(0)
				c.Wait(context.TODO(), 0, 1*time.Second)
			}

			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "wait", nil, MetricsCatAll, MetricsCatOther)
		})
	}
}

/* Redis' geospatial-related commands */

func TestRedisProxy_GeoAdd(t *testing.T) {
	testName := "TestRedisProxy_GeoAdd"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.GeoAdd(context.TODO(), "key", &redis.GeoLocation{Longitude: 13.361389, Latitude: 38.115556, Name: "Palermo"})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "geoAdd", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_GeoDist(t *testing.T) {
	testName := "TestRedisProxy_GeoDist"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.GeoAdd(context.TODO(), "key",
				&redis.GeoLocation{Longitude: 13.361389, Latitude: 38.115556, Name: "member1"},
				&redis.GeoLocation{Longitude: 15.087269, Latitude: 37.502669, Name: "member2"},
			)
			c.GeoDist(context.TODO(), "key", "member1", "member2", "km")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "geoDist", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_GeoHash(t *testing.T) {
	testName := "TestRedisProxy_GeoHash"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.GeoAdd(context.TODO(), "key",
				&redis.GeoLocation{Longitude: 13.361389, Latitude: 38.115556, Name: "member1"},
				&redis.GeoLocation{Longitude: 15.087269, Latitude: 37.502669, Name: "member2"},
			)
			c.GeoHash(context.TODO(), "key", "member1", "member2", "member3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "geoHash", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_GeoPos(t *testing.T) {
	testName := "TestRedisProxy_GeoPos"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.GeoAdd(context.TODO(), "key",
				&redis.GeoLocation{Longitude: 13.361389, Latitude: 38.115556, Name: "member1"},
				&redis.GeoLocation{Longitude: 15.087269, Latitude: 37.502669, Name: "member2"},
			)
			c.GeoPos(context.TODO(), "key", "member1", "member2", "member3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "geoPos", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_GeoSearch(t *testing.T) {
	testName := "TestRedisProxy_GeoSearch"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.GeoAdd(context.TODO(), "key",
				&redis.GeoLocation{Longitude: 13.361389, Latitude: 38.115556, Name: "member1"},
				&redis.GeoLocation{Longitude: 15.087269, Latitude: 37.502669, Name: "member2"},
			)
			c.GeoSearch(context.TODO(), "key", &redis.GeoSearchQuery{})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "geoSearch", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_GeoSearchLocation(t *testing.T) {
	testName := "TestRedisProxy_GeoSearchLocation"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.GeoAdd(context.TODO(), "key",
				&redis.GeoLocation{Longitude: 13.361389, Latitude: 38.115556, Name: "member1"},
				&redis.GeoLocation{Longitude: 15.087269, Latitude: 37.502669, Name: "member2"},
			)
			c.GeoSearchLocation(context.TODO(), "key", &redis.GeoSearchLocationQuery{})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "geoSearchLocation", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_GeoSearchStore(t *testing.T) {
	testName := "TestRedisProxy_GeoSearchStore"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.GeoAdd(context.TODO(), "{key}",
				&redis.GeoLocation{Longitude: 13.361389, Latitude: 38.115556, Name: "member1"},
				&redis.GeoLocation{Longitude: 15.087269, Latitude: 37.502669, Name: "member2"},
			)
			c.GeoSearchStore(context.TODO(), "{key}", "store{key}", &redis.GeoSearchStoreQuery{})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "geoSearchStore", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

/* Redis' hash-related commands */

func TestRedisProxy_HDel(t *testing.T) {
	testName := "TestRedisProxy_HDel"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.HDel(context.TODO(), "key", "field1", "field2", "field3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "hdel", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_HExists(t *testing.T) {
	testName := "TestRedisProxy_HExists"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.HExists(context.TODO(), "key", "field")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "hexists", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_HGet(t *testing.T) {
	testName := "TestRedisProxy_HGet"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.HGet(context.TODO(), "key", "field")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "hget", []error{redis.Nil}, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_HGetAll(t *testing.T) {
	testName := "TestRedisProxy_HGetAll"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.HGetAll(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "hgetAll", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_HIncrBy(t *testing.T) {
	testName := "TestRedisProxy_HIncrBy"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.HIncrBy(context.TODO(), "key", "field", 12)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "hincrBy", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_HIncrByFloat(t *testing.T) {
	testName := "TestRedisProxy_HIncrByFloat"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.HIncrByFloat(context.TODO(), "key", "field", 1.2)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "hincrByFloat", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_HKeys(t *testing.T) {
	testName := "TestRedisProxy_HKeys"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.HKeys(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "hkeys", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_HLen(t *testing.T) {
	testName := "TestRedisProxy_HLen"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.HLen(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "hlen", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_HMGet(t *testing.T) {
	testName := "TestRedisProxy_HMGet"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.HMGet(context.TODO(), "key", "field1", "field2", "field3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "hmget", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_HRandField(t *testing.T) {
	testName := "TestRedisProxy_HRandField"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.HRandField(context.TODO(), "key", 2, true)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "hrandField", []error{redis.Nil}, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_HScan(t *testing.T) {
	testName := "TestRedisProxy_HScan"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.HScan(context.TODO(), "key", 1234, "field*", 2)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "hscan", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_HSet(t *testing.T) {
	testName := "TestRedisProxy_HSet"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.HSet(context.TODO(), "key", map[string]interface{}{"field1": "value1", "field2": 12, "field3": false})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "hset", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_HSetNX(t *testing.T) {
	testName := "TestRedisProxy_HSetNX"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.HSetNX(context.TODO(), "key", "field", "value")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "hsetnx", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_HVals(t *testing.T) {
	testName := "TestRedisProxy_HVals"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.HVals(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "hvals", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

/* Redis' hyper-log-log-related commands */

func TestRedisProxy_PFAdd(t *testing.T) {
	testName := "TestRedisProxy_PFAdd"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.PFAdd(context.TODO(), "key", "value1", "value2", "value3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "pfadd", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_PFCount(t *testing.T) {
	testName := "TestRedisProxy_PFCount"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.PFCount(context.TODO(), "{key}1", "{key}2", "{key}3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "pfcount", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_PFMerge(t *testing.T) {
	testName := "TestRedisProxy_PFMerge"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.PFMerge(context.TODO(), "dest{key}", "{key}1", "{key}2", "{key}3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "pfmerge", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

/* Redis' list-related commands */

func TestRedisProxy_BLMove(t *testing.T) {
	testName := "TestRedisProxy_BLMove"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.BLMove(context.TODO(), "src{Key}", "dest{Key}", "left", "right", 1*time.Second)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "blmove", []error{redis.Nil}, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_BLPop(t *testing.T) {
	testName := "TestRedisProxy_BLPop"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.BLPop(context.TODO(), 1*time.Second, "{key}1", "{key}2", "{key}3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "blpop", []error{redis.Nil}, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_BRPop(t *testing.T) {
	testName := "TestRedisProxy_BRPop"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.BRPop(context.TODO(), 1*time.Second, "{key}1", "{key}2", "{key}3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "brpop", []error{redis.Nil}, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_LIndex(t *testing.T) {
	testName := "TestRedisProxy_LIndex"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.LIndex(context.TODO(), "key", 12)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "lindex", []error{redis.Nil}, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_LInsert(t *testing.T) {
	testName := "TestRedisProxy_LInsert"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.LInsert(context.TODO(), "key", "BEFORE", "pivot", "value")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "linsert", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_LLen(t *testing.T) {
	testName := "TestRedisProxy_LLen"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.LLen(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "llen", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_LMove(t *testing.T) {
	testName := "TestRedisProxy_LMove"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.LMove(context.TODO(), "src{Key}", "dest{Key}", "left", "right")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "lmove", []error{redis.Nil}, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_LPop(t *testing.T) {
	testName := "TestRedisProxy_LPop"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.LPop(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "lpop", []error{redis.Nil}, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_LPos(t *testing.T) {
	testName := "TestRedisProxy_LPos"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.LPos(context.TODO(), "key", "value", redis.LPosArgs{})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "lpos", []error{redis.Nil}, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_LPush(t *testing.T) {
	testName := "TestRedisProxy_LPush"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.LPush(context.TODO(), "key", "value1", 2, true)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "lpush", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_LPushX(t *testing.T) {
	testName := "TestRedisProxy_LPushX"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.LPushX(context.TODO(), "key", "value1", 2, true)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "lpushx", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_LRange(t *testing.T) {
	testName := "TestRedisProxy_LRange"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.LRange(context.TODO(), "key", 1, 3)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "lrange", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_LRem(t *testing.T) {
	testName := "TestRedisProxy_LRem"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.LRem(context.TODO(), "key", 0, "value")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "lrem", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_LSet(t *testing.T) {
	testName := "TestRedisProxy_LSet"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.LPush(context.TODO(), "key", "value0")
			c.LSet(context.TODO(), "key", 0, "value")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "lset", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_LTrim(t *testing.T) {
	testName := "TestRedisProxy_LTrim"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.LTrim(context.TODO(), "key", 0, 2)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "ltrim", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_RPop(t *testing.T) {
	testName := "TestRedisProxy_RPop"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.RPop(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "rpop", []error{redis.Nil}, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_RPush(t *testing.T) {
	testName := "TestRedisProxy_RPush"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.RPush(context.TODO(), "key", "value1", 2, true)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "rpush", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_RPushX(t *testing.T) {
	testName := "TestRedisProxy_RPushX"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.RPushX(context.TODO(), "key", "value1", 2, true)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "rpushx", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

/* Redis' PubSub-related commands */

func TestRedisProxy_PSubscribe(t *testing.T) {
	testName := "TestRedisProxy_PSubscribe"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, rc := range _testRcList {
		t.Run(_testList[i], func(t *testing.T) {
			if rc == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			var pubsub *redis.PubSub
			defer func() {
				if pubsub != nil {
					pubsub.Close()
				}
			}()
			switch strings.ToUpper(_testList[i]) {
			case "FAILOVER":
				c := rc.GetFailoverClientProxy(0)
				pubsub = c.PSubscribe(context.TODO(), "channel1", "channel2", "channel3")
			case "CLUSTER":
				c := rc.GetClusterClientProxy()
				pubsub = c.PSubscribe(context.TODO(), "channel1", "channel2", "channel3")
			default:
				c := rc.GetClientProxy(0)
				pubsub = c.PSubscribe(context.TODO(), "channel1", "channel2", "channel3")
			}
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "psubscribe", nil, MetricsCatAll, MetricsCatOther)
		})
	}
}

func TestRedisProxy_Publish(t *testing.T) {
	testName := "TestRedisProxy_Publish"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Publish(context.TODO(), "channel", "message")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "publish", nil, MetricsCatAll, MetricsCatOther)
		})
	}
}

func TestRedisProxy_PubSubChannels(t *testing.T) {
	testName := "TestRedisProxy_PubSubChannels"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.PubSubChannels(context.TODO(), "pattern")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "pubSubChannels", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_PubSubNumPat(t *testing.T) {
	testName := "TestRedisProxy_PubSubNumPat"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.PubSubNumPat(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "pubSubNumPat", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_PubSubNumSub(t *testing.T) {
	testName := "TestRedisProxy_PubSubNumSub"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.PubSubNumSub(context.TODO(), "channel1", "channel2", "channel3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "pubSubNumSub", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Subscribe(t *testing.T) {
	testName := "TestRedisProxy_Subscribe"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, rc := range _testRcList {
		t.Run(_testList[i], func(t *testing.T) {
			if rc == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			var pubsub *redis.PubSub
			defer func() {
				if pubsub != nil {
					pubsub.Close()
				}
			}()
			switch strings.ToUpper(_testList[i]) {
			case "FAILOVER":
				c := rc.GetFailoverClientProxy(0)
				pubsub = c.Subscribe(context.TODO(), "channel1", "channel2", "channel3")
			case "CLUSTER":
				c := rc.GetClusterClientProxy()
				pubsub = c.Subscribe(context.TODO(), "channel1", "channel2", "channel3")
			default:
				c := rc.GetClientProxy(0)
				pubsub = c.Subscribe(context.TODO(), "channel1", "channel2", "channel3")
			}
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "subscribe", nil, MetricsCatAll, MetricsCatOther)
		})
	}
}

/* Redis' scripting-related commands */

func TestRedisProxy_Eval(t *testing.T) {
	testName := "TestRedisProxy_Eval"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Eval(context.TODO(), "return ARGV[1]", nil, "hello")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "eval", nil, MetricsCatAll, MetricsCatOther)
		})
	}
}

func TestRedisProxy_EvalSha(t *testing.T) {
	testName := "TestRedisProxy_EvalSha"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			scriptSha := c.ScriptLoad(context.TODO(), "return ARGV[1]")
			c.EvalSha(context.TODO(), scriptSha.Val(), nil, "hello")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "evalSha", nil, MetricsCatAll, MetricsCatOther)
		})
	}
}

func TestRedisProxy_ScriptExists(t *testing.T) {
	testName := "TestRedisProxy_ScriptExists"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ScriptExists(context.TODO(), "sha1", "sha2", "sha3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "scriptExists", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ScriptFlush(t *testing.T) {
	testName := "TestRedisProxy_ScriptFlush"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ScriptFlush(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "scriptFlush", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_ScriptKill(t *testing.T) {
	testName := "TestRedisProxy_ScriptKill"
	t.SkipNow() // TODO
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			go func() {
				c.Eval(context.TODO(), "local i=1\nwhile (i > 0) do\ni=i+1\nend\nreturn i", nil)
				// result := c.Eval(context.TODO(), "local i=1\nwhile (i > 0) do\ni=i+1\nend\nreturn i", nil)
				// fmt.Println(result.Err())
				// fmt.Println(result.Val())
			}()
			time.Sleep(10 * time.Millisecond)
			c.ScriptKill(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "scriptKill", nil, MetricsCatAll, MetricsCatOther)
		})
	}
}

func TestRedisProxy_ScriptLoad(t *testing.T) {
	testName := "TestRedisProxy_ScriptLoad"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ScriptLoad(context.TODO(), "return ARGV[1]")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "scriptLoad", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

/* Redis' server-related commands */

func TestRedisProxy_DBSize(t *testing.T) {
	testName := "TestRedisProxy_DBSize"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.DBSize(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "dbsize", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_FlushAll(t *testing.T) {
	testName := "TestRedisProxy_FlushAll"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.FlushAll(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "flushAll", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_FlushAllAsync(t *testing.T) {
	testName := "TestRedisProxy_FlushAllAsync"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.FlushAllAsync(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "flushAllAsync", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_FlushDB(t *testing.T) {
	testName := "TestRedisProxy_FlushDB"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.FlushDB(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "flushDb", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_FlushDBAsync(t *testing.T) {
	testName := "TestRedisProxy_FlushDBAsync"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.FlushDBAsync(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "flushDbAsync", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

/* Redis' set-related commands */

func TestRedisProxy_SAdd(t *testing.T) {
	testName := "TestRedisProxy_SAdd"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.SAdd(context.TODO(), "key", "value1", "value2", "value3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "sadd", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_SCard(t *testing.T) {
	testName := "TestRedisProxy_SCard"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.SCard(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "scard", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SDiff(t *testing.T) {
	testName := "TestRedisProxy_SDiff"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.SDiff(context.TODO(), "{key}1", "{key}2")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "sdiff", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SDiffStore(t *testing.T) {
	testName := "TestRedisProxy_SDiffStore"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.SDiffStore(context.TODO(), "dest{key}", "{key}1", "{key}2")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "sdiffStore", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_SInter(t *testing.T) {
	testName := "TestRedisProxy_SInter"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.SInter(context.TODO(), "{key}1", "{key}2")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "sinter", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SInterStore(t *testing.T) {
	testName := "TestRedisProxy_SInterStore"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.SInterStore(context.TODO(), "dest{key}", "{key}1", "{key}2")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "sinterStore", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_SIsMember(t *testing.T) {
	testName := "TestRedisProxy_SIsMember"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.SIsMember(context.TODO(), "key", "value")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "sisMember", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SMembers(t *testing.T) {
	testName := "TestRedisProxy_SMembers"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.SMembers(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "smembers", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SMIsMember(t *testing.T) {
	testName := "TestRedisProxy_SMIsMember"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.SMIsMember(context.TODO(), "key", "value1", "value2", "value3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "smisMember", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SMove(t *testing.T) {
	testName := "TestRedisProxy_SMove"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.SMove(context.TODO(), "src{key}", "dest{key}", "value")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "smove", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_SPop(t *testing.T) {
	testName := "TestRedisProxy_SPop"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.SPop(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "spop", []error{redis.Nil}, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_SPopN(t *testing.T) {
	testName := "TestRedisProxy_SPopN"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.SPopN(context.TODO(), "key", 12)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "spop", []error{redis.Nil}, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_SRandMember(t *testing.T) {
	testName := "TestRedisProxy_SRandMember"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.SRandMember(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "srandMember", []error{redis.Nil}, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SRandMemberN(t *testing.T) {
	testName := "TestRedisProxy_SRandMemberN"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.SRandMemberN(context.TODO(), "key", 3)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "srandMember", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SRem(t *testing.T) {
	testName := "TestRedisProxy_SRem"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.SRem(context.TODO(), "key", "value1", 2, true)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "srem", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_SScan(t *testing.T) {
	testName := "TestRedisProxy_SScan"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.SScan(context.TODO(), "key", 0, "pattern*", 2)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "sscan", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SUnion(t *testing.T) {
	testName := "TestRedisProxy_SUnion"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.SUnion(context.TODO(), "{key}1", "{key}2", "{key}3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "sunion", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SUnionStore(t *testing.T) {
	testName := "TestRedisProxy_SUnionStore"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.SUnionStore(context.TODO(), "dest{key}", "{key}1", "{key}2", "{key}3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "sunionStore", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

/* Redis' sorted set-related commands */

func TestRedisProxy_BZPopMax(t *testing.T) {
	testName := "TestRedisProxy_BZPopMax"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.BZPopMax(context.TODO(), 10*time.Second, "{key}1", "{key}2", "{key}3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "bzpopMax", []error{redis.Nil}, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_BZPopMin(t *testing.T) {
	testName := "TestRedisProxy_BZPopMin"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.BZPopMin(context.TODO(), 10*time.Second, "{key}1", "{key}2", "{key}3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "bzpopMin", []error{redis.Nil}, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_ZAdd(t *testing.T) {
	testName := "TestRedisProxy_ZAdd"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZAdd(context.TODO(), "key", &redis.Z{Member: "member1", Score: 1.23}, &redis.Z{Member: 2, Score: 2.34}, &redis.Z{Member: true, Score: 3.45})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zadd", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_ZCard(t *testing.T) {
	testName := "TestRedisProxy_ZCard"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZCard(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zcard", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZCount(t *testing.T) {
	testName := "TestRedisProxy_ZCount"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZCount(context.TODO(), "key", "1.23", "2.34")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zcount", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZDiff(t *testing.T) {
	testName := "TestRedisProxy_ZDiff"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZDiff(context.TODO(), "{key}1", "{key}2", "{key}3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zdiff", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZDiffWithScores(t *testing.T) {
	testName := "TestRedisProxy_ZDiffWithScores"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZDiffWithScores(context.TODO(), "{key}1", "{key}2", "{key}3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zdiff", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZDiffStore(t *testing.T) {
	testName := "TestRedisProxy_ZDiffStore"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZDiffStore(context.TODO(), "dest{key}", "{key}1", "{key}2", "{key}3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zdiffStore", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_ZIncrBy(t *testing.T) {
	testName := "TestRedisProxy_ZIncrBy"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZIncrBy(context.TODO(), "key", 1.23, "member")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zincrBy", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_ZInter(t *testing.T) {
	testName := "TestRedisProxy_ZInter"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZInter(context.TODO(), &redis.ZStore{Keys: []string{"{key}1", "{key}2", "{key}3"}})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zinter", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZInterWithScores(t *testing.T) {
	testName := "TestRedisProxy_ZInterWithScores"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZInterWithScores(context.TODO(), &redis.ZStore{Keys: []string{"{key}1", "{key}2", "{key}3"}})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zinter", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZInterStore(t *testing.T) {
	testName := "TestRedisProxy_ZInterStore"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZInterStore(context.TODO(), "dest{key}", &redis.ZStore{Keys: []string{"{key}1", "{key}2", "{key}3"}})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zinterStore", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_ZLexCount(t *testing.T) {
	testName := "TestRedisProxy_ZLexCount"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZLexCount(context.TODO(), "key", "-", "+")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zlexCount", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZMScore(t *testing.T) {
	testName := "TestRedisProxy_ZMScore"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZMScore(context.TODO(), "key", "member1", "member2", "member3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zmscore", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZPopMax(t *testing.T) {
	testName := "TestRedisProxy_ZPopMax"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZPopMax(context.TODO(), "key", 1)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zpopMax", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_ZPopMin(t *testing.T) {
	testName := "TestRedisProxy_ZPopMin"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZPopMin(context.TODO(), "key", 1)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zpopMin", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_ZRandMember(t *testing.T) {
	testName := "TestRedisProxy_ZRandMember"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZRandMember(context.TODO(), "key", 1, false)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zrandMember", []error{redis.Nil}, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZRange(t *testing.T) {
	testName := "TestRedisProxy_ZRange"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZRange(context.TODO(), "key", 0, 10)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zrange", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZRangeWithScores(t *testing.T) {
	testName := "TestRedisProxy_ZRangeWithScores"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZRangeWithScores(context.TODO(), "key", 0, 10)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zrange", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZRangeByLex(t *testing.T) {
	testName := "TestRedisProxy_ZRangeByLex"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZRangeByLex(context.TODO(), "key", &redis.ZRangeBy{Min: "-", Max: "+"})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zrange", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZRangeByScore(t *testing.T) {
	testName := "TestRedisProxy_ZRangeByScore"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZRangeByScore(context.TODO(), "key", &redis.ZRangeBy{Min: "1.23", Max: "2.34"})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zrange", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZRangeByScoreWithScores(t *testing.T) {
	testName := "TestRedisProxy_ZRangeByScoreWithScores"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZRangeByScoreWithScores(context.TODO(), "key", &redis.ZRangeBy{Min: "1.23", Max: "2.34"})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zrange", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZRangeStore(t *testing.T) {
	testName := "TestRedisProxy_ZRangeStore"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZAdd(context.TODO(), "{key}", &redis.Z{Member: "one", Score: 1}, &redis.Z{Member: "two", Score: 2}, &redis.Z{Member: "three", Score: 3}, &redis.Z{Member: "four", Score: 4})
			c.ZRangeStore(context.TODO(), "dest{key}", redis.ZRangeArgs{Key: "{key}", Start: "2", Stop: "-1"})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zrangeStore", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_ZRank(t *testing.T) {
	testName := "TestRedisProxy_ZRank"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZRank(context.TODO(), "key", "member")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zrank", []error{redis.Nil}, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZRem(t *testing.T) {
	testName := "TestRedisProxy_ZRem"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZRem(context.TODO(), "key", "member")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zrem", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_ZRemRangeByLex(t *testing.T) {
	testName := "TestRedisProxy_ZRemRangeByLex"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZRemRangeByLex(context.TODO(), "key", "-", "+")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zremRangeByLex", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_ZRemRangeByRank(t *testing.T) {
	testName := "TestRedisProxy_ZRemRangeByRank"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZRemRangeByRank(context.TODO(), "key", 2, -1)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zremRangeByRank", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_ZRemRangeByScore(t *testing.T) {
	testName := "TestRedisProxy_ZRemRangeByScore"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZRemRangeByScore(context.TODO(), "key", "1.23", "2.34")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zremRangeByScore", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_ZRevRange(t *testing.T) {
	testName := "TestRedisProxy_ZRevRange"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZRevRange(context.TODO(), "key", 2, -1)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zrevRange", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZRevRangeWithScores(t *testing.T) {
	testName := "TestRedisProxy_ZRevRangeWithScores"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZRevRangeWithScores(context.TODO(), "key", 2, -1)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zrevRange", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZRevRangeByLex(t *testing.T) {
	testName := "TestRedisProxy_ZRevRangeByLex"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZRevRangeByLex(context.TODO(), "key", &redis.ZRangeBy{Min: "-", Max: "+"})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zrevRangeByLex", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZRevRangeByScore(t *testing.T) {
	testName := "TestRedisProxy_ZRevRangeByScore"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZRevRangeByScore(context.TODO(), "key", &redis.ZRangeBy{Min: "1.23", Max: "2.34"})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zrevRangeByScore", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZRevRangeByScoreWithScores(t *testing.T) {
	testName := "TestRedisProxy_ZRevRangeByScoreWithScores"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZRevRangeByScoreWithScores(context.TODO(), "key", &redis.ZRangeBy{Min: "1.23", Max: "2.34"})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zrevRangeByScore", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZRevRank(t *testing.T) {
	testName := "TestRedisProxy_ZRevRank"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZRevRank(context.TODO(), "key", "member")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zrevRank", []error{redis.Nil}, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZScan(t *testing.T) {
	testName := "TestRedisProxy_ZScan"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZScan(context.TODO(), "key", 123, "pattern", 1)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zscan", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZScore(t *testing.T) {
	testName := "TestRedisProxy_ZScore"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZScore(context.TODO(), "key", "member")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zscore", []error{redis.Nil}, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZUnion(t *testing.T) {
	testName := "TestRedisProxy_ZUnion"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZUnion(context.TODO(), redis.ZStore{Keys: []string{"{key}1", "{key}2", "{key}3"}})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zunion", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZUnionWithScores(t *testing.T) {
	testName := "TestRedisProxy_ZUnionWithScores"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZUnionWithScores(context.TODO(), redis.ZStore{Keys: []string{"{key}1", "{key}2", "{key}3"}})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zunion", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_ZUnionStore(t *testing.T) {
	testName := "TestRedisProxy_ZUnionStore"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.ZUnionStore(context.TODO(), "dest{key}", &redis.ZStore{Keys: []string{"{key}1", "{key}2", "{key}3"}})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "zunionStore", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

/* Redis' stream-related commands */

func TestRedisProxy_XAck(t *testing.T) {
	testName := "TestRedisProxy_XAck"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XAck(context.TODO(), "stream", "group", "1", "2", "3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xack", nil, MetricsCatAll, MetricsCatOther)
		})
	}
}

func TestRedisProxy_XAdd(t *testing.T) {
	testName := "TestRedisProxy_XAdd"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XAdd(context.TODO(), &redis.XAddArgs{Stream: "stream", Values: map[string]interface{}{"key": "value"}})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xadd", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_XAutoClaim(t *testing.T) {
	testName := "TestRedisProxy_XAutoClaim"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XGroupCreateMkStream(context.TODO(), "stream", "group", "0-0")
			c.XAutoClaim(context.TODO(), &redis.XAutoClaimArgs{Stream: "stream", Group: "group", Consumer: "consumer", MinIdle: 10 * time.Second, Start: "0-0"})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xautoClaim", nil, MetricsCatAll, MetricsCatOther)
		})
	}
}

func TestRedisProxy_XAutoClaimJustID(t *testing.T) {
	testName := "TestRedisProxy_XAutoClaimJustID"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XGroupCreateMkStream(context.TODO(), "stream", "group", "0-0")
			c.XAutoClaimJustID(context.TODO(), &redis.XAutoClaimArgs{Stream: "stream", Group: "group", Consumer: "consumer", MinIdle: 10 * time.Second, Start: "0-0"})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xautoClaim", nil, MetricsCatAll, MetricsCatOther)
		})
	}
}

func TestRedisProxy_XDel(t *testing.T) {
	testName := "TestRedisProxy_XDel"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XDel(context.TODO(), "stream", "0-1", "0-2", "0-3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xdel", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_XGroupCreate(t *testing.T) {
	testName := "TestRedisProxy_XGroupCreate"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XAdd(context.TODO(), &redis.XAddArgs{Stream: "stream", Values: map[string]interface{}{"key": "value"}})
			c.XGroupCreate(context.TODO(), "stream", "group", "0-0")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xgroupCreate", nil, MetricsCatAll, MetricsCatDDL)
		})
	}
}

func TestRedisProxy_XGroupCreateMkStream(t *testing.T) {
	testName := "TestRedisProxy_XGroupCreateMkStream"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XGroupCreateMkStream(context.TODO(), "stream", "group", "0-0")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xgroupCreate", nil, MetricsCatAll, MetricsCatDDL)
		})
	}
}

func TestRedisProxy_XGroupCreateConsumer(t *testing.T) {
	testName := "TestRedisProxy_XGroupCreateConsumer"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XGroupCreateMkStream(context.TODO(), "stream", "group", "0-0")
			c.XGroupCreateConsumer(context.TODO(), "stream", "group", "consumer")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xgroupCreateConsumer", nil, MetricsCatAll, MetricsCatOther)
		})
	}
}

func TestRedisProxy_XGroupDelConsumer(t *testing.T) {
	testName := "TestRedisProxy_XGroupDelConsumer"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XGroupCreateMkStream(context.TODO(), "stream", "group", "0-0")
			c.XGroupDelConsumer(context.TODO(), "stream", "group", "consumer")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xgroupDelConsumer", nil, MetricsCatAll, MetricsCatOther)
		})
	}
}

func TestRedisProxy_XGroupDestroy(t *testing.T) {
	testName := "TestRedisProxy_XGroupDestroy"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XGroupCreateMkStream(context.TODO(), "stream", "group", "0-0")
			c.XGroupDestroy(context.TODO(), "stream", "group")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xgroupDestroy", nil, MetricsCatAll, MetricsCatDDL)
		})
	}
}

func TestRedisProxy_XGroupSetID(t *testing.T) {
	testName := "TestRedisProxy_XGroupSetID"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XGroupCreateMkStream(context.TODO(), "stream", "group", "0-0")
			c.XGroupSetID(context.TODO(), "stream", "group", "0-0")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xgroupSetId", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_XInfoConsumers(t *testing.T) {
	testName := "TestRedisProxy_XInfoConsumers"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XGroupCreateMkStream(context.TODO(), "key", "group", "0-0")
			c.XInfoConsumers(context.TODO(), "key", "group")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xinfoConsumers", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_XInfoGroups(t *testing.T) {
	testName := "TestRedisProxy_XInfoGroups"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XGroupCreateMkStream(context.TODO(), "key", "group", "0-0")
			c.XInfoGroups(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xinfoGroups", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_XInfoStream(t *testing.T) {
	testName := "TestRedisProxy_XInfoStream"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XGroupCreateMkStream(context.TODO(), "key", "group", "0-0")
			c.XInfoStream(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xinfoStream", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_XInfoStreamFull(t *testing.T) {
	testName := "TestRedisProxy_XInfoStreamFull"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XGroupCreateMkStream(context.TODO(), "key", "group", "0-0")
			c.XInfoStreamFull(context.TODO(), "key", 12)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xinfoStream", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_XLen(t *testing.T) {
	testName := "TestRedisProxy_XLen"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XLen(context.TODO(), "stream")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xlen", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_XPending(t *testing.T) {
	testName := "TestRedisProxy_XPending"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XGroupCreateMkStream(context.TODO(), "stream", "group", "0-0")
			c.XPending(context.TODO(), "stream", "group")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xpending", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_XPendingExt(t *testing.T) {
	testName := "TestRedisProxy_XPendingExt"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XGroupCreateMkStream(context.TODO(), "stream", "group", "0-0")
			c.XPendingExt(context.TODO(), &redis.XPendingExtArgs{Stream: "stream", Group: "group", Start: "0-0", End: "0-1", Count: 1})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xpending", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_XRange(t *testing.T) {
	testName := "TestRedisProxy_XRange"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XRange(context.TODO(), "stream", "0-0", "0-1")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xrange", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_XRangeN(t *testing.T) {
	testName := "TestRedisProxy_XRangeN"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XRangeN(context.TODO(), "stream", "0-0", "0-1", 12)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xrange", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_XRead(t *testing.T) {
	testName := "TestRedisProxy_XRead"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XRead(context.TODO(), &redis.XReadArgs{Count: 1, Block: 10 * time.Second, Streams: []string{"stream", "0-0"}})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xread", []error{redis.Nil}, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_XReadGroup(t *testing.T) {
	testName := "TestRedisProxy_XReadGroup"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XGroupCreateMkStream(context.TODO(), "stream", "group", "0-0")
			c.XReadGroup(context.TODO(), &redis.XReadGroupArgs{Group: "group", Count: 1, Block: 10 * time.Second, Streams: []string{"stream", "0-0"}})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xreadGroup", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_XRevRange(t *testing.T) {
	testName := "TestRedisProxy_XRevRange"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XRevRange(context.TODO(), "stream", "0-0", "0-1")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xrevRange", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_XRevRangeN(t *testing.T) {
	testName := "TestRedisProxy_XRevRangeN"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XRevRangeN(context.TODO(), "stream", "0-0", "0-1", 12)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xrevRange", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_XTrimMaxLen(t *testing.T) {
	testName := "TestRedisProxy_XTrimMaxLen"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XTrimMaxLen(context.TODO(), "stream", 12)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xtrim", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_XTrimMaxLenApprox(t *testing.T) {
	testName := "TestRedisProxy_XTrimMaxLenApprox"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XTrimMaxLenApprox(context.TODO(), "stream", 12, 2)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xtrim", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_XTrimMinID(t *testing.T) {
	testName := "TestRedisProxy_XTrimMinID"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XTrimMinID(context.TODO(), "stream", "0-0")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xtrim", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_XTrimMinIDApprox(t *testing.T) {
	testName := "TestRedisProxy_XTrimMinIDApprox"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.XTrimMinIDApprox(context.TODO(), "stream", "0-0", 2)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "xtrim", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

/* Redis' string-related commands */

func TestRedisProxy_Append(t *testing.T) {
	testName := "TestRedisProxy_Append"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Append(context.TODO(), "key", "value")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "append", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_Decr(t *testing.T) {
	testName := "TestRedisProxy_Decr"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Decr(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "decr", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_DecrBy(t *testing.T) {
	testName := "TestRedisProxy_Decrby"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.DecrBy(context.TODO(), "key", 12)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "decrBy", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_Get(t *testing.T) {
	testName := "TestRedisProxy_Get"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Get(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "get", []error{redis.Nil}, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_GetDel(t *testing.T) {
	testName := "TestRedisProxy_GetDel"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.GetDel(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "getDel", []error{redis.Nil}, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_GetEx(t *testing.T) {
	testName := "TestRedisProxy_GetEx"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.GetEx(context.TODO(), "key", 10*time.Second)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "getEx", []error{redis.Nil}, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_GetRange(t *testing.T) {
	testName := "TestRedisProxy_GetRange"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.GetRange(context.TODO(), "key", 0, 10)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "getRange", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_GetSet(t *testing.T) {
	testName := "TestRedisProxy_GetSet"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.GetSet(context.TODO(), "key", "value")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "getSet", []error{redis.Nil}, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_Incr(t *testing.T) {
	testName := "TestRedisProxy_Incr"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Incr(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "incr", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_IncrBy(t *testing.T) {
	testName := "TestRedisProxy_IncrBy"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.IncrBy(context.TODO(), "key", 12)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "incrBy", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_IncrByFloat(t *testing.T) {
	testName := "TestRedisProxy_IncrByFloat"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.IncrByFloat(context.TODO(), "key", 1.2)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "incrByFloat", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_MGet(t *testing.T) {
	testName := "TestRedisProxy_MGet"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.MGet(context.TODO(), "{key}1", "{key}3", "{key}2")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "mget", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

func TestRedisProxy_MSet(t *testing.T) {
	testName := "TestRedisProxy_MSet"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.MSet(context.TODO(), map[string]interface{}{"{key}1": "value1", "{key}2": 2, "{key}3": true})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "mset", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_MSetNX(t *testing.T) {
	testName := "TestRedisProxy_MSetNX"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.MSetNX(context.TODO(), map[string]interface{}{"{key}1": "value1", "{key}2": 2, "{key}3": true})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "msetnx", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_Set(t *testing.T) {
	testName := "TestRedisProxy_Set"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.Set(context.TODO(), "key", "value", 10*time.Second)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "set", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_SetEX(t *testing.T) {
	testName := "TestRedisProxy_SetEX"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.SetEX(context.TODO(), "key", "value", 10*time.Second)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "setex", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_SetNX(t *testing.T) {
	testName := "TestRedisProxy_SetNX"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.SetNX(context.TODO(), "key", "value", 10*time.Second)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "setnx", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_SetRange(t *testing.T) {
	testName := "TestRedisProxy_SetRange"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.SetRange(context.TODO(), "key", 10, "value")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "setRange", nil, MetricsCatAll, MetricsCatDML)
		})
	}
}

func TestRedisProxy_StrLen(t *testing.T) {
	testName := "TestRedisProxy_StrLen"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for i, c := range _testCmdableList {
		t.Run(_testList[i], func(t *testing.T) {
			if c == nil || strings.ToUpper(_testList[i]) == "FAILOVER" {
				t.SkipNow()
			}
			c.StrLen(context.TODO(), "key")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+_testList[i], _testRcList[i], "strLen", nil, MetricsCatAll, MetricsCatDQL)
		})
	}
}

/* Redis' transaction-related commands */
