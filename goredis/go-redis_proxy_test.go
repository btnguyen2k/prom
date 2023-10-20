package goredis

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/btnguyen2k/consu/semver"
	"github.com/btnguyen2k/prom"
	"github.com/redis/go-redis/v9"
)

type _testFailedWithMsgFunc func(msg string)

type _testSetupOrTeardownFunc func(t *testing.T, testName string)

func setupTest(t *testing.T, testName string, extraSetupFunc, extraTeardownFunc _testSetupOrTeardownFunc) func(t *testing.T) {
	if extraSetupFunc != nil {
		extraSetupFunc(t, testName)
	}
	return func(t *testing.T) {
		if extraTeardownFunc != nil {
			extraTeardownFunc(t, testName)
		}
	}
}

func _rcVerifyLastCommand(f _testFailedWithMsgFunc, testName string, rc *GoRedisConnect, cmdName string, ignoredErrs []error, cmdCats ...string) {
	for _, cat := range cmdCats {
		m, err := rc.Metrics(cat, prom.MetricsOpts{ReturnLatestCommands: 1})
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
		if cmd.CmdName != cmdName || cmd.Result != prom.CmdResultOk || cmd.Error != nil || cmd.Cost < 0 {
			f(fmt.Sprintf("%s failed: invalid last command metrics.\nExpected: [Name=%v / Result=%v / Error = %e / Cost = %v]\nReceived: [Name=%v / Result=%v / Error = %s / Cost = %v]",
				testName+"/Metrics("+cat+")",
				cmdName, prom.CmdResultOk, error(nil), ">= 0",
				cmd.CmdName, cmd.Result, cmd.Error, cmd.Cost))
		}
	}
}

func _newGoRedisConnectForRedisClient(t *testing.T, testName, hostAndPort, redisPassword string) *GoRedisConnect {
	hostsAndPorts := hostAndPort
	if hostsAndPorts == "" {
		hostsAndPorts = strings.ReplaceAll(os.Getenv("REDIS_HOSTS_AND_PORTS"), `"`, "")
	}
	if hostsAndPorts == "" {
		return nil
	}
	password := redisPassword
	if password == "" {
		password = strings.ReplaceAll(os.Getenv("REDIS_PASSWORD"), `"`, "")
	}
	rc, err := NewGoRedisConnect(hostsAndPorts, password, 0)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	return rc
}

func _newGoRedisConnectForRedisClusterClient(t *testing.T, testName, hostAndPort, redisPassword string) *GoRedisConnect {
	hostsAndPorts := hostAndPort
	if hostsAndPorts == "" {
		hostsAndPorts = strings.ReplaceAll(os.Getenv("REDIS_CLUSTER_HOSTS_AND_PORTS"), `"`, "")
	}
	if hostsAndPorts == "" {
		return nil
	}
	password := redisPassword
	if password == "" {
		password = strings.ReplaceAll(os.Getenv("REDIS_CLUSTER_PASSWORD"), `"`, "")
	}
	rc, err := NewGoRedisConnect(hostsAndPorts, password, 0)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	rc.poolOpts.ReadTimeout = 30 * time.Second
	rc.poolOpts.WriteTimeout = 30 * time.Second
	rc.poolOpts.DialTimeout = 30 * time.Second
	return rc
}

func _newGoRedisConnectForRedisFailoverClient(t *testing.T, testName, hostAndPort, redisPassword, masterName string) *GoRedisConnect {
	hostsAndPorts := hostAndPort
	if hostsAndPorts == "" {
		hostsAndPorts = strings.ReplaceAll(os.Getenv("REDIS_FAILOVER_HOSTS_AND_PORTS"), `"`, "")
	}
	if hostsAndPorts == "" {
		return nil
	}
	password := redisPassword
	if password == "" {
		password = strings.ReplaceAll(os.Getenv("REDIS_FAILOVER_PASSWORD"), `"`, "")
	}
	rc, err := NewGoRedisConnect(hostsAndPorts, password, 0)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}

	if masterName == "" {
		masterName = strings.ReplaceAll(os.Getenv("REDIS_FAILOVER_MASTER_NAME"), `"`, "")
	}
	rc.SetSentinelMasterName(masterName)

	return rc
}

var _testList = []string{"SINGLE", "FAILOVER", "CLUSTER"}

var (
	_testRcSingle   *GoRedisConnect
	_testRcCluster  *GoRedisConnect
	_testRcFailover map[string]*GoRedisConnect
)

var _setupTestRedisProxy _testSetupOrTeardownFunc = func(t *testing.T, testName string) {
	_testRcSingle = _newGoRedisConnectForRedisClient(t, testName, "localhost:6379", "")
	if _testRcSingle != nil {
		_testRcSingle.GetClient(0).FlushAll(context.TODO())
	}

	_testRcCluster = _newGoRedisConnectForRedisClusterClient(t, testName, "localhost:7000,localhost:7001,localhost:7002", "")
	if _testRcCluster != nil {
		for _, hostAndPort := range _testRcCluster.hostsAndPorts {
			client := _testRcCluster.newClientWithHostAndPort(hostAndPort, 0)
			client.FlushAll(context.TODO())
		}
	}

	_testRcFailover = make(map[string]*GoRedisConnect)
	_testRcFailover["5000"] = _newGoRedisConnectForRedisFailoverClient(t, testName, "localhost:5000", "", "sentinel7000")
	_testRcFailover["5001"] = _newGoRedisConnectForRedisFailoverClient(t, testName, "localhost:5001", "", "sentinel7001")
	_testRcFailover["5002"] = _newGoRedisConnectForRedisFailoverClient(t, testName, "localhost:5002", "", "sentinel7002")
}

var _teardownTestRedisProxy _testSetupOrTeardownFunc = func(t *testing.T, testName string) {
	if _testRcSingle != nil {
		_testRcSingle.Close()
	}
	if _testRcCluster != nil {
		_testRcCluster.Close()
	}
	for _, rc := range _testRcFailover {
		rc.Close()
	}
}

var _testRcList []*GoRedisConnect
var _testCmdableList []redis.Cmdable

func _getRedisConnectAndCmdable(typ, key string) (*GoRedisConnect, redis.Cmdable) {
	switch typ {
	case "SINGLE":
		return _testRcSingle, _testRcSingle.GetClientProxy(0)
	case "FAILOVER":
		client, _ := _testRcCluster.GetClusterClient().MasterForKey(context.TODO(), key)
		info := ParseRedisInfo(client.Info(context.TODO()).Val())
		tcpPort := info.GetSection("Server")["tcp_port"]
		if tcpPort == "7000" {
			return _testRcFailover["5000"], _testRcFailover["5000"].GetFailoverClientProxy(0)
		}
		if tcpPort == "7001" {
			return _testRcFailover["5001"], _testRcFailover["5001"].GetFailoverClientProxy(0)
		}
		if tcpPort == "7002" {
			return _testRcFailover["5002"], _testRcFailover["5002"].GetFailoverClientProxy(0)
		}
	case "CLUSTER":
		return _testRcCluster, _testRcCluster.GetClusterClientProxy()
	}
	return nil, nil
}

func _getRedisVersion(c redis.Cmdable) semver.Semver {
	rv := ParseRedisInfo(c.Info(context.Background()).Val())
	return semver.ParseSemver(rv.GetSection("Server")["redis_version"])
}

/* Redis' bitmap-related commands */

func TestRedisProxy_BitCount(t *testing.T) {
	testName := "TestRedisProxy_BitCount"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.BitCount(context.TODO(), key, &redis.BitCount{})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "bit_count", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_BitField(t *testing.T) {
	testName := "TestRedisProxy_BitField"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.BitField(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "bit_field", nil, prom.MetricsCatAll, prom.MetricsCatOther)
		})
	}
}

func TestRedisProxy_BitOpAnd(t *testing.T) {
	testName := "TestRedisProxy_BitOpAnd"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.BitOpAnd(context.TODO(), key+"dest", key+"1", key+"2")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "bit_op", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_BitOpNot(t *testing.T) {
	testName := "TestRedisProxy_BitOpNot"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.BitOpNot(context.TODO(), key+"dest", key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "bit_op", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_BitOpOr(t *testing.T) {
	testName := "TestRedisProxy_BitOpOr"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.BitOpOr(context.TODO(), key+"dest", key+"1", key+"2")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "bit_op", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_BitOpXor(t *testing.T) {
	testName := "TestRedisProxy_BitOpXor"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.BitOpXor(context.TODO(), key+"dest", key+"1", key+"2")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "bit_op", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_BitPos(t *testing.T) {
	testName := "TestRedisProxy_BitPos"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "dest"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.BitPos(context.TODO(), key, 1, 2, 4)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "bit_pos", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_GetBit(t *testing.T) {
	testName := "TestRedisProxy_GetBit"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.GetBit(context.TODO(), key, 10)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "get_bit", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SetBit(t *testing.T) {
	testName := "TestRedisProxy_SetBit"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.SetBit(context.TODO(), key, 10, 1)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "set_bit", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

/* Redis' cluster-related commands */

func TestRedisProxy_ReadOnly(t *testing.T) {
	testName := "TestRedisProxy_ReadOnly"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			if strings.ToUpper(tc) != "CLUSTER" {
				t.SkipNow()
			}
			rc, c := _getRedisConnectAndCmdable(tc, "")
			c.ReadOnly(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "read_only", nil, prom.MetricsCatAll, prom.MetricsCatOther)
		})
	}
}

func TestRedisProxy_ReadWrite(t *testing.T) {
	testName := "TestRedisProxy_ReadWrite"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			if strings.ToUpper(tc) != "CLUSTER" {
				t.SkipNow()
			}
			rc, c := _getRedisConnectAndCmdable(tc, "")
			c.ReadWrite(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "read_write", nil, prom.MetricsCatAll, prom.MetricsCatOther)
		})
	}
}

/* Redis' generic commands */

func TestRedisProxy_Generic_Copy(t *testing.T) {
	testName := "TestRedisProxy_Generic_Copy"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.Copy(context.TODO(), key+"src", key+"dest", 0, true)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "copy", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Generic_Del(t *testing.T) {
	testName := "TestRedisProxy_Generic_Del"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Del(context.TODO(), key+"1", key+"2", key+"3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "del", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Generic_Dump(t *testing.T) {
	testName := "TestRedisProxy_Generic_Dump"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Dump(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "dump", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Generic_Exists(t *testing.T) {
	testName := "TestRedisProxy_Generic_Exists"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Exists(context.TODO(), key+"1", key+"2", key+"3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "exists", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Generic_Expire(t *testing.T) {
	testName := "TestRedisProxy_Generic_Expire"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Expire(context.TODO(), key, 1*time.Second)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "expire", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

// func TestRedisProxy_Generic_ExpireGT(t *testing.T) {
// 	testName := "TestRedisProxy_Generic_ExpireGT"
// 	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
// 	defer teardownTest(t)
// 	for _, tc := range _testList {
// 		t.Run(tc, func(t *testing.T) {
// 			key := "key"
// 			rc, c := _getRedisConnectAndCmdable(tc, key)
// 			c.ExpireGT(context.TODO(), key, 1*time.Second)
// 			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "expire", nil, prom.MetricsCatAll, prom.MetricsCatDML)
// 		})
// 	}
// }
//
// func TestRedisProxy_Generic_ExpireLT(t *testing.T) {
// 	testName := "TestRedisProxy_Generic_ExpireLT"
// 	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
// 	defer teardownTest(t)
// 	for _, tc := range _testList {
// 		t.Run(tc, func(t *testing.T) {
// 			key := "key"
// 			rc, c := _getRedisConnectAndCmdable(tc, key)
// 			c.ExpireLT(context.TODO(), key, 1*time.Second)
// 			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "expire", nil, prom.MetricsCatAll, prom.MetricsCatDML)
// 		})
// 	}
// }
//
// func TestRedisProxy_Generic_ExpireNX(t *testing.T) {
// 	testName := "TestRedisProxy_Generic_ExpireNX"
// 	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
// 	defer teardownTest(t)
// 	for _, tc := range _testList {
// 		t.Run(tc, func(t *testing.T) {
// 			key := "key"
// 			rc, c := _getRedisConnectAndCmdable(tc, key)
// 			c.ExpireNX(context.TODO(), key, 1*time.Second)
// 			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "expire", nil, prom.MetricsCatAll, prom.MetricsCatDML)
// 		})
// 	}
// }
//
// func TestRedisProxy_Generic_ExpireXX(t *testing.T) {
// 	testName := "TestRedisProxy_Generic_ExpireXX"
// 	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
// 	defer teardownTest(t)
// 	for _, tc := range _testList {
// 		t.Run(tc, func(t *testing.T) {
// 			key := "key"
// 			rc, c := _getRedisConnectAndCmdable(tc, key)
// 			c.ExpireXX(context.TODO(), key, 1*time.Second)
// 			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "expire", nil, prom.MetricsCatAll, prom.MetricsCatDML)
// 		})
// 	}
// }

func TestRedisProxy_Generic_ExpireTime(t *testing.T) {
	testName := "TestRedisProxy_Generic_ExpireTime"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v7_0_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v7_0_0)
			}
			c.ExpireTime(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "expire_time", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Generic_ExpireAt(t *testing.T) {
	testName := "TestRedisProxy_Generic_ExpireAt"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ExpireAt(context.TODO(), key, time.Now())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "expire_at", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Generic_Keys(t *testing.T) {
	testName := "TestRedisProxy_Generic_Keys"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Keys(context.TODO(), "pattern")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "keys", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Generic_Migrate(t *testing.T) {
	testName := "TestRedisProxy_Generic_Migrate"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Migrate(context.TODO(), "host", "6379", key, 1, 1*time.Second)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "migrate", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Generic_Move(t *testing.T) {
	testName := "TestRedisProxy_Generic_Move"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			if tc != "SINGLE" {
				t.Skipf("%s skipped: command MOVE is not supported in cluster mode", testName+"/"+tc)
			}
			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Move(context.TODO(), key, 1)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "move", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Generic_ObjectEncoding(t *testing.T) {
	testName := "TestRedisProxy_Generic_ObjectEncoding"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ObjectEncoding(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "object_encoding", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Generic_ObjectIdleTime(t *testing.T) {
	testName := "TestRedisProxy_Generic_ObjectIdleTime"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ObjectIdleTime(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "object_idle_time", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Generic_ObjectRefCount(t *testing.T) {
	testName := "TestRedisProxy_Generic_ObjectRefCount"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ObjectRefCount(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "object_ref_count", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Generic_Persist(t *testing.T) {
	testName := "TestRedisProxy_Generic_Persist"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Persist(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "persist", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Generic_PExpire(t *testing.T) {
	testName := "TestRedisProxy_Generic_PExpire"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.PExpire(context.TODO(), key, 1*time.Second)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "pexpire", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Generic_PExpireAt(t *testing.T) {
	testName := "TestRedisProxy_Generic_PExpireAt"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.PExpireAt(context.TODO(), key, time.Now())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "pexpire_at", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Generic_PExpireTime(t *testing.T) {
	testName := "TestRedisProxy_Generic_PExpireTime"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v7_0_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v7_0_0)
			}
			c.PExpireTime(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "pexpire_time", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Generic_Ping(t *testing.T) {
	testName := "TestRedisProxy_Generic_Ping"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Ping(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "ping", nil, prom.MetricsCatAll, prom.MetricsCatOther)
		})
	}
}

func TestRedisProxy_Generic_PTTL(t *testing.T) {
	testName := "TestRedisProxy_Generic_PTTL"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.PTTL(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "pttl", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Generic_RandomKey(t *testing.T) {
	testName := "TestRedisProxy_Generic_RandomKey"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.RandomKey(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "random_key", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Generic_Rename(t *testing.T) {
	testName := "TestRedisProxy_Generic_Rename"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Set(context.TODO(), key, "value", 0)
			c.Rename(context.TODO(), key, key+"new")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "rename", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Generic_RenameNX(t *testing.T) {
	testName := "TestRedisProxy_Generic_RenameNX"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Set(context.TODO(), key, "value", 0)
			c.RenameNX(context.TODO(), key, key+"new")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "rename_nx", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Generic_Restore(t *testing.T) {
	testName := "TestRedisProxy_Generic_Restore"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Set(context.TODO(), key, "value", 0)
			dumpResult := c.Dump(context.TODO(), key)
			c.Restore(context.TODO(), key+"new", 10*time.Second, dumpResult.Val())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "restore", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Generic_Scan(t *testing.T) {
	testName := "TestRedisProxy_Generic_Scan"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Scan(context.TODO(), 1234, key+"*", 2)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "scan", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Generic_Sort(t *testing.T) {
	testName := "TestRedisProxy_Generic_Sort"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Sort(context.TODO(), key, &redis.Sort{})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "sort", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Generic_Touch(t *testing.T) {
	testName := "TestRedisProxy_Generic_Touch"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Touch(context.TODO(), key+"1", key+"2", key+"3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "touch", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Generic_TTL(t *testing.T) {
	testName := "TestRedisProxy_Generic_TTL"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.TTL(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "ttl", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Generic_Type(t *testing.T) {
	testName := "TestRedisProxy_Generic_Type"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Type(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "type", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Generic_Unlink(t *testing.T) {
	testName := "TestRedisProxy_Generic_Unlink"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Unlink(context.TODO(), key+"1", key+"2", key+"3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "unlink", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Generic_Wait(t *testing.T) {
	testName := "TestRedisProxy_Generic_Wait"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, _ := _getRedisConnectAndCmdable(tc, key)
			switch tc {
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

			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "wait", nil, prom.MetricsCatAll, prom.MetricsCatOther)
		})
	}
}

/* Redis' geospatial-related commands */

func TestRedisProxy_GeoAdd(t *testing.T) {
	testName := "TestRedisProxy_GeoAdd"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.GeoAdd(context.TODO(), key, &redis.GeoLocation{Longitude: 13.361389, Latitude: 38.115556, Name: "Palermo"})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "geo_add", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_GeoDist(t *testing.T) {
	testName := "TestRedisProxy_GeoDist"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.GeoAdd(context.TODO(), key,
				&redis.GeoLocation{Longitude: 13.361389, Latitude: 38.115556, Name: "member1"},
				&redis.GeoLocation{Longitude: 15.087269, Latitude: 37.502669, Name: "member2"},
			)
			c.GeoDist(context.TODO(), key, "member1", "member2", "km")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "geo_dist", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_GeoHash(t *testing.T) {
	testName := "TestRedisProxy_GeoHash"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.GeoAdd(context.TODO(), key,
				&redis.GeoLocation{Longitude: 13.361389, Latitude: 38.115556, Name: "member1"},
				&redis.GeoLocation{Longitude: 15.087269, Latitude: 37.502669, Name: "member2"},
			)
			c.GeoHash(context.TODO(), key, "member1", "member2", "member3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "geo_hash", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_GeoPos(t *testing.T) {
	testName := "TestRedisProxy_GeoPos"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.GeoAdd(context.TODO(), key,
				&redis.GeoLocation{Longitude: 13.361389, Latitude: 38.115556, Name: "member1"},
				&redis.GeoLocation{Longitude: 15.087269, Latitude: 37.502669, Name: "member2"},
			)
			c.GeoPos(context.TODO(), key, "member1", "member2", "member3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "geo_pos", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_GeoRadius(t *testing.T) {
	testName := "TestRedisProxy_GeoRadius"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.GeoAdd(context.TODO(), key,
				&redis.GeoLocation{Longitude: 13.361389, Latitude: 38.115556, Name: "member1"},
				&redis.GeoLocation{Longitude: 15.087269, Latitude: 37.502669, Name: "member2"},
			)
			c.GeoRadius(context.TODO(), key, 13.361389, 38.115556, &redis.GeoRadiusQuery{Radius: 12.34})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "geo_radius", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_GeoRadiusByMember(t *testing.T) {
	testName := "TestRedisProxy_GeoRadiusByMember"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.GeoAdd(context.TODO(), key,
				&redis.GeoLocation{Longitude: 13.361389, Latitude: 38.115556, Name: "member1"},
				&redis.GeoLocation{Longitude: 15.087269, Latitude: 37.502669, Name: "member2"},
			)
			c.GeoRadiusByMember(context.TODO(), key, "member1", &redis.GeoRadiusQuery{Radius: 12.34})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "geo_radius", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_GeoSearch(t *testing.T) {
	testName := "TestRedisProxy_GeoSearch"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.GeoAdd(context.TODO(), key,
				&redis.GeoLocation{Longitude: 13.361389, Latitude: 38.115556, Name: "member1"},
				&redis.GeoLocation{Longitude: 15.087269, Latitude: 37.502669, Name: "member2"},
			)
			c.GeoSearch(context.TODO(), key, &redis.GeoSearchQuery{})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "geo_search", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_GeoSearchLocation(t *testing.T) {
	testName := "TestRedisProxy_GeoSearchLocation"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.GeoAdd(context.TODO(), key,
				&redis.GeoLocation{Longitude: 13.361389, Latitude: 38.115556, Name: "member1"},
				&redis.GeoLocation{Longitude: 15.087269, Latitude: 37.502669, Name: "member2"},
			)
			c.GeoSearchLocation(context.TODO(), key, &redis.GeoSearchLocationQuery{})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "geo_search", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_GeoSearchStore(t *testing.T) {
	testName := "TestRedisProxy_GeoSearchStore"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.GeoAdd(context.TODO(), key,
				&redis.GeoLocation{Longitude: 13.361389, Latitude: 38.115556, Name: "member1"},
				&redis.GeoLocation{Longitude: 15.087269, Latitude: 37.502669, Name: "member2"},
			)
			c.GeoSearchStore(context.TODO(), key, key+"store", &redis.GeoSearchStoreQuery{})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "geo_search_store", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

/* Redis' hash-related commands */

func TestRedisProxy_HDel(t *testing.T) {
	testName := "TestRedisProxy_HDel"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.HDel(context.TODO(), key, "field1", "field2", "field3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "hdel", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_HExists(t *testing.T) {
	testName := "TestRedisProxy_HExists"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.HExists(context.TODO(), key, "field")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "hexists", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_HGet(t *testing.T) {
	testName := "TestRedisProxy_HGet"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.HGet(context.TODO(), key, "field")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "hget", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_HGetAll(t *testing.T) {
	testName := "TestRedisProxy_HGetAll"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.HGetAll(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "hget_all", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_HIncrBy(t *testing.T) {
	testName := "TestRedisProxy_HIncrBy"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.HIncrBy(context.TODO(), key, "field", 12)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "hincr_by", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_HIncrByFloat(t *testing.T) {
	testName := "TestRedisProxy_HIncrByFloat"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.HIncrByFloat(context.TODO(), key, "field", 1.2)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "hincr_by_float", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_HKeys(t *testing.T) {
	testName := "TestRedisProxy_HKeys"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.HSet(context.TODO(), key, map[string]interface{}{"field1": "value1", "field2": 12, "field3": false})
			c.HKeys(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "hkeys", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_HLen(t *testing.T) {
	testName := "TestRedisProxy_HLen"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.HLen(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "hlen", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_HMGet(t *testing.T) {
	testName := "TestRedisProxy_HMGet"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.HMGet(context.TODO(), key, "field1", "field2", "field3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "hmget", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_HRandField(t *testing.T) {
	testName := "TestRedisProxy_HRandField"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.HRandField(context.TODO(), key, 2)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "hrand_field", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_HRandFieldWithValues(t *testing.T) {
	testName := "TestRedisProxy_HRandFieldWithValues"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.HRandFieldWithValues(context.TODO(), key, 2)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "hrand_field", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_HScan(t *testing.T) {
	testName := "TestRedisProxy_HScan"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.HScan(context.TODO(), key, 1234, "field*", 2)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "hscan", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_HSet(t *testing.T) {
	testName := "TestRedisProxy_HSet"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.HSet(context.TODO(), key, map[string]interface{}{"field1": "value1", "field2": 12, "field3": false})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "hset", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_HSetNX(t *testing.T) {
	testName := "TestRedisProxy_HSetNX"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.HSetNX(context.TODO(), key, "field", "value")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "hset_nx", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_HVals(t *testing.T) {
	testName := "TestRedisProxy_HVals"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.HSet(context.TODO(), key, map[string]interface{}{"field1": "value1", "field2": 12, "field3": false})
			c.HVals(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "hvals", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

/* Redis' hyper-log-log-related commands */

func TestRedisProxy_PFAdd(t *testing.T) {
	testName := "TestRedisProxy_PFAdd"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.PFAdd(context.TODO(), key, "value1", "value2", "value3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "pfadd", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_PFCount(t *testing.T) {
	testName := "TestRedisProxy_PFCount"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.PFCount(context.TODO(), key+"1", key+"2", key+"3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "pfcount", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_PFMerge(t *testing.T) {
	testName := "TestRedisProxy_PFMerge"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.PFMerge(context.TODO(), key+"dest", key+"1", key+"2", key+"3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "pfmerge", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

/* Redis' list-related commands */

func TestRedisProxy_List_BLMove(t *testing.T) {
	testName := "TestRedisProxy_List_BLMove"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.BLMove(context.TODO(), key+"src", key+"dest", "left", "right", 1*time.Second)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "blmove", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_List_BLMPop(t *testing.T) {
	testName := "TestRedisProxy_List_BLMPop"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v7_0_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v7_0_0)
			}
			c.BLMPop(context.TODO(), 1*time.Second, "LEFT", 2, key+"1", key+"2", key+"3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "blpop", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_List_BLPop(t *testing.T) {
	testName := "TestRedisProxy_List_BLPop"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.BLPop(context.TODO(), 1*time.Second, key+"1", key+"2", key+"3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "blpop", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_List_BRPop(t *testing.T) {
	testName := "TestRedisProxy_List_BRPop"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.BRPop(context.TODO(), 1*time.Second, key+"1", key+"2", key+"3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "brpop", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_List_LIndex(t *testing.T) {
	testName := "TestRedisProxy_List_LIndex"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.LIndex(context.TODO(), key, 12)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "lindex", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_List_LInsert(t *testing.T) {
	testName := "TestRedisProxy_List_LInsert"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.LInsert(context.TODO(), key, "BEFORE", "pivot", "value")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "linsert", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_List_LLen(t *testing.T) {
	testName := "TestRedisProxy_List_LLen"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.LLen(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "llen", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_List_LMove(t *testing.T) {
	testName := "TestRedisProxy_List_LMove"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.LMove(context.TODO(), key+"src", key+"dest", "left", "right")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "lmove", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_List_LPop(t *testing.T) {
	testName := "TestRedisProxy_List_LPop"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.LPop(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "lpop", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_List_LPos(t *testing.T) {
	testName := "TestRedisProxy_List_LPos"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.LPos(context.TODO(), key, "value", redis.LPosArgs{})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "lpos", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_List_LPush(t *testing.T) {
	testName := "TestRedisProxy_List_LPush"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.LPush(context.TODO(), key, "value1", 2, true)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "lpush", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_List_LPushX(t *testing.T) {
	testName := "TestRedisProxy_List_LPushX"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.LPushX(context.TODO(), key, "value1", 2, true)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "lpushx", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_List_LRange(t *testing.T) {
	testName := "TestRedisProxy_List_LRange"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.LRange(context.TODO(), key, 1, 3)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "lrange", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_List_LRem(t *testing.T) {
	testName := "TestRedisProxy_List_LRem"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.LRem(context.TODO(), key, 0, "value")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "lrem", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_List_LSet(t *testing.T) {
	testName := "TestRedisProxy_List_LSet"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.LPush(context.TODO(), key, "value0")
			c.LSet(context.TODO(), key, 0, "value")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "lset", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_List_LTrim(t *testing.T) {
	testName := "TestRedisProxy_List_LTrim"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.LTrim(context.TODO(), key, 0, 2)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "ltrim", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_List_RPop(t *testing.T) {
	testName := "TestRedisProxy_List_RPop"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.RPop(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "rpop", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_List_RPush(t *testing.T) {
	testName := "TestRedisProxy_List_RPush"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.RPush(context.TODO(), key, "value1", 2, true)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "rpush", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_List_RPushX(t *testing.T) {
	testName := "TestRedisProxy_List_RPushX"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.RPushX(context.TODO(), key, "value1", 2, true)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "rpushx", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

/* Redis' PubSub-related commands */

func TestRedisProxy_PubSub_PSubscribe(t *testing.T) {
	testName := "TestRedisProxy_PubSub_PSubscribe"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, _ := _getRedisConnectAndCmdable(tc, key)
			var pubsub *redis.PubSub
			defer func() {
				if pubsub != nil {
					pubsub.Close()
				}
			}()
			switch tc {
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
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "psubscribe", nil, prom.MetricsCatAll, prom.MetricsCatOther)
		})
	}
}

func TestRedisProxy_PubSub_Publish(t *testing.T) {
	testName := "TestRedisProxy_PubSub_Publish"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Publish(context.TODO(), "channel", "message")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "publish", nil, prom.MetricsCatAll, prom.MetricsCatOther)
		})
	}
}

func TestRedisProxy_PubSub_PubSubChannels(t *testing.T) {
	testName := "TestRedisProxy_PubSub_PubSubChannels"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.PubSubChannels(context.TODO(), "pattern")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "pubsub_channels", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_PubSub_PubSubNumPat(t *testing.T) {
	testName := "TestRedisProxy_PubSub_PubSubNumPat"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.PubSubNumPat(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "pubsub_num_pat", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_PubSub_PubSubNumSub(t *testing.T) {
	testName := "TestRedisProxy_PubSub_PubSubNumSub"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.PubSubNumSub(context.TODO(), "channel1", "channel2", "channel3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "pubsub_num_sub", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_PubSub_Subscribe(t *testing.T) {
	testName := "TestRedisProxy_PubSub_Subscribe"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, _ := _getRedisConnectAndCmdable(tc, key)
			var pubsub *redis.PubSub
			defer func() {
				if pubsub != nil {
					pubsub.Close()
				}
			}()
			switch tc {
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
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "subscribe", nil, prom.MetricsCatAll, prom.MetricsCatOther)
		})
	}
}

/* Redis' scripting-related commands */

func TestRedisProxy_Script_Eval(t *testing.T) {
	testName := "TestRedisProxy_Script_Eval"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Eval(context.TODO(), "return ARGV[1]", nil, "hello")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "eval", nil, prom.MetricsCatAll, prom.MetricsCatOther)
		})
	}
}

func TestRedisProxy_Script_EvalSha(t *testing.T) {
	testName := "TestRedisProxy_Script_EvalSha"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			scriptSha := c.ScriptLoad(context.TODO(), "return ARGV[1]")
			c.EvalSha(context.TODO(), scriptSha.Val(), nil, "hello")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "eval_sha", nil, prom.MetricsCatAll, prom.MetricsCatOther)
		})
	}
}

func TestRedisProxy_Script_ScriptExists(t *testing.T) {
	testName := "TestRedisProxy_Script_ScriptExists"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ScriptExists(context.TODO(), "sha1", "sha2", "sha3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "script_exists", nil, prom.MetricsCatAll, prom.MetricsCatOther)
		})
	}
}

func TestRedisProxy_Script_ScriptFlush(t *testing.T) {
	testName := "TestRedisProxy_Script_ScriptFlush"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ScriptFlush(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "script_flush", nil, prom.MetricsCatAll, prom.MetricsCatOther)
		})
	}
}

func TestRedisProxy_Script_ScriptKill(t *testing.T) {
	testName := "TestRedisProxy_Script_ScriptKill"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)
			if tc == "CLUSTER" {
				t.Skipf("%s skipped: ignore test on cluster", testName+"/"+tc)
			}

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			go func() {
				c.Eval(context.TODO(), "local i=1\nwhile (i > 0) do\ni=i+1\nend\nreturn i", []string{key})
			}()
			time.Sleep(100 * time.Millisecond)
			c.ScriptKill(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "script_kill", nil, prom.MetricsCatAll, prom.MetricsCatOther)
		})
	}
}

func TestRedisProxy_Script_ScriptLoad(t *testing.T) {
	testName := "TestRedisProxy_Script_ScriptLoad"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ScriptLoad(context.TODO(), "return ARGV[1]")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "script_load", nil, prom.MetricsCatAll, prom.MetricsCatOther)
		})
	}
}

/* Redis' server-related commands */

func TestRedisProxy_Server_DBSize(t *testing.T) {
	testName := "TestRedisProxy_Server_DBSize"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.DBSize(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "dbsize", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Server_FlushAll(t *testing.T) {
	testName := "TestRedisProxy_Server_FlushAll"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.FlushAll(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "flush_all", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Server_FlushAllAsync(t *testing.T) {
	testName := "TestRedisProxy_Server_FlushAllAsync"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.FlushAllAsync(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "flush_all", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Server_FlushDB(t *testing.T) {
	testName := "TestRedisProxy_Server_FlushDB"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.FlushDB(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "flush_db", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Server_FlushDBAsync(t *testing.T) {
	testName := "TestRedisProxy_Server_FlushDBAsync"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.FlushDBAsync(context.TODO())
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "flush_db", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

/* Redis' set-related commands */

func TestRedisProxy_Set_SAdd(t *testing.T) {
	testName := "TestRedisProxy_Set_SAdd"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.SAdd(context.TODO(), key, "value1", "value2", "value3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "sadd", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Set_SCard(t *testing.T) {
	testName := "TestRedisProxy_Set_SCard"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.SCard(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "scard", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Set_SDiff(t *testing.T) {
	testName := "TestRedisProxy_Set_SDiff"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.SDiff(context.TODO(), key+"1", key+"2")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "sdiff", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Set_SDiffStore(t *testing.T) {
	testName := "TestRedisProxy_Set_SDiffStore"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.SDiffStore(context.TODO(), key+"dest", key+"1", key+"2")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "sdiff_store", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Set_SInter(t *testing.T) {
	testName := "TestRedisProxy_Set_SInter"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.SInter(context.TODO(), key+"1", key+"2")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "sinter", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Set_SInterStore(t *testing.T) {
	testName := "TestRedisProxy_Set_SInterStore"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.SInterStore(context.TODO(), key+"dest", key+"1", key+"2")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "sinter_store", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Set_SIsMember(t *testing.T) {
	testName := "TestRedisProxy_Set_SIsMember"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.SIsMember(context.TODO(), key, "value")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "sis_member", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Set_SMembers(t *testing.T) {
	testName := "TestRedisProxy_Set_SMembers"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.SMembers(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "smembers", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Set_SMIsMember(t *testing.T) {
	testName := "TestRedisProxy_Set_SMIsMember"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.SMIsMember(context.TODO(), key, "value1", "value2", "value3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "smis_member", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Set_SMove(t *testing.T) {
	testName := "TestRedisProxy_Set_SMove"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.SMove(context.TODO(), key+"src", key+"dest", "value")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "smove", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Set_SPop(t *testing.T) {
	testName := "TestRedisProxy_Set_SPop"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.SPop(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "spop", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Set_SPopN(t *testing.T) {
	testName := "TestRedisProxy_Set_SPopN"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.SPopN(context.TODO(), key, 12)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "spop", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Set_SRandMember(t *testing.T) {
	testName := "TestRedisProxy_Set_SRandMember"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.SRandMember(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "srand_member", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Set_SRandMemberN(t *testing.T) {
	testName := "TestRedisProxy_Set_SRandMemberN"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.SRandMemberN(context.TODO(), key, 3)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "srand_member", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Set_SRem(t *testing.T) {
	testName := "TestRedisProxy_Set_SRem"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.SRem(context.TODO(), key, "value1", 2, true)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "srem", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Set_SScan(t *testing.T) {
	testName := "TestRedisProxy_Set_SScan"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.SScan(context.TODO(), key, 0, "pattern*", 2)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "sscan", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Set_SUnion(t *testing.T) {
	testName := "TestRedisProxy_Set_SUnion"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.SUnion(context.TODO(), key+"1", key+"2", key+"3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "sunion", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Set_SUnionStore(t *testing.T) {
	testName := "TestRedisProxy_Set_SUnionStore"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.SUnionStore(context.TODO(), key+"dest", key+"1", key+"2", key+"3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "sunion_store", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

/* Redis' sorted set-related commands */

func TestRedisProxy_SSet_BZMPop(t *testing.T) {
	testName := "TestRedisProxy_SSet_BZMPop"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v7_0_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v7_0_0)
			}
			c.BZMPop(context.TODO(), 10*time.Second, "MAX", 3, key+"1", key+"2", key+"3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "bzmpop", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_SSet_BZPopMax(t *testing.T) {
	testName := "TestRedisProxy_SSet_BZPopMax"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.BZPopMax(context.TODO(), 10*time.Second, key+"1", key+"2", key+"3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "bzpop_max", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_SSet_BZPopMin(t *testing.T) {
	testName := "TestRedisProxy_SSet_BZPopMin"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.BZPopMin(context.TODO(), 10*time.Second, key+"1", key+"2", key+"3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "bzpop_min", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_ZSSet_Add(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZAdd"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ZAdd(context.TODO(), key, redis.Z{Member: "member1", Score: 1.23}, redis.Z{Member: 2, Score: 2.34}, redis.Z{Member: true, Score: 3.45})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zadd", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_SSet_ZCard(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZCard"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ZCard(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zcard", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZCount(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZCount"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ZCount(context.TODO(), key, "1.23", "2.34")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zcount", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZDiff(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZDiff"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.ZDiff(context.TODO(), key+"1", key+"2", key+"3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zdiff", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZDiffWithScores(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZDiffWithScores"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.ZDiffWithScores(context.TODO(), key+"1", key+"2", key+"3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zdiff", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZDiffStore(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZDiffStore"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.ZDiffStore(context.TODO(), key+"dest", key+"1", key+"2", key+"3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zdiff_store", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_SSet_ZIncrBy(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZIncrBy"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ZIncrBy(context.TODO(), key, 1.23, "member")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zincr_by", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_SSet_ZInter(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZInter"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.ZInter(context.TODO(), &redis.ZStore{Keys: []string{key + "1", key + "2", key + "3"}})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zinter", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZInterWithScores(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZInterWithScores"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.ZInterWithScores(context.TODO(), &redis.ZStore{Keys: []string{key + "1", key + "2", key + "3"}})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zinter", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZInterStore(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZInterStore"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ZInterStore(context.TODO(), key+"dest", &redis.ZStore{Keys: []string{key + "1", key + "2", key + "3"}})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zinter_store", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_SSet_ZLexCount(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZLexCount"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ZLexCount(context.TODO(), key, "-", "+")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zlex_count", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZMScore(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZMScore"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.ZMScore(context.TODO(), key, "member1", "member2", "member3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zmscore", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZPopMax(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZPopMax"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ZPopMax(context.TODO(), key, 1)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zpop_max", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_SSet_ZPopMin(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZPopMin"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ZPopMin(context.TODO(), key, 1)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zpop_min", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_SSet_ZRandMember(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZRandMember"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.ZRandMember(context.TODO(), key, 1)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zrand_member", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZRange(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZRange"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ZRange(context.TODO(), key, 0, 10)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zrange", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZRangeWithScores(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZRangeWithScores"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ZRangeWithScores(context.TODO(), key, 0, 10)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zrange", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZRangeByLex(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZRangeByLex"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			cmdName := "zrange"
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				cmdName = "zrange_by_lex"
			}
			c.ZAdd(context.TODO(), key, redis.Z{Member: "a", Score: 0}, redis.Z{Member: "b", Score: 0}, redis.Z{Member: "c", Score: 0})
			c.ZRangeByLex(context.TODO(), key, &redis.ZRangeBy{Min: "(a", Max: "[c"})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, cmdName, nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZRangeByScore(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZRangeByScore"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			cmdName := "zrange"
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				cmdName = "zrange_by_score"
			}
			c.ZAdd(context.TODO(), key, redis.Z{Member: "a", Score: 1.2}, redis.Z{Member: "b", Score: 2.3}, redis.Z{Member: "c", Score: 3.4})
			c.ZRangeByScore(context.TODO(), key, &redis.ZRangeBy{Min: "1.23", Max: "2.34"})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, cmdName, nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZRangeByScoreWithScores(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZRangeByScoreWithScores"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			cmdName := "zrange"
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				cmdName = "zrange_by_score"
			}
			c.ZRangeByScoreWithScores(context.TODO(), key, &redis.ZRangeBy{Min: "1.23", Max: "2.34"})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, cmdName, nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZRangeStore(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZRangeStore"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.ZAdd(context.TODO(), key, redis.Z{Member: "one", Score: 1}, redis.Z{Member: "two", Score: 2}, redis.Z{Member: "three", Score: 3}, redis.Z{Member: "four", Score: 4})
			c.ZRangeStore(context.TODO(), key+"dest", redis.ZRangeArgs{Key: key, Start: "2", Stop: "-1"})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zrange_store", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_SSet_ZRank(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZRank"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ZRank(context.TODO(), key, "member")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zrank", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZRem(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZRem"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ZRem(context.TODO(), key, "member")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zrem", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_SSet_ZRemRangeByLex(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZRemRangeByLex"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ZRemRangeByLex(context.TODO(), key, "-", "+")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zrem_range_by_lex", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_SSet_ZRemRangeByRank(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZRemRangeByRank"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ZRemRangeByRank(context.TODO(), key, 2, -1)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zrem_range_by_rank", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_SSet_ZRemRangeByScore(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZRemRangeByScore"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ZRemRangeByScore(context.TODO(), key, "1.23", "2.34")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zrem_range_by_score", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_SSet_ZRevRange(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZRevRange"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			cmdName := "zrange"
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				cmdName = "zrev_range"
			}
			c.ZRevRange(context.TODO(), key, 2, -1)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, cmdName, nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZRevRangeWithScores(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZRevRangeWithScores"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			cmdName := "zrange"
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				cmdName = "zrev_range"
			}
			c.ZRevRangeWithScores(context.TODO(), key, 2, -1)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, cmdName, nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZRevRangeByLex(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZRevRangeByLex"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			cmdName := "zrange"
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				cmdName = "zrev_range_by_lex"
			}
			c.ZRevRangeByLex(context.TODO(), key, &redis.ZRangeBy{Min: "-", Max: "+"})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, cmdName, nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZRevRangeByScore(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZRevRangeByScore"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			cmdName := "zrange"
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				cmdName = "zrev_range_by_score"
			}
			c.ZRevRangeByScore(context.TODO(), "key", &redis.ZRangeBy{Min: "1.23", Max: "2.34"})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, cmdName, nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZRevRangeByScoreWithScores(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZRevRangeByScoreWithScores"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			cmdName := "zrange"
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				cmdName = "zrev_range_by_score"
			}
			c.ZRevRangeByScoreWithScores(context.TODO(), "key", &redis.ZRangeBy{Min: "1.23", Max: "2.34"})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, cmdName, nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZRevRank(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZRevRank"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ZRevRank(context.TODO(), key, "member")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zrev_rank", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZScan(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZScan"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ZScan(context.TODO(), key, 123, "pattern", 1)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zscan", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZScore(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZScore"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.ZScore(context.TODO(), key, "member")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zscore", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZUnion(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZUnion"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.ZUnion(context.TODO(), redis.ZStore{Keys: []string{key + "1", key + "2", key + "3"}})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zunion", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZUnionWithScores(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZUnionWithScores"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.ZUnionWithScores(context.TODO(), redis.ZStore{Keys: []string{key + "1", key + "2", key + "3"}})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zunion", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_SSet_ZUnionStore(t *testing.T) {
	testName := "TestRedisProxy_SSet_ZUnionStore"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.ZUnionStore(context.TODO(), key+"dest", &redis.ZStore{Keys: []string{key + "1", key + "2", key + "3"}})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "zunion_store", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

/* Redis' stream-related commands */

func TestRedisProxy_Stream_XAck(t *testing.T) {
	testName := "TestRedisProxy_Stream_XAck"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XAck(context.TODO(), key, "group", "1", "2", "3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xack", nil, prom.MetricsCatAll, prom.MetricsCatOther)
		})
	}
}

func TestRedisProxy_Stream_XAdd(t *testing.T) {
	testName := "TestRedisProxy_Stream_XAdd"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XAdd(context.TODO(), &redis.XAddArgs{Stream: key, Values: map[string]interface{}{"key": "value"}})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xadd", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Stream_XAutoClaim(t *testing.T) {
	testName := "TestRedisProxy_Stream_XAutoClaim"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.XGroupCreateMkStream(context.TODO(), key, "group", "0-0")
			c.XAutoClaim(context.TODO(), &redis.XAutoClaimArgs{Stream: key, Group: "group", Consumer: "consumer", MinIdle: 10 * time.Second, Start: "0-0"})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xauto_claim", nil, prom.MetricsCatAll, prom.MetricsCatOther)
		})
	}
}

func TestRedisProxy_Stream_XAutoClaimJustID(t *testing.T) {
	testName := "TestRedisProxy_Stream_XAutoClaimJustID"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.XGroupCreateMkStream(context.TODO(), key, "group", "0-0")
			c.XAutoClaimJustID(context.TODO(), &redis.XAutoClaimArgs{Stream: key, Group: "group", Consumer: "consumer", MinIdle: 10 * time.Second, Start: "0-0"})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xauto_claim", nil, prom.MetricsCatAll, prom.MetricsCatOther)
		})
	}
}

func TestRedisProxy_Stream_XDel(t *testing.T) {
	testName := "TestRedisProxy_Stream_XDel"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XDel(context.TODO(), key, "0-1", "0-2", "0-3")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xdel", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Stream_XGroupCreate(t *testing.T) {
	testName := "TestRedisProxy_Stream_XGroupCreate"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XAdd(context.TODO(), &redis.XAddArgs{Stream: key, Values: map[string]interface{}{"key": "value"}})
			c.XGroupCreate(context.TODO(), key, "group", "0-0")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xgroup_create", nil, prom.MetricsCatAll, prom.MetricsCatDDL)
		})
	}
}

func TestRedisProxy_Stream_XGroupCreateMkStream(t *testing.T) {
	testName := "TestRedisProxy_Stream_XGroupCreateMkStream"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XGroupCreateMkStream(context.TODO(), key, "group", "0-0")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xgroup_create", nil, prom.MetricsCatAll, prom.MetricsCatDDL)
		})
	}
}

func TestRedisProxy_Stream_XGroupCreateConsumer(t *testing.T) {
	testName := "TestRedisProxy_Stream_XGroupCreateConsumer"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.XGroupCreateMkStream(context.TODO(), key, "group", "0-0")
			c.XGroupCreateConsumer(context.TODO(), key, "group", "consumer")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xgroup_create_consumer", nil, prom.MetricsCatAll, prom.MetricsCatOther)
		})
	}
}

func TestRedisProxy_Stream_XGroupDelConsumer(t *testing.T) {
	testName := "TestRedisProxy_Stream_XGroupDelConsumer"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XGroupCreateMkStream(context.TODO(), key, "group", "0-0")
			c.XGroupDelConsumer(context.TODO(), key, "group", "consumer")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xgroup_del_consumer", nil, prom.MetricsCatAll, prom.MetricsCatOther)
		})
	}
}

func TestRedisProxy_Stream_XGroupDestroy(t *testing.T) {
	testName := "TestRedisProxy_Stream_XGroupDestroy"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XGroupCreateMkStream(context.TODO(), key, "group", "0-0")
			c.XGroupDestroy(context.TODO(), key, "group")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xgroup_destroy", nil, prom.MetricsCatAll, prom.MetricsCatDDL)
		})
	}
}

func TestRedisProxy_Stream_XGroupSetID(t *testing.T) {
	testName := "TestRedisProxy_Stream_XGroupSetID"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XGroupCreateMkStream(context.TODO(), key, "group", "0-0")
			c.XGroupSetID(context.TODO(), key, "group", "0-0")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xgroup_set_id", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Stream_XInfoConsumers(t *testing.T) {
	testName := "TestRedisProxy_Stream_XInfoConsumers"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XGroupCreateMkStream(context.TODO(), key, "group", "0-0")
			c.XInfoConsumers(context.TODO(), key, "group")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xinfo_consumers", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Stream_XInfoGroups(t *testing.T) {
	testName := "TestRedisProxy_Stream_XInfoGroups"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XGroupCreateMkStream(context.TODO(), key, "group", "0-0")
			c.XInfoGroups(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xinfo_groups", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Stream_XInfoStream(t *testing.T) {
	testName := "TestRedisProxy_Stream_XInfoStream"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XGroupCreateMkStream(context.TODO(), key, "group", "0-0")
			c.XInfoStream(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xinfo_stream", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Stream_XInfoStreamFull(t *testing.T) {
	testName := "TestRedisProxy_Stream_XInfoStreamFull"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XGroupCreateMkStream(context.TODO(), key, "group", "0-0")
			c.XInfoStreamFull(context.TODO(), key, 12)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xinfo_stream", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Stream_XLen(t *testing.T) {
	testName := "TestRedisProxy_Stream_XLen"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XLen(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xlen", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Stream_XPending(t *testing.T) {
	testName := "TestRedisProxy_Stream_XPending"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XGroupCreateMkStream(context.TODO(), key, "group", "0-0")
			c.XPending(context.TODO(), key, "group")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xpending", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Stream_XPendingExt(t *testing.T) {
	testName := "TestRedisProxy_Stream_XPendingExt"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XGroupCreateMkStream(context.TODO(), key, "group", "0-0")
			c.XPendingExt(context.TODO(), &redis.XPendingExtArgs{Stream: key, Group: "group", Start: "0-0", End: "0-1", Count: 1})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xpending", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Stream_XRange(t *testing.T) {
	testName := "TestRedisProxy_Stream_XRange"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XRange(context.TODO(), key, "0-0", "0-1")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xrange", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Stream_XRangeN(t *testing.T) {
	testName := "TestRedisProxy_Stream_XRangeN"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XRangeN(context.TODO(), key, "0-0", "0-1", 12)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xrange", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Stream_XRead(t *testing.T) {
	testName := "TestRedisProxy_Stream_XRead"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XRead(context.TODO(), &redis.XReadArgs{Count: 1, Block: 10 * time.Second, Streams: []string{key, "0-0"}})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xread", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Stream_XReadGroup(t *testing.T) {
	testName := "TestRedisProxy_Stream_XReadGroup"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XGroupCreateMkStream(context.TODO(), key, "group", "0-0")
			c.XReadGroup(context.TODO(), &redis.XReadGroupArgs{Group: "group", Count: 1, Block: 10 * time.Second, Streams: []string{key, "0-0"}})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xread_group", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Stream_XRevRange(t *testing.T) {
	testName := "TestRedisProxy_Stream_XRevRange"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XRevRange(context.TODO(), key, "0-0", "0-1")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xrev_range", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Stream_XRevRangeN(t *testing.T) {
	testName := "TestRedisProxy_Stream_XRevRangeN"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XRevRangeN(context.TODO(), key, "0-0", "0-1", 12)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xrev_range", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_Stream_XTrimMaxLen(t *testing.T) {
	testName := "TestRedisProxy_Stream_XTrimMaxLen"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XTrimMaxLen(context.TODO(), key, 12)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xtrim", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Stream_XTrimMaxLenApprox(t *testing.T) {
	testName := "TestRedisProxy_Stream_XTrimMaxLenApprox"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XTrimMaxLenApprox(context.TODO(), key, 12, 2)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xtrim", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Stream_XTrimMinID(t *testing.T) {
	testName := "TestRedisProxy_Stream_XTrimMinID"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XTrimMinID(context.TODO(), key, "0-0")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xtrim", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_Stream_XTrimMinIDApprox(t *testing.T) {
	testName := "TestRedisProxy_Stream_XTrimMinIDApprox"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.XTrimMinIDApprox(context.TODO(), key, "0-0", 2)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "xtrim", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

/* Redis' string-related commands */

func TestRedisProxy_String_Append(t *testing.T) {
	testName := "TestRedisProxy_String_Append"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Append(context.TODO(), key, "value")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "append", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_String_Decr(t *testing.T) {
	testName := "TestRedisProxy_String_Decr"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Decr(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "decr", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_String_DecrBy(t *testing.T) {
	testName := "TestRedisProxy_String_Decrby"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.DecrBy(context.TODO(), key, 12)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "decr_by", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_String_Get(t *testing.T) {
	testName := "TestRedisProxy_String_Get"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Get(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "get", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_String_GetDel(t *testing.T) {
	testName := "TestRedisProxy_String_GetDel"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.GetDel(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "get_del", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_String_GetEx(t *testing.T) {
	testName := "TestRedisProxy_String_GetEx"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			ver := _getRedisVersion(c)
			if ver.Compare(v6_2_0) < 0 {
				t.Skipf("%s skipped: Redis version %s does support the specified command, need version %s", testName+"/"+tc, ver, v6_2_0)
			}
			c.GetEx(context.TODO(), key, 10*time.Second)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "get_ex", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_String_GetRange(t *testing.T) {
	testName := "TestRedisProxy_String_GetRange"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.GetRange(context.TODO(), key, 0, 10)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "get_range", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_String_GetSet(t *testing.T) {
	testName := "TestRedisProxy_String_GetSet"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.GetSet(context.TODO(), key, "value")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "get_set", []error{redis.Nil}, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_String_Incr(t *testing.T) {
	testName := "TestRedisProxy_String_Incr"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Incr(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "incr", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_String_IncrBy(t *testing.T) {
	testName := "TestRedisProxy_String_IncrBy"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.IncrBy(context.TODO(), key, 12)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "incr_by", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_String_IncrByFloat(t *testing.T) {
	testName := "TestRedisProxy_String_IncrByFloat"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.IncrByFloat(context.TODO(), key, 1.2)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "incr_by_float", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_String_MGet(t *testing.T) {
	testName := "TestRedisProxy_String_MGet"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.MGet(context.TODO(), key+"1", key+"3", key+"2")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "mget", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

func TestRedisProxy_String_MSet(t *testing.T) {
	testName := "TestRedisProxy_String_MSet"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.MSet(context.TODO(), map[string]interface{}{key + "1": "value1", key + "2": 2, key + "3": true})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "mset", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_String_MSetNX(t *testing.T) {
	testName := "TestRedisProxy_String_MSetNX"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "{key}"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.MSetNX(context.TODO(), map[string]interface{}{key + "1": "value1", key + "2": 2, key + "3": true})
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "mset_nx", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_String_Set(t *testing.T) {
	testName := "TestRedisProxy_String_Set"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.Set(context.TODO(), key, "value", 10*time.Second)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "set", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_String_SetEX(t *testing.T) {
	testName := "TestRedisProxy_String_SetEX"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.SetEx(context.TODO(), key, "value", 10*time.Second)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "set_ex", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_String_SetNX(t *testing.T) {
	testName := "TestRedisProxy_String_SetNX"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.SetNX(context.TODO(), key, "value", 10*time.Second)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "set_nx", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_String_SetRange(t *testing.T) {
	testName := "TestRedisProxy_String_SetRange"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.SetRange(context.TODO(), key, 10, "value")
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "set_range", nil, prom.MetricsCatAll, prom.MetricsCatDML)
		})
	}
}

func TestRedisProxy_String_StrLen(t *testing.T) {
	testName := "TestRedisProxy_String_StrLen"
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
			defer teardownTest(t)

			key := "key"
			rc, c := _getRedisConnectAndCmdable(tc, key)
			c.StrLen(context.TODO(), key)
			_rcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName+"/"+tc, rc, "strlen", nil, prom.MetricsCatAll, prom.MetricsCatDQL)
		})
	}
}

/* Redis' transaction-related commands */
