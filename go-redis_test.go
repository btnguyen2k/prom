package prom

import (
	"strings"
	"testing"
	"time"
)

func TestNewGoRedisConnectClose(t *testing.T) {
	name := "TestNewGoRedisConnectClose"
	rc := NewGoRedisConnect("localhost", "", 3)
	if rc == nil {
		t.Fatalf("%s failed", name)
	}
	err := rc.Close()
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}
}

func TestGoRedisConnect_GetClient(t *testing.T) {
	name := "TestGoRedisConnect_GetClient"
	rc := NewGoRedisConnect("localhost", "", -1)
	defer rc.Close()
	client := rc.GetClient(-1)
	if client == nil {
		t.Fatalf("%s failed", name)
	}
}

func TestGoRedisConnect_GetFailoverClient(t *testing.T) {
	name := "TestGoRedisConnect_GetFailoverClient"
	rc := NewGoRedisConnect("localhost", "", -1)
	defer rc.Close()
	client := rc.GetFailoverClient(-1)
	if client == nil {
		t.Fatalf("%s failed", name)
	}
}

func TestGoRedisConnect_GetClusterClient(t *testing.T) {
	name := "TestGoRedisConnect_GetClusterClient"
	rc := NewGoRedisConnect("localhost", "", -1)
	defer rc.Close()
	client := rc.GetClusterClient()
	if client == nil {
		t.Fatalf("%s failed", name)
	}
}

func TestGoRedisConnect_GetSlaveReadOnly(t *testing.T) {
	name := "TestGoRedisConnect_GetSlaveReadOnly"
	rc := NewGoRedisConnect("localhost", "", 3)
	defer rc.Close()
	rc.SetSlaveReadOnly(true)
	if !rc.GetSlaveReadOnly() {
		t.Fatalf("%s failed: should be true", name)
	}
	rc.SetSlaveReadOnly(false)
	if rc.GetSlaveReadOnly() {
		t.Fatalf("%s failed: should be false", name)
	}
}

func TestGoRedisConnect_GetSentinelMasterName(t *testing.T) {
	name := "TestGoRedisConnect_GetSentinelMasterName"
	rc := NewGoRedisConnect("localhost", "", 3)
	defer rc.Close()
	rc.SetSentinelMasterName("mymastername")
	if rc.GetSentinelMasterName() != "mymastername" {
		t.Fatalf("%s failed: expected %#v but received %#v", name, "mymastername", rc.GetSentinelMasterName())
	}
}

func TestGoRedisConnect_GetRedisPoolOpts(t *testing.T) {
	name := "TestGoRedisConnect_GetRedisPoolOpts"
	rc := NewGoRedisConnect("localhost", "", 3)
	defer rc.Close()
	if rc.GetRedisPoolOpts() == nil {
		t.Fatalf("%s failed: nil", name)
	}
}

func _newGoRedisConnect(t *testing.T, testName, redisHostsAndPorts, redisPassword string, maxRetries int) *GoRedisConnect {
	// redisHostsAndPorts := strings.ReplaceAll(os.Getenv("REDIS_HOSTS_AND_PORTS"), `"`, "")
	// if redisHostsAndPorts == "" {
	// 	t.Skipf("%s skipped", testName)
	// 	return nil
	// }
	// redisPassword := strings.ReplaceAll(os.Getenv("REDIS_PASSWORD"), `"`, "")
	redisHostsAndPorts = strings.ReplaceAll(redisHostsAndPorts, `"`, "")
	if redisHostsAndPorts == "" {
		t.Skipf("%s skipped", testName)
		return nil
	}
	redisPassword = strings.ReplaceAll(redisPassword, `"`, "")
	return NewGoRedisConnect(redisHostsAndPorts, redisPassword, maxRetries)
}

func TestGoRedis_FastFailed_Client(t *testing.T) {
	name := "TestGoRedis_FastFailed_Client"
	rc := _newGoRedisConnect(t, name, "localhost:1234", "", 0)
	defer rc.Close()

	rc.SetRedisPoolOpts(&RedisPoolOpts{
		PoolSize:     1,
		MinIdleConns: 0,
		DialTimeout:  10 * time.Millisecond,
		ReadTimeout:  20 * time.Millisecond,
		WriteTimeout: 20 * time.Millisecond,
	})
	client := rc.GetClient(0)
	tstart := time.Now()
	result := client.Ping()
	if result.Err() == nil {
		t.Fatalf("%s failed: the operation should not success", name)
	}
	d := time.Duration(time.Now().UnixNano() - tstart.UnixNano())
	dmax := 20 * time.Millisecond
	if d > dmax {
		t.Fatalf("%s failed: operation is expected to fail within %#v ms but in fact %#v ms", name, dmax/1E6, d/1E6)
	}
}

func TestGoRedis_FastFailed_FailoverClient(t *testing.T) {
	name := "TestGoRedis_FastFailed_FailoverClient"
	rc := _newGoRedisConnect(t, name, "localhost:1234", "", 0)
	defer rc.Close()

	rc.SetRedisPoolOpts(&RedisPoolOpts{
		PoolSize:     1,
		MinIdleConns: 0,
		DialTimeout:  10 * time.Millisecond,
		ReadTimeout:  20 * time.Millisecond,
		WriteTimeout: 20 * time.Millisecond,
	})
	client := rc.GetFailoverClient(0)
	tstart := time.Now()
	result := client.Ping()
	if result.Err() == nil {
		t.Fatalf("%s failed: the operation should not success", name)
	}
	d := time.Duration(time.Now().UnixNano() - tstart.UnixNano())
	dmax := 20 * time.Millisecond
	if d > dmax {
		t.Fatalf("%s failed: operation is expected to fail within %#v ms but in fact %#v ms", name, dmax/1E6, d/1E6)
	}
}

func TestGoRedis_FastFailed_GetClusterClient(t *testing.T) {
	name := "TestGoRedis_FastFailed_GetClusterClient"
	rc := _newGoRedisConnect(t, name, "localhost:1234", "", 0)
	defer rc.Close()

	rc.SetRedisPoolOpts(&RedisPoolOpts{
		PoolSize:     1,
		MinIdleConns: 0,
		DialTimeout:  10 * time.Millisecond,
		ReadTimeout:  20 * time.Millisecond,
		WriteTimeout: 20 * time.Millisecond,
	})
	client := rc.GetClusterClient()
	tstart := time.Now()
	result := client.Ping()
	if result.Err() == nil {
		t.Fatalf("%s failed: the operation should not success", name)
	}
	d := time.Duration(time.Now().UnixNano() - tstart.UnixNano())
	dmax := 20 * time.Millisecond
	if d > dmax {
		t.Fatalf("%s failed: operation is expected to fail within %#v ms but in fact %#v ms", name, dmax/1E6, d/1E6)
	}
}
