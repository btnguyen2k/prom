package goredis

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestNewGoRedisConnectClose(t *testing.T) {
	testName := "TestNewGoRedisConnectClose"
	rc, err := NewGoRedisConnect("localhost", "", 3)
	if rc == nil || err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	err = rc.Close()
	if err != nil {
		t.Fatalf("%s failed: error [%e]", testName, err)
	}
}

func TestGoRedisConnect_GetClient(t *testing.T) {
	testName := "TestGoRedisConnect_GetClient"
	rc, err := NewGoRedisConnect("localhost", "", 3)
	if rc == nil || err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	defer rc.Close()
	client := rc.GetClient(-1)
	if client == nil {
		t.Fatalf("%s failed", testName)
	}
}

func TestGoRedisConnect_GetFailoverClient(t *testing.T) {
	testName := "TestGoRedisConnect_GetFailoverClient"
	rc, err := NewGoRedisConnect("localhost", "", 3)
	if rc == nil || err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	defer rc.Close()
	client := rc.GetFailoverClient(-1)
	if client == nil {
		t.Fatalf("%s failed", testName)
	}
}

func TestGoRedisConnect_GetClusterClient(t *testing.T) {
	testName := "TestGoRedisConnect_GetClusterClient"
	rc, err := NewGoRedisConnect("localhost", "", 3)
	if rc == nil || err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	defer rc.Close()
	client := rc.GetClusterClient()
	if client == nil {
		t.Fatalf("%s failed", testName)
	}
}

func TestGoRedisConnect_GetSlaveReadOnly(t *testing.T) {
	testName := "TestGoRedisConnect_GetSlaveReadOnly"
	rc, err := NewGoRedisConnect("localhost", "", 3)
	if rc == nil || err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	defer rc.Close()
	rc.SetSlaveReadOnly(true)
	if !rc.GetSlaveReadOnly() {
		t.Fatalf("%s failed: should be true", testName)
	}
	rc.SetSlaveReadOnly(false)
	if rc.GetSlaveReadOnly() {
		t.Fatalf("%s failed: should be false", testName)
	}
}

func TestGoRedisConnect_GetSentinelMasterName(t *testing.T) {
	testName := "TestGoRedisConnect_GetSentinelMasterName"
	rc, err := NewGoRedisConnect("localhost", "", 3)
	if rc == nil || err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	defer rc.Close()
	rc.SetSentinelMasterName("mymastername")
	if rc.GetSentinelMasterName() != "mymastername" {
		t.Fatalf("%s failed: expected %#v but received %#v", testName, "mymastername", rc.GetSentinelMasterName())
	}
}

func TestGoRedisConnect_GetRedisPoolOpts(t *testing.T) {
	testName := "TestGoRedisConnect_GetRedisPoolOpts"
	rc, err := NewGoRedisConnect("localhost", "", 3)
	if rc == nil || err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	defer rc.Close()
	if rc.GetRedisPoolOpts() == nil {
		t.Fatalf("%s failed: nil", testName)
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
	rc, err := NewGoRedisConnect(redisHostsAndPorts, redisPassword, maxRetries)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	return rc
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
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Millisecond)
	result := client.Ping(ctx)
	if result.Err() == nil {
		t.Fatalf("%s failed: the operation should not success", name)
	}
	d := time.Now().Sub(tstart)
	dmax := 31 * time.Millisecond
	if d > dmax {
		t.Fatalf("%s failed: operation is expected to fail within %#v ms but in fact %#v ms", name, dmax/1e6, d/1e6)
	}
}

// func TestGoRedis_FastFailed_FailoverClient(t *testing.T) {
// 	name := "TestGoRedis_FastFailed_FailoverClient"
// 	rc := _newGoRedisConnect(t, name, "localhost:1234", "", 0)
// 	defer rc.Close()
//
// 	rc.SetRedisPoolOpts(&RedisPoolOpts{
// 		PoolSize:     1,
// 		MinIdleConns: 0,
// 		DialTimeout:  10 * time.Millisecond,
// 		ReadTimeout:  20 * time.Millisecond,
// 		WriteTimeout: 20 * time.Millisecond,
// 	})
// 	client := rc.GetFailoverClient(0)
// 	tstart := time.Now()
// 	ctx, _ := context.WithTimeout(context.Background(), 10*time.Millisecond)
// 	result := client.Ping(ctx)
// 	if result.Err() == nil {
// 		t.Fatalf("%s failed: the operation should not success", name)
// 	}
// 	d := time.Duration(time.Now().UnixNano() - tstart.UnixNano())
// 	dmax := 21 * time.Millisecond
// 	if d > dmax {
// 		t.Fatalf("%s failed: operation is expected to fail within %#v ms but in fact %#v ms", name, dmax/1e6, d/1e6)
// 	}
// }

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
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Millisecond)
	result := client.Ping(ctx)
	if result.Err() == nil {
		t.Fatalf("%s failed: the operation should not success", name)
	}
	d := time.Now().Sub(tstart)
	dmax := 21 * time.Millisecond
	if d > dmax {
		t.Fatalf("%s failed: operation is expected to fail within %#v ms but in fact %#v ms", name, dmax/1e6, d/1e6)
	}
}