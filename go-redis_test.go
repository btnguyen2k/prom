package prom

import (
	"testing"
)

func newGoRedisConnect(hostsAndPorts, password string) *GoRedisConnect {
	return NewGoRedisConnect(hostsAndPorts, hostsAndPorts, 3)
}

func TestNewGoRedisConnect(t *testing.T) {
	name := "TestNewGoRedisConnect"
	rc := newGoRedisConnect("localhost:6379", "")
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
	rc := newGoRedisConnect("localhost:6379", "")
	defer rc.Close()
	client := rc.GetClient(0)
	if client == nil {
		t.Fatalf("%s failed", name)
	}
}

func TestGoRedisConnect_GetFailoverClient(t *testing.T) {
	name := "TestGoRedisConnect_GetFailoverClient"
	rc := newGoRedisConnect("localhost:6379", "")
	defer rc.Close()
	client := rc.GetFailoverClient(0)
	if client == nil {
		t.Fatalf("%s failed", name)
	}
}

func TestGoRedisConnect_GetClusterClient(t *testing.T) {
	name := "TestGoRedisConnect_GetClusterClient"
	rc := newGoRedisConnect("localhost:6379", "")
	defer rc.Close()
	client := rc.GetClusterClient()
	if client == nil {
		t.Fatalf("%s failed", name)
	}
}
