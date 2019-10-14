package prom

import "testing"

const (
	_testRedisHostsAndPorts = "localhost:6379"
	_testRedisPassword      = ""
)

func _newGoRedisConnect() *GoRedisConnect {
	return NewGoRedisConnect(_testRedisHostsAndPorts, _testRedisPassword, 3)
}

func TestNewGoRedisConnect(t *testing.T) {
	name := "TestNewGoRedisConnect"
	rc := _newGoRedisConnect()
	if rc == nil {
		t.Fatalf("%s failed", name)
	}
}

func TestGoRedisConnect_GetClient(t *testing.T) {
	name := "TestGoRedisConnect_GetClient"
	rc := _newGoRedisConnect()

	client := rc.GetClient(0)
	if client == nil {
		t.Fatalf("%s failed", name)
	}
}

func TestGoRedisConnect_GetFailoverClient(t *testing.T) {
	name := "TestGoRedisConnect_GetFailoverClient"
	rc := _newGoRedisConnect()

	client := rc.GetFailoverClient(0)
	if client == nil {
		t.Fatalf("%s failed", name)
	}
}

func TestGoRedisConnect_GetClusterClient(t *testing.T) {
	name := "TestGoRedisConnect_GetClusterClient"
	rc := _newGoRedisConnect()

	client := rc.GetClusterClient()
	if client == nil {
		t.Fatalf("%s failed", name)
	}
}
