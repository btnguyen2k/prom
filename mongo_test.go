package prom

import (
	"testing"
	"time"
)

const (
	_testMongoUrl        = "mongodb://test:test@localhost:27017/test"
	_testMongoDb         = "test"
	_testMongoCollection = "test"
)

func _newMongoConnect() *MongoConnect {
	mc, err := NewMongoConnect(_testMongoUrl, _testMongoDb, 10000)
	if err != nil {
		panic(err)
	}
	return mc
}

func TestNewMongoConnect(t *testing.T) {
	name := "TestNewMongoConnect"
	mc := _newMongoConnect()
	if mc == nil || mc.GetDatabase() == nil {
		t.Fatalf("%s failed", name)
	}
}

func TestMongoConnect_GetDatabase(t *testing.T) {
	name := "TestMongoConnect_GetDatabase"
	mc := _newMongoConnect()
	db := mc.GetDatabase()
	if db == nil || mc.GetDatabase().Name() != _testMongoDb {
		t.Fatalf("%s failed", name)
	}
}

func TestMongoConnect_HasDatabase(t *testing.T) {
	name := "TestMongoConnect_HasDatabase"
	mc := _newMongoConnect()
	ok, err := mc.HasDatabase(_testMongoDb)
	if !ok || err != nil {
		t.Fatalf("%s failed: %v - %e", name, ok, err)
	}
}

func TestMongoConnect_NewContext(t *testing.T) {
	name := "TestMongoConnect_NewContext"
	mc := _newMongoConnect()
	now := time.Now()
	context, _ := mc.NewContext(10000)
	if context == nil {
		t.Fatalf("%s failed: NewContext returns nil", name)
	}
	deadline, ok := context.Deadline()
	if !ok || deadline.Unix() < now.Unix()+10 {
		t.Fatalf("%s failed - expected deadline [%#v], expected [%#v]", name, now.Add(10000).Unix(), deadline.Unix())
	}
}

func TestMongoConnect_CreateCollection(t *testing.T) {
	name := "TestMongoConnect_CreateCollection"
	mc := _newMongoConnect()
	err := mc.GetCollection(_testMongoCollection).Drop(nil)
	if err != nil {
		t.Fatalf("%s failed - error dropping collection [%s]: %e", name, _testMongoCollection, err)
	}
	ok, err := mc.HasCollection(_testMongoCollection)
	if ok || err != nil {
		t.Fatalf("%s failed: %v - %e", name, ok, err)
	}
	_, err = mc.CreateCollection(_testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed - error creating collection [%s]: %e", name, _testMongoCollection, err)
	}
	ok, err = mc.HasCollection(_testMongoCollection)
	if !ok || err != nil {
		t.Fatalf("%s failed: %v - %e", name, ok, err)
	}
}

func TestMongoConnect_CreateIndexes(t *testing.T) {
	name := "TestMongoConnect_CreateIndexes"
	mc := _newMongoConnect()
	err := mc.GetCollection(_testMongoCollection).Drop(nil)
	if err != nil {
		t.Fatalf("%s failed - error dropping collection [%s]: %e", name, _testMongoCollection, err)
	}
	_, err = mc.CreateCollection(_testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed - error creating collection [%s]: %e", name, _testMongoCollection, err)
	}
	indexes := []interface{}{
		map[string]interface{}{
			"key": map[string]interface{}{
				"id": 1, // ascending index
			},
			"name":   "uidx_id",
			"unique": true,
		},
		map[string]interface{}{
			"key": map[string]interface{}{
				"email": -1, // descending index
			},
			"name": "uidx_email",
		},
	}
	_, err = mc.CreateIndexes(_testMongoCollection, indexes)
	if err != nil {
		t.Fatalf("%s failed - error creating indexes: %e", name, err)
	}
}
