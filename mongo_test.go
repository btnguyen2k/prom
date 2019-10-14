package prom

import (
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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

func TestMongoConnect_CreateIndexes1(t *testing.T) {
	name := "TestMongoConnect_CreateIndexes1"
	mc := _newMongoConnect()

	fieldMapNamespace := "ns"
	fieldMapFrom := "frm"
	fieldMapTo := "to"
	indexes := []interface{}{
		map[string]interface{}{
			"key": map[string]interface{}{
				fieldMapNamespace: 1,
				fieldMapFrom:      1,
			},
			"name":   "uidx_from",
			"unique": true,
		},
		map[string]interface{}{
			"key": map[string]interface{}{
				fieldMapNamespace: 1,
				fieldMapTo:        1,
			},
			"name": "idx_to",
		},
	}
	for i := 0; i < 10; i++ {
		err := mc.GetCollection(_testMongoCollection).Drop(nil)
		if err != nil {
			t.Fatalf("[Loop %d] %s failed - error dropping collection [%s]: %e", i, name, _testMongoCollection, err)
		}
		colResult, err := mc.CreateCollection(_testMongoCollection)
		if err != nil || colResult.Err() != nil {
			if err != nil {
				t.Fatalf("[Loop %d] %s failed - error creating collection [%s]: %e", i, name, _testMongoCollection, err)
			} else {
				t.Fatalf("[Loop %d] %s failed - error creating collection [%s]: %e", i, name, _testMongoCollection, colResult.Err())
			}
		}
		idxResult, err := mc.CreateCollectionIndexes(_testMongoCollection, indexes)
		if err != nil || idxResult == nil || len(idxResult) < 1 {
			if err != nil {
				t.Fatalf("[Loop %d] %s failed - error creating indexes: %e", i, name, err)
			} else {
				t.Fatalf("[Loop %d] %s failed - error creating indexes: %#v", i, name, idxResult)
			}
		}
	}
}

func TestMongoConnect_CreateIndexes2(t *testing.T) {
	name := "TestMongoConnect_CreateIndexes2"
	mc := _newMongoConnect()

	fieldMapNamespace := "ns"
	fieldMapFrom := "frm"
	fieldMapTo := "to"
	idxName := "idx_to"
	indexes := []interface{}{
		map[string]interface{}{
			"key": map[string]interface{}{
				fieldMapNamespace: 1,
				fieldMapFrom:      1,
			},
			"name":   "uidx_from",
			"unique": true,
		},
		mongo.IndexModel{
			Keys: map[string]interface{}{
				fieldMapNamespace: 1,
				fieldMapTo:        1,
			},
			Options: &options.IndexOptions{Name: &idxName},
		},
	}
	for i := 0; i < 10; i++ {
		err := mc.GetCollection(_testMongoCollection).Drop(nil)
		if err != nil {
			t.Fatalf("[Loop %d] %s failed - error dropping collection [%s]: %e", i, name, _testMongoCollection, err)
		}
		colResult, err := mc.CreateCollection(_testMongoCollection)
		if err != nil || colResult.Err() != nil {
			if err != nil {
				t.Fatalf("[Loop %d] %s failed - error creating collection [%s]: %e", i, name, _testMongoCollection, err)
			} else {
				t.Fatalf("[Loop %d] %s failed - error creating collection [%s]: %e", i, name, _testMongoCollection, colResult.Err())
			}
		}
		idxResult, err := mc.CreateCollectionIndexes(_testMongoCollection, indexes)
		if err != nil || idxResult == nil || len(idxResult) < 1 {
			if err != nil {
				t.Fatalf("[Loop %d] %s failed - error creating indexes: %e", i, name, err)
			} else {
				t.Fatalf("[Loop %d] %s failed - error creating indexes: %#v", i, name, idxResult)
			}
		}
	}
}

func TestMongoConnect_CreateIndexes3(t *testing.T) {
	name := "TestMongoConnect_CreateIndexes3"
	mc := _newMongoConnect()

	fieldMapNamespace := "ns"
	fieldMapFrom := "frm"
	fieldMapTo := "to"
	uidxName := "uidx_from"
	isUnique := true
	idxName := "idx_to"
	indexes := []interface{}{
		mongo.IndexModel{
			Keys: map[string]interface{}{
				fieldMapNamespace: 1,
				fieldMapFrom:      1,
			},
			Options: &options.IndexOptions{Name: &uidxName, Unique: &isUnique},
		},
		mongo.IndexModel{
			Keys: map[string]interface{}{
				fieldMapNamespace: 1,
				fieldMapTo:        1,
			},
			Options: &options.IndexOptions{Name: &idxName},
		},
	}
	for i := 0; i < 10; i++ {
		err := mc.GetCollection(_testMongoCollection).Drop(nil)
		if err != nil {
			t.Fatalf("[Loop %d] %s failed - error dropping collection [%s]: %e", i, name, _testMongoCollection, err)
		}
		colResult, err := mc.CreateCollection(_testMongoCollection)
		if err != nil || colResult.Err() != nil {
			if err != nil {
				t.Fatalf("[Loop %d] %s failed - error creating collection [%s]: %e", i, name, _testMongoCollection, err)
			} else {
				t.Fatalf("[Loop %d] %s failed - error creating collection [%s]: %e", i, name, _testMongoCollection, colResult.Err())
			}
		}
		idxResult, err := mc.CreateCollectionIndexes(_testMongoCollection, indexes)
		if err != nil || idxResult == nil || len(idxResult) < 1 {
			if err != nil {
				t.Fatalf("[Loop %d] %s failed - error creating indexes: %e", i, name, err)
			} else {
				t.Fatalf("[Loop %d] %s failed - error creating indexes: %#v", i, name, idxResult)
			}
		}
	}
}
