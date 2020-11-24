package prom

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/consu/semita"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestNewMongoConnect(t *testing.T) {
	name := "TestNewMongoConnect"
	opts := defaultMongoPoolOpts
	opts.MaxPoolSize = 10
	mc, err := NewMongoConnect("mongodb://test:test@localhost:27017/?authSource=admin", "test", -1, opts)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	}
	if err := mc.Init(); err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	}

	url := "url"
	mc.SetUrl(url)
	if mc.GetUrl() != url {
		t.Fatalf("%s failed: expected url to be %#v but received %#v", name, url, mc.GetUrl())
	}

	db := "db"
	mc.SetDb(db)
	if mc.GetDb() != db {
		t.Fatalf("%s failed: expected DB to be %#v but received %#v", name, db, mc.GetDb())
	}

	timeoutMs := 1234
	mc.SetTimeoutMs(timeoutMs)
	if mc.GetTimeoutMs() != timeoutMs {
		t.Fatalf("%s failed: expected timeout to be %#v but received %#v", name, timeoutMs, mc.GetTimeoutMs())
	}

	poolOpts := &MongoPoolOpts{}
	mc.SetMongoPoolOpts(poolOpts)
	if mc.GetMongoPoolOpts() != poolOpts {
		t.Fatalf("%s failed: expected pool options to be %#v but received %#v", name, poolOpts, mc.GetMongoPoolOpts())
	}

	if err := mc.Close(nil); err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	}
}

func TestMongoConnect_GetMongoClient(t *testing.T) {
	name := "TestMongoConnect_GetMongoClient"
	mc, err := NewMongoConnect("mongodb://test:test@localhost:27017/?authSource=admin", "test", 10000, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	}
	defer mc.Close(nil)
	if mc.GetMongoClient() == nil {
		t.Fatalf("%s failed: nil", name)
	}
}

func TestMongoConnect_GetDatabase(t *testing.T) {
	name := "TestMongoConnect_GetDatabase"
	mc, err := NewMongoConnect("mongodb://test:test@localhost:27017/?authSource=admin", "test", 10000, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	}
	defer mc.Close(nil)
	db := mc.GetDatabase(nil)
	if db == nil {
		t.Fatalf("%s failed: nil", name)
	}
}

func TestMongoConnect_GetCollection(t *testing.T) {
	name := "TestMongoConnect_GetCollection"
	mc, err := NewMongoConnect("mongodb://test:test@localhost:27017/?authSource=admin", "test", 10000, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	}
	defer mc.Close(nil)
	c := mc.GetCollection("test_collection")
	if c == nil {
		t.Fatalf("%s failed: nil", name)
	}
}

func TestMongo_FastFailed(t *testing.T) {
	name := "TestMongo_FastFailed"
	timeoutMs := 20
	mc, err := NewMongoConnect("mongodb://test:test@localhost:1234/?authSource=admin", "test", timeoutMs, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	}
	defer mc.Close(nil)

	tstart := time.Now()
	err = mc.Ping(nil)
	if err == nil {
		t.Fatalf("%s failed: the operation should not success", name)
	}
	d := time.Duration(time.Now().UnixNano() - tstart.UnixNano())
	dmax := time.Duration(float64(time.Duration(timeoutMs)*time.Millisecond) * 1.5)
	if d > dmax {
		t.Fatalf("%s failed: operation is expected to fail within %#v ms but in fact %#v ms", name, dmax/1E6, d/1E6)
	}
}

func _createMongoConnect(t *testing.T, testName string) *MongoConnect {
	mongoUrl := strings.ReplaceAll(os.Getenv("MONGO_URL"), `"`, "")
	mongoDb := strings.ReplaceAll(os.Getenv("MONGO_DB"), `"`, "")
	if mongoUrl == "" || mongoDb == "" {
		t.Skipf("%s skipped", testName)
		return nil
	}
	mc, err := NewMongoConnect(mongoUrl, mongoDb, 10000, nil)
	if err != nil {
		t.Fatalf("%s/%s failed: %s", testName, "NewMongoConnect", err)
	}
	return mc
}

func TestMongoConnect_Ping(t *testing.T) {
	name := "TestMongoConnect_Ping"
	mc := _createMongoConnect(t, name)
	defer mc.Close(nil)
	err := mc.Ping(nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	}
}

func TestMongoConnect_IsConnected(t *testing.T) {
	name := "TestMongoConnect_IsConnected"
	mc := _createMongoConnect(t, name)
	defer mc.Close(nil)
	if !mc.IsConnected() {
		t.Fatalf("%s failed: not connected", name)
	}
}

func TestMongoConnect_HasDatabase(t *testing.T) {
	name := "TestMongoConnect_HasDatabase"
	mc := _createMongoConnect(t, name)
	defer mc.Close(nil)

	ok, err := mc.HasDatabase(mc.db)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	}
	if !ok {
		t.Fatalf("%s failed: no database [%s]", name, mc.db)
	}

	ok, err = mc.HasDatabase("should_not_exist")
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	}
	if ok {
		t.Fatalf("%s failed: database [%s]", name, "should_not_exist")
	}
}

func _initMongoCollection(mc *MongoConnect, collectionName string) error {
	mc.GetCollection(collectionName).Drop(nil)
	_, err := mc.CreateCollection(collectionName)
	return err
}

const (
	testMongoCollection = "test_user"
)

func TestMongoConnect_HasCollection(t *testing.T) {
	name := "TestMongoConnect_HasCollection"
	mc := _createMongoConnect(t, name)
	defer mc.Close(nil)

	err := _initMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	}

	ok, err := mc.HasCollection(testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	}
	if !ok {
		t.Fatalf("%s failed: no collection [%s]", name, testMongoCollection)
	}

	ok, err = mc.HasCollection("should_not_exist")
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	}
	if ok {
		t.Fatalf("%s failed: collection [%s]", name, "should_not_exist")
	}
}

func TestMongoConnect_Collection(t *testing.T) {
	name := "TestMongoConnect_Collection"
	mc := _createMongoConnect(t, name)
	defer mc.Close(nil)

	ok, err := mc.HasCollection(testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name+"/HasCollection", err)
	} else if ok {
		err = mc.GetCollection(testMongoCollection).Drop(mc.NewContext())
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/GetCollection", err)
		}
	}

	ok, err = mc.HasCollection(testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name+"/HasCollection", err)
	}
	if ok {
		t.Fatalf("%s failed: collection not deleted [%s]", name+"/HasCollection", testMongoCollection)
	}

	_, err = mc.CreateCollection(testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name+"/CreateCollection", err)
	}

	ok, err = mc.HasCollection(testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name+"/HasCollection", err)
	}
	if !ok {
		t.Fatalf("%s failed: collection not created [%s]", name+"/HasCollection", testMongoCollection)
	}
}

func TestMongoConnect_CreateIndexes1(t *testing.T) {
	name := "TestMongoConnect_CreateIndexes1"
	mc := _createMongoConnect(t, name)
	defer mc.Close(nil)

	err := _initMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name+"/_initMongoCollection", err)
	}

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
	idxResult, err := mc.CreateCollectionIndexes(testMongoCollection, indexes)
	if err != nil || idxResult == nil || len(idxResult) < 1 {
		if err != nil {
			t.Fatalf("%s failed: error creating indexes: %s", name, err)
		} else {
			t.Fatalf("%s failed: error creating indexes: %#v", name, idxResult)
		}
	}
	cur, err := mc.GetCollection(testMongoCollection).Indexes().List(nil)
	if err != nil {
		t.Fatalf("%s failed: error listing collection index [%s]: %s", name, testMongoCollection, err)
	}
	var ok1, ok2 = false, false
	for cur.Next(nil) {
		var i map[string]interface{}
		cur.Decode(&i)
		s := semita.NewSemita(i)
		name, _ := s.GetValueOfType("name", reddo.TypeString)
		if name.(string) == "uidx_from" {
			ok, _ := s.GetValueOfType("unique", reddo.TypeBool)
			ok1 = ok.(bool)
		}
		if name.(string) == "idx_to" {
			ok2 = true
		}
	}
	cur.Close(nil)
	if !ok1 {
		t.Fatalf("%s failed: cannot find index [%s] or its unique attribute is not true", name, "uidx_from")
	}
	if !ok2 {
		t.Fatalf("%s failed: cannot find index [%s]", name, "idx_to")
	}
}

func TestMongoConnect_CreateIndexes2(t *testing.T) {
	name := "TestMongoConnect_CreateIndexes2"
	mc := _createMongoConnect(t, name)
	defer mc.Close(nil)

	err := _initMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name+"/_initMongoCollection", err)
	}

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
	idxResult, err := mc.CreateCollectionIndexes(testMongoCollection, indexes)
	if err != nil || idxResult == nil || len(idxResult) < 1 {
		if err != nil {
			t.Fatalf("%s failed:  error creating indexes: %s", name, err)
		} else {
			t.Fatalf("%s failed: error creating indexes: %#v", name, idxResult)
		}
	}
	cur, err := mc.GetCollection(testMongoCollection).Indexes().List(nil)
	if err != nil {
		t.Fatalf("%s failed: error listing collection index [%s]: %s", name, testMongoCollection, err)
	}
	var ok1, ok2 = false, false
	for cur.Next(nil) {
		var i map[string]interface{}
		cur.Decode(&i)
		s := semita.NewSemita(i)
		name, _ := s.GetValueOfType("name", reddo.TypeString)
		if name.(string) == "uidx_from" {
			ok, _ := s.GetValueOfType("unique", reddo.TypeBool)
			ok1 = ok.(bool)
		}
		if name.(string) == "idx_to" {
			ok2 = true
		}
	}
	cur.Close(nil)
	if !ok1 {
		t.Fatalf("%s failed: cannot find index [%s] or its unique attribute is not true", name, "uidx_from")
	}
	if !ok2 {
		t.Fatalf("%s failed: cannot find index [%s]", name, "idx_to")
	}
}

func TestMongoConnect_CreateIndexes3(t *testing.T) {
	name := "TestMongoConnect_CreateIndexes3"
	mc := _createMongoConnect(t, name)
	defer mc.Close(nil)

	err := _initMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name+"/_initMongoCollection", err)
	}

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
	idxResult, err := mc.CreateCollectionIndexes(testMongoCollection, indexes)
	if err != nil || idxResult == nil || len(idxResult) < 1 {
		if err != nil {
			t.Fatalf("%s failed: error creating indexes: %s", name, err)
		} else {
			t.Fatalf("%s failed: error creating indexes: %#v", name, idxResult)
		}
	}
	cur, err := mc.GetCollection(testMongoCollection).Indexes().List(nil)
	if err != nil {
		t.Fatalf("%s failed: error listing collection index [%s]: %s", name, testMongoCollection, err)
	}
	var ok1, ok2 = false, false
	for cur.Next(nil) {
		var i map[string]interface{}
		cur.Decode(&i)
		s := semita.NewSemita(i)
		name, _ := s.GetValueOfType("name", reddo.TypeString)
		if name.(string) == "uidx_from" {
			ok, _ := s.GetValueOfType("unique", reddo.TypeBool)
			ok1 = ok.(bool)
		}
		if name.(string) == "idx_to" {
			ok2 = true
		}
	}
	cur.Close(nil)
	if !ok1 {
		t.Fatalf("%s failed: cannot find index [%s] or its unique attribute is not true", name, "uidx_from")
	}
	if !ok2 {
		t.Fatalf("%s failed: cannot find index [%s]", name, "idx_to")
	}
}

func TestMongoConnect_DecodeSingleResult(t *testing.T) {
	name := "TestMongoConnect_DecodeSingleResult"
	mc := _createMongoConnect(t, name)
	defer mc.Close(nil)

	err := _initMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name+"/_initMongoCollection", err)
	}

	item := map[string]interface{}{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  time.Now(),
		"actived":  true,
	}
	collection := mc.GetCollection(testMongoCollection)
	_, err = collection.InsertOne(nil, item)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name+"/InsertOne", err)
	}

	singleResult := collection.FindOne(nil, bson.M{"username": "btnguyen2k"})
	row, err := mc.DecodeSingleResult(singleResult)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name+"/DecodeSingleResult", err)
	}
	if row == nil {
		t.Fatalf("%s failed: nil", name+"/DecodeSingleResult")
	}

	singleResult = collection.FindOne(nil, bson.M{"username": "not-exists"})
	row, err = mc.DecodeSingleResult(singleResult)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name+"/DecodeSingleResult", err)
	}
	if row != nil {
		t.Fatalf("%s failed: should be nil", name+"/DecodeSingleResult")
	}
}

func TestMongoConnect_DecodeSingleResultRaw(t *testing.T) {
	name := "TestMongoConnect_DecodeSingleResultRaw"
	mc := _createMongoConnect(t, name)
	defer mc.Close(nil)

	err := _initMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name+"/_initMongoCollection", err)
	}

	item := map[string]interface{}{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  time.Now(),
		"actived":  true,
	}
	collection := mc.GetCollection(testMongoCollection)
	_, err = collection.InsertOne(nil, item)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name+"/InsertOne", err)
	}

	singleResult := collection.FindOne(nil, bson.M{"username": "btnguyen2k"})
	row, err := mc.DecodeSingleResultRaw(singleResult)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name+"/DecodeSingleResultRaw", err)
	}
	if row == nil {
		t.Fatalf("%s failed: nil", name+"/DecodeSingleResultRaw")
	}

	singleResult = collection.FindOne(nil, bson.M{"username": "not-exists"})
	row, err = mc.DecodeSingleResultRaw(singleResult)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name+"/DecodeSingleResultRaw", err)
	}
	if row != nil {
		t.Fatalf("%s failed: should be nil", name+"/DecodeSingleResultRaw")
	}
}

func TestMongoConnect_DecodeResultCallback(t *testing.T) {
	name := "TestMongoConnect_DecodeResultCallback"
	mc := _createMongoConnect(t, name)
	defer mc.Close(nil)

	err := _initMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name+"/_initMongoCollection", err)
	}

	collection := mc.GetCollection(testMongoCollection)
	for i := 0; i < 10; i++ {
		item := map[string]interface{}{
			"id":       i,
			"username": strconv.Itoa(i),
			"email":    strconv.Itoa(i) + "@domain.com",
			"version":  time.Now(),
			"actived":  i%2 == 0,
		}
		_, err = collection.InsertOne(nil, item)
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/InsertOne", err)
		}
	}

	numDocs := 0
	cursor, err := collection.Find(nil, bson.M{"id": bson.M{"$gt": 4}})
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name+"/Find", err)
	}
	mc.DecodeResultCallback(nil, cursor, func(docNum int, doc bson.M, err error) bool {
		if err == nil {
			numDocs++
			return true
		}
		return false
	})
	if numDocs != 5 {
		t.Fatalf("%s failed: expected %d docs but received %d", name+"/DecodeResultCallback", 5, numDocs)
	}
}

func TestMongoConnect_DecodeResultCallbackRaw(t *testing.T) {
	name := "TestMongoConnect_DecodeResultCallbackRaw"
	mc := _createMongoConnect(t, name)
	defer mc.Close(nil)

	err := _initMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name+"/_initMongoCollection", err)
	}

	collection := mc.GetCollection(testMongoCollection)
	for i := 0; i < 10; i++ {
		item := map[string]interface{}{
			"id":       i,
			"username": strconv.Itoa(i),
			"email":    strconv.Itoa(i) + "@domain.com",
			"version":  time.Now(),
			"actived":  i%2 == 0,
		}
		_, err = collection.InsertOne(nil, item)
		if err != nil {
			t.Fatalf("%s failed: error [%s]", name+"/InsertOne", err)
		}
	}

	numDocs := 0
	cursor, err := collection.Find(nil, bson.M{"id": bson.M{"$lte": 4}})
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name+"/Find", err)
	}
	mc.DecodeResultCallbackRaw(nil, cursor, func(docNum int, doc []byte, err error) bool {
		if err == nil {
			numDocs++
			return true
		}
		return false
	})
	if numDocs != 5 {
		t.Fatalf("%s failed: expected %d docs but received %d", name+"/DecodeResultCallbackRaw", 5, numDocs)
	}
}
