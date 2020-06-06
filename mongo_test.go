package prom

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/consu/semita"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func newMongoConnect(url, db string) (*MongoConnect, error) {
	return NewMongoConnect(url, db, 10000)
}

func TestNewMongoConnect(t *testing.T) {
	name := "TestNewMongoConnect"
	mc, err := newMongoConnect("mongodb://test:test@localhost:27017/test", "test")
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}
	if mc == nil {
		t.Fatalf("%s failed: nil", name)
	}
	err = mc.Close(nil)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}
}

func TestMongoConnect_GetMongoClient(t *testing.T) {
	name := "TestMongoConnect_GetMongoClient"
	mc, err := newMongoConnect("mongodb://test:test@localhost:27017/test", "test")
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}
	if mc.GetMongoClient() == nil {
		t.Fatalf("%s failed: nil", name)
	}
}

func TestMongoConnect_GetDatabase(t *testing.T) {
	name := "TestMongoConnect_GetDatabase"
	mc, err := newMongoConnect("mongodb://test:test@localhost:27017/test", "test")
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}
	db := mc.GetDatabase(nil)
	if db == nil {
		t.Fatalf("%s failed: nil", name)
	}
}

const (
	envMongoUrl = "MONGO_URL"
	envMongoDb  = "MONGO_DB"
)

func TestMongoConnect_Ping(t *testing.T) {
	mongoUrl := os.Getenv(envMongoUrl)
	mongoDb := os.Getenv(envMongoDb)
	if mongoUrl == "" || mongoDb == "" {
		return
	}
	name := "TestMongoConnect_Ping"
	mc, err := newMongoConnect(mongoUrl, mongoDb)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}
	err = mc.Ping(nil)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}
}

func TestMongoConnect_IsConnected(t *testing.T) {
	mongoUrl := os.Getenv(envMongoUrl)
	mongoDb := os.Getenv(envMongoDb)
	if mongoUrl == "" || mongoDb == "" {
		return
	}
	name := "TestMongoConnect_IsConnected"
	mc, err := newMongoConnect(mongoUrl, mongoDb)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}
	if !mc.IsConnected() {
		t.Fatalf("%s failed: not connected", name)
	}
}

func TestMongoConnect_HasDatabase(t *testing.T) {
	mongoUrl := os.Getenv(envMongoUrl)
	mongoDb := os.Getenv(envMongoDb)
	if mongoUrl == "" || mongoDb == "" {
		return
	}
	name := "TestMongoConnect_HasDatabase"
	mc, err := newMongoConnect(mongoUrl, mongoDb)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}

	ok, err := mc.HasDatabase(mongoDb)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}
	if !ok {
		t.Fatalf("%s failed: no database [%s]", name, mongoDb)
	}

	ok, err = mc.HasDatabase("should_not_exist")
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}
	if ok {
		t.Fatalf("%s failed: database [%s]", name, "should_not_exist")
	}
}

const (
	testMongoCollection = "test_user"
)

func TestMongoConnect_GetCollection(t *testing.T) {
	name := "TestMongoConnect_GetCollection"
	mc, err := newMongoConnect("mongodb://hehe:hehe@localhost:27017/hehe", "hehe")
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}
	c := mc.GetCollection(testMongoCollection)
	if c == nil {
		t.Fatalf("%s failed: nil", name)
	}
}

func TestMongoConnect_HasCollection(t *testing.T) {
	mongoUrl := os.Getenv(envMongoUrl)
	mongoDb := os.Getenv(envMongoDb)
	if mongoUrl == "" || mongoDb == "" {
		return
	}
	name := "TestMongoConnect_HasCollection"
	mc, err := newMongoConnect(mongoUrl, mongoDb)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}

	ok, err := mc.HasCollection(testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}
	if !ok {
		t.Fatalf("%s failed: no collection [%s]", name, mongoDb)
	}

	ok, err = mc.HasCollection("should_not_exist")
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}
	if ok {
		t.Fatalf("%s failed: collection [%s]", name, "should_not_exist")
	}
}

func TestMongoConnect_Collection(t *testing.T) {
	mongoUrl := os.Getenv(envMongoUrl)
	mongoDb := os.Getenv(envMongoDb)
	if mongoUrl == "" || mongoDb == "" {
		return
	}
	name := "TestMongoConnect_Collection"
	mc, err := newMongoConnect(mongoUrl, mongoDb)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}

	ok, err := mc.HasCollection(testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/HasCollection", err)
	} else if ok {
		err = mc.GetCollection(testMongoCollection).Drop(nil)
		if err != nil {
			t.Fatalf("%s failed: error [%e]", name+"/GetCollection", err)
		}
	}

	ok, err = mc.HasCollection(testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/HasCollection", err)
	}
	if ok {
		t.Fatalf("%s failed: collection not deleted [%s]", name+"/HasCollection", testMongoCollection)
	}

	_, err = mc.CreateCollection(testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/CreateCollection", err)
	}

	ok, err = mc.HasCollection(testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/HasCollection", err)
	}
	if !ok {
		t.Fatalf("%s failed: collection not created [%s]", name+"/HasCollection", testMongoCollection)
	}
}

func prepareMongoCollection(mc *MongoConnect, collectionName string) error {
	mc.GetCollection(collectionName).Drop(nil)
	_, err := mc.CreateCollection(collectionName)
	return err
}

func TestMongoConnect_CreateIndexes1(t *testing.T) {
	mongoUrl := os.Getenv(envMongoUrl)
	mongoDb := os.Getenv(envMongoDb)
	if mongoUrl == "" || mongoDb == "" {
		return
	}
	name := "TestMongoConnect_CreateIndexes1"
	mc, err := newMongoConnect(mongoUrl, mongoDb)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}

	err = prepareMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/prepareMongoCollection", err)
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
			t.Fatalf("%s failed: error creating indexes: %e", name, err)
		} else {
			t.Fatalf("%s failed: error creating indexes: %#v", name, idxResult)
		}
	}
	cur, err := mc.GetCollection(testMongoCollection).Indexes().List(nil)
	if err != nil {
		t.Fatalf("%s failed: error listing collection index [%s]: %e", name, testMongoCollection, err)
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
	mongoUrl := os.Getenv(envMongoUrl)
	mongoDb := os.Getenv(envMongoDb)
	if mongoUrl == "" || mongoDb == "" {
		return
	}
	name := "TestMongoConnect_CreateIndexes2"
	mc, err := newMongoConnect(mongoUrl, mongoDb)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}

	err = prepareMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/prepareMongoCollection", err)
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
			t.Fatalf("%s failed:  error creating indexes: %e", name, err)
		} else {
			t.Fatalf("%s failed: error creating indexes: %#v", name, idxResult)
		}
	}
	cur, err := mc.GetCollection(testMongoCollection).Indexes().List(nil)
	if err != nil {
		t.Fatalf("%s failed: error listing collection index [%s]: %e", name, testMongoCollection, err)
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
	mongoUrl := os.Getenv(envMongoUrl)
	mongoDb := os.Getenv(envMongoDb)
	if mongoUrl == "" || mongoDb == "" {
		return
	}
	name := "TestMongoConnect_CreateIndexes3"
	mc, err := newMongoConnect(mongoUrl, mongoDb)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}

	err = prepareMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/prepareMongoCollection", err)
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
			t.Fatalf("%s failed: error creating indexes: %e", name, err)
		} else {
			t.Fatalf("%s failed: error creating indexes: %#v", name, idxResult)
		}
	}
	cur, err := mc.GetCollection(testMongoCollection).Indexes().List(nil)
	if err != nil {
		t.Fatalf("%s failed: error listing collection index [%s]: %e", name, testMongoCollection, err)
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
	mongoUrl := os.Getenv(envMongoUrl)
	mongoDb := os.Getenv(envMongoDb)
	if mongoUrl == "" || mongoDb == "" {
		return
	}
	name := "TestMongoConnect_DecodeSingleResult"
	mc, err := newMongoConnect(mongoUrl, mongoDb)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}

	err = prepareMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/prepareMongoCollection", err)
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
		t.Fatalf("%s failed: error [%e]", name+"/InsertOne", err)
	}

	singleResult := collection.FindOne(nil, bson.M{"username": "btnguyen2k"})
	row, err := mc.DecodeSingleResult(singleResult)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/DecodeSingleResult", err)
	}
	if row == nil {
		t.Fatalf("%s failed: nil", name+"/DecodeSingleResult")
	}

	singleResult = collection.FindOne(nil, bson.M{"username": "not-exists"})
	row, err = mc.DecodeSingleResult(singleResult)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/DecodeSingleResult", err)
	}
	if row != nil {
		t.Fatalf("%s failed: should be nil", name+"/DecodeSingleResult")
	}
}

func TestMongoConnect_DecodeSingleResultRaw(t *testing.T) {
	mongoUrl := os.Getenv(envMongoUrl)
	mongoDb := os.Getenv(envMongoDb)
	if mongoUrl == "" || mongoDb == "" {
		return
	}
	name := "TestMongoConnect_DecodeSingleResultRaw"
	mc, err := newMongoConnect(mongoUrl, mongoDb)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}

	err = prepareMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/prepareMongoCollection", err)
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
		t.Fatalf("%s failed: error [%e]", name+"/InsertOne", err)
	}

	singleResult := collection.FindOne(nil, bson.M{"username": "btnguyen2k"})
	row, err := mc.DecodeSingleResultRaw(singleResult)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/DecodeSingleResultRaw", err)
	}
	if row == nil {
		t.Fatalf("%s failed: nil", name+"/DecodeSingleResultRaw")
	}

	singleResult = collection.FindOne(nil, bson.M{"username": "not-exists"})
	row, err = mc.DecodeSingleResultRaw(singleResult)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/DecodeSingleResultRaw", err)
	}
	if row != nil {
		t.Fatalf("%s failed: should be nil", name+"/DecodeSingleResultRaw")
	}
}

func TestMongoConnect_DecodeResultCallback(t *testing.T) {
	mongoUrl := os.Getenv(envMongoUrl)
	mongoDb := os.Getenv(envMongoDb)
	if mongoUrl == "" || mongoDb == "" {
		return
	}
	name := "TestMongoConnect_DecodeResultCallback"
	mc, err := newMongoConnect(mongoUrl, mongoDb)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}

	err = prepareMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/prepareMongoCollection", err)
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
			t.Fatalf("%s failed: error [%e]", name+"/InsertOne", err)
		}
	}

	numDocs := 0
	cursor, err := collection.Find(nil, bson.M{"id": bson.M{"$gt": 4}})
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/Find", err)
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
	mongoUrl := os.Getenv(envMongoUrl)
	mongoDb := os.Getenv(envMongoDb)
	if mongoUrl == "" || mongoDb == "" {
		return
	}
	name := "TestMongoConnect_DecodeResultCallbackRaw"
	mc, err := newMongoConnect(mongoUrl, mongoDb)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name, err)
	}

	err = prepareMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/prepareMongoCollection", err)
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
			t.Fatalf("%s failed: error [%e]", name+"/InsertOne", err)
		}
	}

	numDocs := 0
	cursor, err := collection.Find(nil, bson.M{"id": bson.M{"$lte": 4}})
	if err != nil {
		t.Fatalf("%s failed: error [%e]", name+"/Find", err)
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
