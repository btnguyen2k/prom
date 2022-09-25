package prom

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
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
	testName := "TestNewMongoConnect"
	url := "mongodb://test:test@server:27017/?authSource=admin"
	db := "mydb"
	timeoutMs := 12345
	mc, err := NewMongoConnect(url, db, timeoutMs)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	if err := mc.Init(); err != nil {
		// calling Init() multiple times should not cause error
		t.Fatalf("%s failed: error [%s]", testName+"/Init", err)
	}

	if mc.GetUrl() != url {
		t.Fatalf("%s failed: expected URL to be %#v but received %#v", testName, url, mc.GetUrl())
	}
	url += "-new"
	mc.SetUrl(url)
	if mc.GetUrl() != url {
		t.Fatalf("%s failed: expected URL to be %#v but received %#v", testName, url, mc.GetUrl())
	}

	if mc.GetDb() != db {
		t.Fatalf("%s failed: expected DB to be %#v but received %#v", testName, db, mc.GetDb())
	}
	db += "-new"
	mc.SetDb(db)
	if mc.GetDb() != db {
		t.Fatalf("%s failed: expected DB to be %#v but received %#v", testName, db, mc.GetDb())
	}

	if mc.GetTimeoutMs() != timeoutMs {
		t.Fatalf("%s failed: expected TIMEOUT to be %#v but received %#v", testName, timeoutMs, mc.GetTimeoutMs())
	}
	timeoutMs += 56
	mc.SetTimeoutMs(timeoutMs)
	if mc.GetTimeoutMs() != timeoutMs {
		t.Fatalf("%s failed: expected TIMEOUT to be %#v but received %#v", testName, timeoutMs, mc.GetTimeoutMs())
	}

	if !reflect.DeepEqual(mc.GetMongoPoolOpts(), defaultMongoPoolOpts) {
		t.Fatalf("%s failed: expected POOL options to be %#v but received %#v", testName, defaultMongoPoolOpts, mc.GetMongoPoolOpts())
	}
	poolOpts := &MongoPoolOpts{}
	mc.SetMongoPoolOpts(poolOpts)
	if !reflect.DeepEqual(mc.GetMongoPoolOpts(), poolOpts) {
		t.Fatalf("%s failed: expected POOL options to be %#v but received %#v", testName, poolOpts, mc.GetMongoPoolOpts())
	}

	if err := mc.Close(nil); err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
}

func TestNewMongoConnect_DefaultTimeoutms(t *testing.T) {
	testName := "TestNewMongoConnectDefaultTimeoutms"
	mc, err := NewMongoConnect("mongodb://test:test@server:27017/?authSource=admin", "mydb", -1)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	if mc.GetTimeoutMs() != 0 {
		t.Fatalf("%s failed: expected TIMEOUT to be %#v but received %#v", testName, 0, mc.GetTimeoutMs())
	}
	if !reflect.DeepEqual(mc.GetMongoPoolOpts(), defaultMongoPoolOpts) {
		t.Fatalf("%s failed: expected POOL options to be %#v but received %#v", testName, defaultMongoPoolOpts, mc.GetMongoPoolOpts())
	}
}

func TestNewMongoConnectWithPoolOptions(t *testing.T) {
	testName := "TestNewMongoConnectWithPoolOptions"
	url := "mongodb://test:test@server:27017/?authSource=admin"
	db := "mydb"
	timeoutMs := 12345
	opts := &MongoPoolOpts{MinPoolSize: 12, MaxPoolSize: 34, ConnectTimeout: 2345}
	mc, err := NewMongoConnectWithPoolOptions(url, db, timeoutMs, opts)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	if err := mc.Init(); err != nil {
		// calling Init() multiple times should not cause error
		t.Fatalf("%s failed: error [%s]", testName+"/Init", err)
	}

	if mc.GetUrl() != url {
		t.Fatalf("%s failed: expected URL to be %#v but received %#v", testName, url, mc.GetUrl())
	}

	if mc.GetDb() != db {
		t.Fatalf("%s failed: expected DB to be %#v but received %#v", testName, db, mc.GetDb())
	}

	if mc.GetTimeoutMs() != timeoutMs {
		t.Fatalf("%s failed: expected TIMEOUT to be %#v but received %#v", testName, timeoutMs, mc.GetTimeoutMs())
	}

	if !reflect.DeepEqual(mc.GetMongoPoolOpts(), opts) {
		t.Fatalf("%s failed: expected POOL options to be %#v but received %#v", testName, opts, mc.GetMongoPoolOpts())
	}

	if err := mc.Close(nil); err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
}

func TestNewMongoConnectWithPoolOptions_nil(t *testing.T) {
	testName := "TestNewMongoConnectWithPoolOptions_nil"
	mc, err := NewMongoConnectWithPoolOptions("mongodb://test:test@server:27017/?authSource=admin", "mydb", -1, nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	if mc.GetTimeoutMs() != 0 {
		t.Fatalf("%s failed: expected TIMEOUT to be %#v but received %#v", testName, 0, mc.GetTimeoutMs())
	}
	if !reflect.DeepEqual(mc.GetMongoPoolOpts(), defaultMongoPoolOpts) {
		t.Fatalf("%s failed: expected POOL options to be %#v but received %#v", testName, defaultMongoPoolOpts, mc.GetMongoPoolOpts())
	}
}

func TestMongoConnect_Metrics(t *testing.T) {
	testName := "TestMongoConnect_Metrics"
	mc, err := NewMongoConnect("mongodb://test:test@localhost:27017/?authSource=admin", "test", 10000)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	defer mc.Close(nil)

	if mc.MetricsLogger() == nil {
		t.Fatalf("%s failed: nil", testName)
	}

	ml := NewMemoryStoreMetricsLogger(1028)
	mc.RegisterMetricsLogger(ml)
	if mc.MetricsLogger() != ml {
		t.Fatalf("%s failed", testName)
	}

	cmd := mc.NewCmdExecInfo()
	if err := mc.LogMetrics(MetricsCatAll, cmd); err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	m, err := mc.Metrics(MetricsCatAll, MetricsOpts{ReturnLatestCommands: 1})
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	if m == nil {
		t.Fatalf("%s failed: nil", testName)
	}
	if len(m.LastNCmds) != 1 || m.LastNCmds[0] != cmd {
		t.Fatalf("%s failed.", testName)
	}
}

func TestMongoConnect_GetMongoClient(t *testing.T) {
	testName := "TestMongoConnect_GetMongoClient"
	mc, err := NewMongoConnect("mongodb://test:test@localhost:27017/?authSource=admin", "test", 10000)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	defer mc.Close(nil)
	if mc.GetMongoClient() == nil {
		t.Fatalf("%s failed: nil", testName)
	}
}

func TestMongoConnect_GetDatabase(t *testing.T) {
	testName := "TestMongoConnect_GetDatabase"
	dbname := "mydb"
	mc, err := NewMongoConnect("mongodb://test:test@localhost:27017/?authSource=admin", dbname, 10000)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	defer mc.Close(nil)
	db := mc.GetDatabase(nil)
	if db == nil {
		t.Fatalf("%s failed: nil", testName)
	}
	if db.Name() != dbname {
		t.Fatalf("%s failed: expected database to be [%s] but received [%s]", testName, dbname, db.Name())
	}
}

func TestMongoConnect_GetDatabaseProxy(t *testing.T) {
	testName := "TestMongoConnect_GetDatabaseProxy"
	dbname := "mydb"
	mc, err := NewMongoConnect("mongodb://test:test@localhost:27017/?authSource=admin", dbname, 10000)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	defer mc.Close(nil)
	db := mc.GetDatabaseProxy(nil)
	if db == nil {
		t.Fatalf("%s failed: nil", testName)
	}
	if db.Name() != dbname {
		t.Fatalf("%s failed: expected database to be [%s] but received [%s]", testName, dbname, db.Name())
	}
}

func TestMongoConnect_GetCollection(t *testing.T) {
	testName := "TestMongoConnect_GetCollection"
	dbname := "mydb"
	collname := "mycollection"
	mc, err := NewMongoConnect("mongodb://test:test@localhost:27017/?authSource=admin", dbname, 10000)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	defer mc.Close(nil)

	c := mc.GetCollection(collname)
	if c == nil {
		t.Fatalf("%s failed: nil", testName)
	}
	if c.Database().Name() != dbname {
		t.Fatalf("%s failed: expected database to be [%s] but received [%s]", testName, dbname, c.Database().Name())
	}
	if c.Name() != collname {
		t.Fatalf("%s failed: expected database to be [%s] but received [%s]", testName, collname, c.Name())
	}
}

func TestMongoConnect_GetCollectionProxy(t *testing.T) {
	testName := "TestMongoConnect_GetCollectionProxy"
	dbname := "mydb"
	collname := "mycollection"
	mc, err := NewMongoConnect("mongodb://test:test@localhost:27017/?authSource=admin", dbname, 10000)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	defer mc.Close(nil)

	c := mc.GetCollectionProxy(collname)
	if c == nil {
		t.Fatalf("%s failed: nil", testName)
	}
	if c.Database().Name() != dbname {
		t.Fatalf("%s failed: expected database to be [%s] but received [%s]", testName, dbname, c.Database().Name())
	}
	if c.Name() != collname {
		t.Fatalf("%s failed: expected database to be [%s] but received [%s]", testName, collname, c.Name())
	}
}

func TestMongoConnect_NewContext(t *testing.T) {
	testName := "TestMongoConnect_NewContext"
	mc, err := NewMongoConnect("mongodb://test:test@localhost:27017/?authSource=admin", "mydb", 10000)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	defer mc.Close(nil)

	if mc.NewContext() == nil {
		t.Fatalf("%s failed: nil", testName)
	}
	if mc.NewContext(12) == nil {
		t.Fatalf("%s failed: nil", testName)
	}
	if mc.NewContext(-34) == nil {
		t.Fatalf("%s failed: nil", testName)
	}
}

func TestMongoConnect_NewContextIfNil(t *testing.T) {
	testName := "TestMongoConnect_NewContextIfNil"
	mc, err := NewMongoConnect("mongodb://test:test@localhost:27017/?authSource=admin", "mydb", 10000)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	defer mc.Close(nil)

	if mc.NewContextIfNil(nil) == nil {
		t.Fatalf("%s failed: nil", testName)
	}
	if mc.NewContextIfNil(nil, 12) == nil {
		t.Fatalf("%s failed: nil", testName)
	}
	if mc.NewContextIfNil(nil, -34) == nil {
		t.Fatalf("%s failed: nil", testName)
	}

	ctx := context.Background()
	if mc.NewContextIfNil(ctx) != ctx {
		t.Fatalf("%s failed", testName)
	}
	if mc.NewContextIfNil(ctx, 12) != ctx {
		t.Fatalf("%s failed", testName)
	}
	if mc.NewContextIfNil(ctx, -34) != ctx {
		t.Fatalf("%s failed", testName)
	}
}

func TestMongoConnect_NewContextWithCancel(t *testing.T) {
	testName := "TestMongoConnect_NewContextWithCancel"
	mc, err := NewMongoConnect("mongodb://test:test@localhost:27017/?authSource=admin", "mydb", 10000)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	defer mc.Close(nil)

	if ctx, cancel := mc.NewContextWithCancel(); ctx == nil || cancel == nil {
		t.Fatalf("%s failed: nil", testName)
	}

	if ctx, cancel := mc.NewContextWithCancel(12); ctx == nil || cancel == nil {
		t.Fatalf("%s failed: nil", testName)
	}

	if ctx, cancel := mc.NewContextWithCancel(-34); ctx == nil || cancel == nil {
		t.Fatalf("%s failed: nil", testName)
	}
}

func TestMongo_FastFailed(t *testing.T) {
	name := "TestMongo_FastFailed"
	timeoutMs := 20
	mc, err := NewMongoConnect("mongodb://test:test@localhost:1234/?authSource=admin", "test", timeoutMs)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", name, err)
	}
	defer mc.Close(nil)

	tstart := time.Now()
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(timeoutMs)*time.Millisecond)
	err = mc.Ping(ctx)
	if err == nil {
		t.Fatalf("%s failed: the operation should not success", name)
	}
	d := time.Now().Sub(tstart)
	dmax := time.Duration(float64(timeoutMs)*10.0) * time.Millisecond
	if d > dmax {
		t.Fatalf("%s failed: operation is expected to fail within %#v ms but in fact %#v ms", name, dmax/1e6, d/1e6)
	}
}

func _createMongoConnect(t *testing.T, testName string) *MongoConnect {
	mongoUrl := strings.ReplaceAll(os.Getenv("MONGO_URL"), `"`, "")
	mongoDb := strings.ReplaceAll(os.Getenv("MONGO_DB"), `"`, "")
	if mongoUrl == "" || mongoDb == "" {
		t.Skipf("%s skipped", testName)
		return nil
	}
	mc, err := NewMongoConnect(mongoUrl, mongoDb, 10000)
	if err != nil {
		t.Fatalf("%s/%s failed: %s", testName, "NewMongoConnect", err)
	}

	// HACK to force database creation
	mc.CreateCollection("__prom")

	return mc
}

func _mcVerifyLastCommand(f TestFailedWithMsgFunc, testName string, mc *MongoConnect, cmdName string, cmdCats ...string) {
	for _, cat := range cmdCats {
		m, err := mc.Metrics(cat, MetricsOpts{ReturnLatestCommands: 1})
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
		cmd.CmdRequest, cmd.CmdResponse, cmd.CmdMeta = nil, nil, nil
		if cmd.CmdName != cmdName || cmd.Result != CmdResultOk || cmd.Error != nil || cmd.Cost < 0 {
			f(fmt.Sprintf("%s failed: invalid last command metrics.\nExpected: [Name=%v / Result=%v / Error = %e / Cost = %v]\nReceived: [Name=%v / Result=%v / Error = %s / Cost = %v]",
				testName+"/Metrics("+cat+")",
				cmdName, CmdResultOk, error(nil), ">= 0",
				cmd.CmdName, cmd.Result, cmd.Error, cmd.Cost))
		}
	}
}

func TestMongoConnect_Ping(t *testing.T) {
	testName := "TestMongoConnect_Ping"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)
	err := mc.Ping(nil)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "ping", MetricsCatAll)
}

func TestMongoConnect_IsConnected(t *testing.T) {
	testName := "TestMongoConnect_IsConnected"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)
	if !mc.IsConnected() {
		t.Fatalf("%s failed: not connected", testName)
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "ping", MetricsCatAll)
}

func TestMongoConnect_HasDatabase(t *testing.T) {
	testName := "TestMongoConnect_HasDatabase"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	ok, err := mc.HasDatabase(mc.db)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	if !ok {
		t.Fatalf("%s failed: no database [%s]", testName, mc.db)
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "listDatabases", MetricsCatAll)

	ok, err = mc.HasDatabase("should_not_exist")
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	if ok {
		t.Fatalf("%s failed: database [%s]", testName, "should_not_exist")
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "listDatabases", MetricsCatAll)
}

func _initMongoCollection(mc *MongoConnect, collectionName string) error {
	if err := mc.DropCollection(collectionName); err != nil {
		return err
	}
	return mc.CreateCollection(collectionName)
}

const (
	testMongoCollection = "test_user"
)

func TestMongoConnect_View(t *testing.T) {
	testName := "TestMongoConnect_View"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	err := _initMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/_initMongoCollection", err)
	}

	viewName := testMongoCollection + "_view"
	mc.DropCollection(viewName)
	err = mc.CreateView(viewName, testMongoCollection, bson.A{bson.M{"$match": bson.M{"year": 1}}})
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/CreateView", err)
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "createView", MetricsCatAll)

	ok, err := mc.HasCollection(viewName)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/HasCollection", err)
	}
	if !ok {
		t.Fatalf("%s failed: view not created [%s]", testName+"/HasCollection", viewName)
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "listCollections", MetricsCatAll)

	dataJson := `[
	{ "sID": 22001, "name": "Alex", "year": 1, "score": 4.0 },
	{ "sID": 21001, "name": "bernie", "year": 2, "score": 3.7 },
	{ "sID": 20010, "name": "Chris", "year": 3, "score": 2.5 },
	{ "sID": 22021, "name": "Drew", "year": 1, "score": 3.2 },
	{ "sID": 17301, "name": "harley", "year": 6, "score": 3.1 },
	{ "sID": 21022, "name": "Farmer", "year": 1, "score": 2.2 },
	{ "sID": 20020, "name": "george", "year": 3, "score": 2.8 },
	{ "sID": 18020, "name": "Harley", "year": 5, "score": 2.8 }
]`
	var documents []interface{}
	if err := json.Unmarshal([]byte(dataJson), &documents); err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	if _, err := mc.GetCollection(testMongoCollection).InsertMany(nil, documents); err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}

	if err := mc.DropCollection(viewName); err != nil {
		t.Fatalf("%s failed: view not created [%s]", testName+"/DropCollection", viewName)
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "dropCollection", MetricsCatAll)

	ok, err = mc.HasCollection(viewName)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/HasCollection", err)
	}
	if ok {
		t.Fatalf("%s failed: view should no longer exist [%s]", testName+"/HasCollection", viewName)
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "listCollections", MetricsCatAll)
}

func TestMongoConnect_HasCollection(t *testing.T) {
	testName := "TestMongoConnect_HasCollection"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	err := _initMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "createCollection", MetricsCatAll)

	ok, err := mc.HasCollection(testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	if !ok {
		t.Fatalf("%s failed: no collection [%s]", testName, testMongoCollection)
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "listCollections", MetricsCatAll)

	ok, err = mc.HasCollection("should_not_exist")
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName, err)
	}
	if ok {
		t.Fatalf("%s failed: collection [%s]", testName, "should_not_exist")
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "listCollections", MetricsCatAll)
}

func TestMongoConnect_Collection(t *testing.T) {
	testName := "TestMongoConnect_Collection"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	ok, err := mc.HasCollection(testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/HasCollection", err)
	} else if ok {
		err = mc.GetCollection(testMongoCollection).Drop(mc.NewContext())
		if err != nil {
			t.Fatalf("%s failed: error [%s]", testName+"/GetCollection", err)
		}
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "listCollections", MetricsCatAll)

	ok, err = mc.HasCollection(testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/HasCollection", err)
	}
	if ok {
		t.Fatalf("%s failed: collection not deleted [%s]", testName+"/HasCollection", testMongoCollection)
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "listCollections", MetricsCatAll)

	err = mc.CreateCollection(testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/CreateCollection", err)
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "createCollection", MetricsCatAll)

	ok, err = mc.HasCollection(testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/HasCollection", err)
	}
	if !ok {
		t.Fatalf("%s failed: collection not created [%s]", testName+"/HasCollection", testMongoCollection)
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "listCollections", MetricsCatAll)
}

func TestMongoConnect_CreateIndexes1(t *testing.T) {
	testName := "TestMongoConnect_CreateIndexes1"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	err := _initMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/_initMongoCollection", err)
	}

	fieldMapNamespace := "ns"
	fieldMapFrom := "frm"
	fieldMapTo := "to"
	indexes := []interface{}{
		map[string]interface{}{
			"key": bson.D{
				{fieldMapNamespace, 1},
				{fieldMapFrom, 1},
			},
			"name":   "uidx_from",
			"unique": true,
		},
		map[string]interface{}{
			"key": bson.D{
				{fieldMapNamespace, 1},
				{fieldMapTo, 1},
			},
			"name": "idx_to",
		},
	}
	idxResult, err := mc.CreateCollectionIndexes(testMongoCollection, indexes)
	if err != nil || idxResult == nil || len(idxResult) < 1 {
		if err != nil {
			t.Fatalf("%s failed: error creating indexes: %s", testName, err)
		} else {
			t.Fatalf("%s failed: error creating indexes: %#v", testName, idxResult)
		}
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "createIndexes", MetricsCatAll)

	cur, err := mc.GetCollection(testMongoCollection).Indexes().List(nil)
	if err != nil {
		t.Fatalf("%s failed: error listing collection index [%s]: %s", testName, testMongoCollection, err)
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
		t.Fatalf("%s failed: cannot find index [%s] or its unique attribute is not true", testName, "uidx_from")
	}
	if !ok2 {
		t.Fatalf("%s failed: cannot find index [%s]", testName, "idx_to")
	}
}

func TestMongoConnect_CreateIndexes2(t *testing.T) {
	testName := "TestMongoConnect_CreateIndexes2"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	err := _initMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/_initMongoCollection", err)
	}

	fieldMapNamespace := "ns"
	fieldMapFrom := "frm"
	fieldMapTo := "to"
	idxName := "idx_to"
	indexes := []interface{}{
		map[string]interface{}{
			"key": bson.D{
				{fieldMapNamespace, 1},
				{fieldMapFrom, 1},
			},
			"name":   "uidx_from",
			"unique": true,
		},
		mongo.IndexModel{
			Keys: bson.D{
				{fieldMapNamespace, 1},
				{fieldMapTo, 1},
			},
			Options: &options.IndexOptions{Name: &idxName},
		},
	}
	idxResult, err := mc.CreateCollectionIndexes(testMongoCollection, indexes)
	if err != nil || idxResult == nil || len(idxResult) < 1 {
		if err != nil {
			t.Fatalf("%s failed:  error creating indexes: %s", testName, err)
		} else {
			t.Fatalf("%s failed: error creating indexes: %#v", testName, idxResult)
		}
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "createIndexes", MetricsCatAll)

	cur, err := mc.GetCollection(testMongoCollection).Indexes().List(nil)
	if err != nil {
		t.Fatalf("%s failed: error listing collection index [%s]: %s", testName, testMongoCollection, err)
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
		t.Fatalf("%s failed: cannot find index [%s] or its unique attribute is not true", testName, "uidx_from")
	}
	if !ok2 {
		t.Fatalf("%s failed: cannot find index [%s]", testName, "idx_to")
	}
}

func TestMongoConnect_CreateIndexes3(t *testing.T) {
	testName := "TestMongoConnect_CreateIndexes3"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	err := _initMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/_initMongoCollection", err)
	}

	fieldMapNamespace := "ns"
	fieldMapFrom := "frm"
	fieldMapTo := "to"
	uidxName := "uidx_from"
	isUnique := true
	idxName := "idx_to"
	indexes := []interface{}{
		mongo.IndexModel{
			Keys: bson.D{
				{fieldMapNamespace, 1},
				{fieldMapFrom, 1},
			},
			Options: &options.IndexOptions{Name: &uidxName, Unique: &isUnique},
		},
		mongo.IndexModel{
			Keys: bson.D{
				{fieldMapNamespace, 1},
				{fieldMapTo, 1},
			},
			Options: &options.IndexOptions{Name: &idxName},
		},
	}
	idxResult, err := mc.CreateCollectionIndexes(testMongoCollection, indexes)
	if err != nil || idxResult == nil || len(idxResult) < 1 {
		if err != nil {
			t.Fatalf("%s failed: error creating indexes: %s", testName, err)
		} else {
			t.Fatalf("%s failed: error creating indexes: %#v", testName, idxResult)
		}
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "createIndexes", MetricsCatAll)

	cur, err := mc.GetCollection(testMongoCollection).Indexes().List(nil)
	if err != nil {
		t.Fatalf("%s failed: error listing collection index [%s]: %s", testName, testMongoCollection, err)
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
		t.Fatalf("%s failed: cannot find index [%s] or its unique attribute is not true", testName, "uidx_from")
	}
	if !ok2 {
		t.Fatalf("%s failed: cannot find index [%s]", testName, "idx_to")
	}
}

func TestMongoConnect_DecodeSingleResult(t *testing.T) {
	testName := "TestMongoConnect_DecodeSingleResult"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	err := _initMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/_initMongoCollection", err)
	}

	item := map[string]interface{}{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  time.Now(),
		"actived":  true,
	}
	collection := mc.GetCollectionProxy(testMongoCollection)
	_, err = collection.InsertOne(nil, item)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/InsertOne", err)
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "insert", MetricsCatAll)

	singleResult := collection.FindOne(nil, bson.M{"username": "btnguyen2k"})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "find", MetricsCatAll)
	row, err := mc.DecodeSingleResult(singleResult)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/DecodeSingleResult", err)
	}
	if row == nil {
		t.Fatalf("%s failed: nil", testName+"/DecodeSingleResult")
	}

	singleResult = collection.FindOne(nil, bson.M{"username": "not-exists"})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "find", MetricsCatAll)
	row, err = mc.DecodeSingleResult(singleResult)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/DecodeSingleResult", err)
	}
	if row != nil {
		t.Fatalf("%s failed: should be nil", testName+"/DecodeSingleResult")
	}
}

func TestMongoConnect_DecodeSingleResultRaw(t *testing.T) {
	testName := "TestMongoConnect_DecodeSingleResultRaw"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	err := _initMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/_initMongoCollection", err)
	}

	item := map[string]interface{}{
		"username": "btnguyen2k",
		"email":    "me@domain.com",
		"version":  time.Now(),
		"actived":  true,
	}
	collection := mc.GetCollectionProxy(testMongoCollection)
	_, err = collection.InsertOne(nil, item)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/InsertOne", err)
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "insert", MetricsCatAll)

	singleResult := collection.FindOne(nil, bson.M{"username": "btnguyen2k"})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "find", MetricsCatAll)
	row, err := mc.DecodeSingleResultRaw(singleResult)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/DecodeSingleResultRaw", err)
	}
	if row == nil {
		t.Fatalf("%s failed: nil", testName+"/DecodeSingleResultRaw")
	}

	singleResult = collection.FindOne(nil, bson.M{"username": "not-exists"})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "find", MetricsCatAll)
	row, err = mc.DecodeSingleResultRaw(singleResult)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/DecodeSingleResultRaw", err)
	}
	if row != nil {
		t.Fatalf("%s failed: should be nil", testName+"/DecodeSingleResultRaw")
	}
}

func TestMongoConnect_DecodeResultCallback(t *testing.T) {
	testName := "TestMongoConnect_DecodeResultCallback"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	err := _initMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/_initMongoCollection", err)
	}

	collection := mc.GetCollectionProxy(testMongoCollection)
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
			t.Fatalf("%s failed: error [%s]", testName+"/InsertOne", err)
		}
		_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "insert", MetricsCatAll)
	}

	numDocs := 0
	cursor, err := collection.Find(nil, bson.M{"id": bson.M{"$gt": 4}})
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/Find", err)
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "find", MetricsCatAll)
	mc.DecodeResultCallback(nil, cursor, func(docNum int, doc bson.M, err error) bool {
		if err == nil {
			numDocs++
			return true
		}
		return false
	})
	if numDocs != 5 {
		t.Fatalf("%s failed: expected %d docs but received %d", testName+"/DecodeResultCallback", 5, numDocs)
	}
}

func TestMongoConnect_DecodeResultCallbackRaw(t *testing.T) {
	testName := "TestMongoConnect_DecodeResultCallbackRaw"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	err := _initMongoCollection(mc, testMongoCollection)
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/_initMongoCollection", err)
	}

	collection := mc.GetCollectionProxy(testMongoCollection)
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
			t.Fatalf("%s failed: error [%s]", testName+"/InsertOne", err)
		}
		_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "insert", MetricsCatAll)
	}

	numDocs := 0
	cursor, err := collection.Find(nil, bson.M{"id": bson.M{"$lte": 4}})
	if err != nil {
		t.Fatalf("%s failed: error [%s]", testName+"/Find", err)
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "find", MetricsCatAll)
	mc.DecodeResultCallbackRaw(nil, cursor, func(docNum int, doc []byte, err error) bool {
		if err == nil {
			numDocs++
			return true
		}
		return false
	})
	if numDocs != 5 {
		t.Fatalf("%s failed: expected %d docs but received %d", testName+"/DecodeResultCallbackRaw", 5, numDocs)
	}
}
