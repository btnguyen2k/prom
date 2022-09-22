package prom

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func TestMongoClientProxy_Ping(t *testing.T) {
	testName := "TestMongoClientProxy_Ping"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	cp := mc.GetMongoClientProxy()
	err := cp.Ping(mc.NewContext(), readpref.Primary())
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "ping", MetricsCatAll)
}

func TestMongoClientProxy_ListDatabases(t *testing.T) {
	testName := "TestMongoClientProxy_ListDatabases"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	cp := mc.GetMongoClientProxy()
	cp.ListDatabases(mc.NewContext(), bson.M{})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "listDatabases", MetricsCatAll)
}

func TestMongoClientProxy_ListDatabaseNames(t *testing.T) {
	testName := "TestMongoClientProxy_ListDatabaseNames"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	cp := mc.GetMongoClientProxy()
	cp.ListDatabaseNames(mc.NewContext(), bson.M{})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "listDatabases", MetricsCatAll)
}

func TestMongoClientProxy_DatabaseProxy(t *testing.T) {
	testName := "TestMongoClientProxy_DatabaseProxy"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	cp := mc.GetMongoClientProxy()
	dp := cp.DatabaseProxy(mc.db)
	if dp == nil {
		t.Fatalf("%s failed: nil", testName)
	}
}

/*----------------------------------------------------------------------*/

func TestMongoDatabaseProxy_Aggregate(t *testing.T) {
	testName := "TestMongoClientProxy_Ping"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	dp := mc.GetDatabaseProxy()
	result, _ := dp.Aggregate(mc.NewContext(), bson.A{bson.M{"$listLocalSessions": bson.M{"allUsers": true}}})
	if result != nil {
		result.Close(mc.NewContext())
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "aggregate", MetricsCatAll)
}

func TestMongoDatabaseProxy_RunCommand(t *testing.T) {
	testName := "TestMongoDatabaseProxy_RunCommand"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	dp := mc.GetDatabaseProxy()
	dp.RunCommand(mc.NewContext(), bson.M{"hello": 1})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "runCommand", MetricsCatAll)
}

func TestMongoDatabaseProxy_RunCommandCursor(t *testing.T) {
	testName := "TestMongoDatabaseProxy_RunCommandCursor"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	dp := mc.GetDatabaseProxy()
	result, _ := dp.RunCommandCursor(mc.NewContext(), bson.D{
		{"aggregate", 1},
		{"pipeline", bson.A{bson.M{"$listLocalSessions": bson.M{"allUsers": true}}}},
		{"cursor", bson.M{}},
	})
	if result != nil {
		result.Close(mc.NewContext())
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "runCommand", MetricsCatAll)
}

func TestMongoDatabaseProxy_ListCollectionSpecifications(t *testing.T) {
	testName := "TestMongoDatabaseProxy_ListCollectionSpecifications"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	dp := mc.GetDatabaseProxy()
	dp.ListCollectionSpecifications(mc.NewContext(), bson.M{})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "listCollections", MetricsCatAll)
}

func TestMongoDatabaseProxy_ListCollections(t *testing.T) {
	testName := "TestMongoDatabaseProxy_ListCollections"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	dp := mc.GetDatabaseProxy()
	dp.ListCollections(mc.NewContext(), bson.M{})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "listCollections", MetricsCatAll)
}

func TestMongoDatabaseProxy_ListCollectionNames(t *testing.T) {
	testName := "TestMongoDatabaseProxy_ListCollectionNames"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	dp := mc.GetDatabaseProxy()
	dp.ListCollectionNames(mc.NewContext(), bson.M{})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "listCollections", MetricsCatAll)
}

func TestMongoDatabaseProxy_CreateCollection(t *testing.T) {
	testName := "TestMongoDatabaseProxy_CreateCollection"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	dp := mc.GetDatabaseProxy()
	dp.Collection(testMongoCollection).Drop(mc.NewContext())
	dp.CreateCollection(mc.NewContext(), testMongoCollection)
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "createCollection", MetricsCatAll)
}

func TestMongoDatabaseProxy_CreateView(t *testing.T) {
	testName := "TestMongoDatabaseProxy_CreateView"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	dp := mc.GetDatabaseProxy()
	dp.Collection(testMongoCollection + "_view").Drop(mc.NewContext())
	dp.CreateView(mc.NewContext(), testMongoCollection+"_view", testMongoCollection, bson.A{bson.M{"$match": bson.M{"year": 1}}})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "createView", MetricsCatAll)
}

func TestMongoDatabaseProxy_CollectionProxy(t *testing.T) {
	testName := "TestMongoDatabaseProxy_CollectionProxy"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	dp := mc.GetDatabaseProxy()
	cp := dp.CollectionProxy(testMongoCollection)
	if cp == nil {
		t.Fatalf("%s failed: nil", testName)
	}
}

/*----------------------------------------------------------------------*/

func TestMongoCollectionProxy_BulkWrite(t *testing.T) {
	testName := "TestMongoCollectionProxy_BulkWrite"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	mc.CreateCollection(testMongoCollection)
	cp := mc.GetCollectionProxy(testMongoCollection)
	documents := []mongo.WriteModel{mongo.NewInsertOneModel().SetDocument(bson.M{"year": 1, "name": "Thanh Nguyen"})}
	cp.BulkWrite(mc.NewContext(), documents)
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "bulkWrite", MetricsCatAll)
}

func TestMongoCollectionProxy_InsertOne(t *testing.T) {
	testName := "TestMongoCollectionProxy_InsertOne"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	mc.CreateCollection(testMongoCollection)
	cp := mc.GetCollectionProxy(testMongoCollection)
	document := bson.M{"year": 1, "name": "Thanh Nguyen"}
	cp.InsertOne(mc.NewContext(), document)
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "insert", MetricsCatAll)
}

func TestMongoCollectionProxy_InsertMany(t *testing.T) {
	testName := "TestMongoCollectionProxy_InsertMany"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	mc.CreateCollection(testMongoCollection)
	cp := mc.GetCollectionProxy(testMongoCollection)
	document1 := bson.M{"year": 1, "name": "Thanh Nguyen"}
	document2 := bson.M{"year": 2, "name": "Tom Nguyen"}
	cp.InsertMany(mc.NewContext(), bson.A{document1, document2})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "insert", MetricsCatAll)
}

func TestMongoCollectionProxy_DeleteOne(t *testing.T) {
	testName := "TestMongoCollectionProxy_DeleteOne"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	mc.CreateCollection(testMongoCollection)
	cp := mc.GetCollectionProxy(testMongoCollection)
	cp.DeleteOne(mc.NewContext(), bson.M{})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "delete", MetricsCatAll)
}

func TestMongoCollectionProxy_DeleteMany(t *testing.T) {
	testName := "TestMongoCollectionProxy_DeleteMany"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	mc.CreateCollection(testMongoCollection)
	cp := mc.GetCollectionProxy(testMongoCollection)
	cp.DeleteMany(mc.NewContext(), bson.M{})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "delete", MetricsCatAll)
}

func TestMongoCollectionProxy_UpdateByID(t *testing.T) {
	testName := "TestMongoCollectionProxy_UpdateByID"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	mc.CreateCollection(testMongoCollection)
	cp := mc.GetCollectionProxy(testMongoCollection)
	cp.UpdateByID(mc.NewContext(), "1", bson.M{"$set": bson.M{"year": 1}})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "update", MetricsCatAll)
}

func TestMongoCollectionProxy_UpdateOne(t *testing.T) {
	testName := "TestMongoCollectionProxy_UpdateOne"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	mc.CreateCollection(testMongoCollection)
	cp := mc.GetCollectionProxy(testMongoCollection)
	cp.UpdateOne(mc.NewContext(), bson.M{"_id": "1"}, bson.M{"$set": bson.M{"year": 1}})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "update", MetricsCatAll)
}

func TestMongoCollectionProxy_UpdateMany(t *testing.T) {
	testName := "TestMongoCollectionProxy_UpdateMany"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	mc.CreateCollection(testMongoCollection)
	cp := mc.GetCollectionProxy(testMongoCollection)
	cp.UpdateMany(mc.NewContext(), bson.M{"_id": "1"}, bson.M{"$set": bson.M{"year": 1}})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "update", MetricsCatAll)
}

func TestMongoCollectionProxy_ReplaceOne(t *testing.T) {
	testName := "TestMongoCollectionProxy_ReplaceOne"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	mc.CreateCollection(testMongoCollection)
	cp := mc.GetCollectionProxy(testMongoCollection)
	cp.ReplaceOne(mc.NewContext(), bson.M{"_id": "1"}, bson.M{"year": 1})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "update", MetricsCatAll)
}

func TestMongoCollectionProxy_Aggregate(t *testing.T) {
	testName := "TestMongoCollectionProxy_Aggregate"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	mc.CreateCollection(testMongoCollection)
	cp := mc.GetCollectionProxy(testMongoCollection)
	cp.Aggregate(mc.NewContext(), bson.A{bson.M{"$count": "year"}})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "aggregate", MetricsCatAll)
}

func TestMongoCollectionProxy_CountDocuments(t *testing.T) {
	testName := "TestMongoCollectionProxy_CountDocuments"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	mc.CreateCollection(testMongoCollection)
	cp := mc.GetCollectionProxy(testMongoCollection)
	cp.CountDocuments(mc.NewContext(), bson.M{"year": 1})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "aggregate", MetricsCatAll)
}

func TestMongoCollectionProxy_EstimatedDocumentCount(t *testing.T) {
	testName := "TestMongoCollectionProxy_EstimatedDocumentCount"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	mc.CreateCollection(testMongoCollection)
	cp := mc.GetCollectionProxy(testMongoCollection)
	cp.EstimatedDocumentCount(mc.NewContext())
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "count", MetricsCatAll)
}

func TestMongoCollectionProxy_Distinct(t *testing.T) {
	testName := "TestMongoCollectionProxy_Distinct"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	mc.CreateCollection(testMongoCollection)
	cp := mc.GetCollectionProxy(testMongoCollection)
	cp.Distinct(mc.NewContext(), "year", bson.D{})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "distinct", MetricsCatAll)
}

func TestMongoCollectionProxy_Find(t *testing.T) {
	testName := "TestMongoCollectionProxy_Find"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	mc.CreateCollection(testMongoCollection)
	cp := mc.GetCollectionProxy(testMongoCollection)
	cp.Find(mc.NewContext(), bson.M{"year": 1})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "find", MetricsCatAll)
}

func TestMongoCollectionProxy_FindOne(t *testing.T) {
	testName := "TestMongoCollectionProxy_FindOne"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	mc.CreateCollection(testMongoCollection)
	cp := mc.GetCollectionProxy(testMongoCollection)
	cp.FindOne(mc.NewContext(), bson.M{"year": 1})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "find", MetricsCatAll)
}

func TestMongoCollectionProxy_FindOneAndDelete(t *testing.T) {
	testName := "TestMongoCollectionProxy_FindOneAndDelete"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	mc.CreateCollection(testMongoCollection)
	cp := mc.GetCollectionProxy(testMongoCollection)
	cp.FindOneAndDelete(mc.NewContext(), bson.M{"year": 1})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "findAndModify", MetricsCatAll)
}

func TestMongoCollectionProxy_FindOneAndReplace(t *testing.T) {
	testName := "TestMongoCollectionProxy_FindOneAndReplace"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	mc.CreateCollection(testMongoCollection)
	cp := mc.GetCollectionProxy(testMongoCollection)
	cp.FindOneAndReplace(mc.NewContext(), bson.M{"_id": "1"}, bson.M{"name": "Thanh Nguyen", "year": 2})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "findAndModify", MetricsCatAll)
}

func TestMongoCollectionProxy_FindOneAndUpdate(t *testing.T) {
	testName := "TestMongoCollectionProxy_FindOneAndUpdate"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	mc.CreateCollection(testMongoCollection)
	cp := mc.GetCollectionProxy(testMongoCollection)
	cp.FindOneAndUpdate(mc.NewContext(), bson.M{"_id": "1"}, bson.M{"$set": bson.M{"year": 2}})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "findAndModify", MetricsCatAll)
}

func TestMongoCollectionProxy_Drop(t *testing.T) {
	testName := "TestMongoCollectionProxy_Drop"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	cp := mc.GetCollectionProxy(testMongoCollection)

	mc.CreateCollection(testMongoCollection)
	cp.Drop(mc.NewContext())
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "dropCollection", MetricsCatAll)

	cp.Drop(mc.NewContext())
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "dropCollection", MetricsCatAll)
}

func TestMongoCollectionProxy_IndexesProxy(t *testing.T) {
	testName := "TestMongoCollectionProxy_IndexesProxy"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	cp := mc.GetCollectionProxy(testMongoCollection)
	ivp := cp.IndexesProxy()
	if ivp.db != cp.db || ivp.collection != cp.collection || ivp.mc != cp.mc {
		t.Fatalf("%s failed", testName)
	}
}

/*----------------------------------------------------------------------*/

func TestMongoIndexViewProxy_List(t *testing.T) {
	testName := "TestMongoIndexViewProxy_List"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	ivp := mc.GetCollectionProxy(testMongoCollection).IndexesProxy()
	result, _ := ivp.List(mc.NewContext())
	if result != nil {
		result.Close(mc.NewContext())
	}
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "listIndexes", MetricsCatAll)
}

func TestMongoIndexViewProxy_ListSpecifications(t *testing.T) {
	testName := "TestMongoIndexViewProxy_ListSpecifications"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	ivp := mc.GetCollectionProxy(testMongoCollection).IndexesProxy()
	ivp.ListSpecifications(mc.NewContext())
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "listIndexes", MetricsCatAll)
}

func TestMongoIndexViewProxy_CreateOne(t *testing.T) {
	testName := "TestMongoIndexViewProxy_CreateOne"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	mc.DropCollection(testMongoCollection)
	mc.CreateCollection(testMongoCollection)
	ivp := mc.GetCollectionProxy(testMongoCollection).IndexesProxy()
	ivp.CreateOne(mc.NewContext(), mongo.IndexModel{Keys: bson.M{"year": 1}})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "createIndexes", MetricsCatAll)
}

func TestMongoIndexViewProxy_CreateMany(t *testing.T) {
	testName := "TestMongoIndexViewProxy_CreateMany"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	mc.DropCollection(testMongoCollection)
	mc.CreateCollection(testMongoCollection)
	ivp := mc.GetCollectionProxy(testMongoCollection).IndexesProxy()
	ivp.CreateMany(mc.NewContext(), []mongo.IndexModel{{Keys: bson.M{"year": 1}}, {Keys: bson.M{"grade": 1}}})
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "createIndexes", MetricsCatAll)
}

func TestMongoIndexViewProxy_DropOne(t *testing.T) {
	testName := "TestMongoIndexViewProxy_DropOne"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	mc.DropCollection(testMongoCollection)
	mc.CreateCollection(testMongoCollection)
	ivp := mc.GetCollectionProxy(testMongoCollection).IndexesProxy()
	idxName := "idx_year"
	ivp.CreateOne(mc.NewContext(), mongo.IndexModel{Keys: bson.M{"year": 1}, Options: &options.IndexOptions{Name: &idxName}})
	ivp.DropOne(mc.NewContext(), idxName)
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "dropIndexes", MetricsCatAll)
}

func TestMongoIndexViewProxy_DropAll(t *testing.T) {
	testName := "TestMongoIndexViewProxy_DropAll"
	mc := _createMongoConnect(t, testName)
	defer mc.Close(nil)

	mc.DropCollection(testMongoCollection)
	mc.CreateCollection(testMongoCollection)
	ivp := mc.GetCollectionProxy(testMongoCollection).IndexesProxy()
	ivp.CreateMany(mc.NewContext(), []mongo.IndexModel{{Keys: bson.M{"year": 1}}, {Keys: bson.M{"grade": 1}}})
	ivp.DropAll(mc.NewContext())
	_mcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, mc, "dropIndexes", MetricsCatAll)
}
