package mongo

import (
	"context"

	"github.com/btnguyen2k/prom"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// MongoClientProxy is a proxy that can be used as replacement for mongo.Client.
//
// This proxy overrides some functions from mongo.Client and automatically logs the execution metrics.
//
// Available since v0.3.0
type MongoClientProxy struct {
	*mongo.Client
	mc *MongoConnect
}

// Ping overrides mongo.Client.Ping to log execution metrics.
func (cp *MongoClientProxy) Ping(ctx context.Context, rp *readpref.ReadPref) error {
	cmd := cp.mc.NewCmdExecInfo()
	defer cp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName = "ping"
	err := cp.Client.Ping(ctx, rp)
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return err
}

// ListDatabases overrides mongo.Client.ListDatabases to log execution metrics.
func (cp *MongoClientProxy) ListDatabases(ctx context.Context, filter interface{}, opts ...*options.ListDatabasesOptions) (mongo.ListDatabasesResult, error) {
	cmd := cp.mc.NewCmdExecInfo()
	defer cp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest = "listDatabases", filter
	result, err := cp.Client.ListDatabases(ctx, filter, opts...)
	cmd.CmdResponse = result.Databases
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result, err
}

// ListDatabaseNames overrides mongo.Client.ListDatabaseNames to log execution metrics.
func (cp *MongoClientProxy) ListDatabaseNames(ctx context.Context, filter interface{}, opts ...*options.ListDatabasesOptions) ([]string, error) {
	cmd := cp.mc.NewCmdExecInfo()
	defer cp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest = "listDatabases", filter
	result, err := cp.Client.ListDatabaseNames(ctx, filter, opts...)
	cmd.CmdResponse = result
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result, err
}

// DatabaseProxy is similar to mongo.Client.Database, but returns a proxy that can be used as a replacement.
//
// See MongoDatabaseProxy.
func (cp *MongoClientProxy) DatabaseProxy(name string, opts ...*options.DatabaseOptions) *MongoDatabaseProxy {
	return &MongoDatabaseProxy{
		Database: cp.Client.Database(name, opts...),
		mc:       cp.mc,
		db:       name,
	}
}

/*----------------------------------------------------------------------*/

// MongoDatabaseProxy is a proxy that can be used as replacement for mongo.Database.
//
// This proxy overrides some functions from mongo.Database and automatically logs the execution metrics.
//
// Available since v0.3.0
type MongoDatabaseProxy struct {
	*mongo.Database
	mc *MongoConnect
	db string
}

// Aggregate overrides mongo.Database.Aggregate to log execution metrics.
func (dp *MongoDatabaseProxy) Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (*mongo.Cursor, error) {
	cmd := dp.mc.NewCmdExecInfo()
	defer dp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "aggregate", pipeline, bson.M{"db": dp.Database.Name()}
	result, err := dp.Database.Aggregate(ctx, pipeline, opts...)
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result, err
}

// RunCommand overrides mongo.Database.RunCommand to log execution metrics.
func (dp *MongoDatabaseProxy) RunCommand(ctx context.Context, runCommand interface{}, opts ...*options.RunCmdOptions) *mongo.SingleResult {
	cmd := dp.mc.NewCmdExecInfo()
	defer dp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "runCommand", runCommand, bson.M{"db": dp.Database.Name()}
	result := dp.Database.RunCommand(ctx, runCommand, opts...)
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, result.Err())
	return result
}

// RunCommandCursor overrides mongo.Database.RunCommandCursor to log execution metrics.
func (dp *MongoDatabaseProxy) RunCommandCursor(ctx context.Context, runCommand interface{}, opts ...*options.RunCmdOptions) (*mongo.Cursor, error) {
	cmd := dp.mc.NewCmdExecInfo()
	defer dp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "runCommand", runCommand, bson.M{"db": dp.Database.Name()}
	result, err := dp.Database.RunCommandCursor(ctx, runCommand, opts...)
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	if err == nil {
		cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, result.Err())
	}
	return result, err
}

// Drop overrides mongo.Database.Drop to log execution metrics.
func (dp *MongoDatabaseProxy) Drop(ctx context.Context) error {
	cmd := dp.mc.NewCmdExecInfo()
	defer dp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "dropDatabase", dp.db, bson.M{"db": dp.db}
	err := dp.Database.Drop(ctx)
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return err
}

// ListCollectionSpecifications overrides mongo.Database.ListCollectionSpecifications to log execution metrics.
func (dp *MongoDatabaseProxy) ListCollectionSpecifications(ctx context.Context, filter interface{}, opts ...*options.ListCollectionsOptions) ([]*mongo.CollectionSpecification, error) {
	cmd := dp.mc.NewCmdExecInfo()
	defer dp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "listCollections", filter, bson.M{"db": dp.Database.Name()}
	result, err := dp.Database.ListCollectionSpecifications(ctx, filter, opts...)
	cmd.CmdResponse = result
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result, err
}

// ListCollections overrides mongo.Database.ListCollections to log execution metrics.
func (dp *MongoDatabaseProxy) ListCollections(ctx context.Context, filter interface{}, opts ...*options.ListCollectionsOptions) (*mongo.Cursor, error) {
	cmd := dp.mc.NewCmdExecInfo()
	defer dp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "listCollections", filter, bson.M{"db": dp.Database.Name()}
	result, err := dp.Database.ListCollections(ctx, filter, opts...)
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	if err == nil {
		cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, result.Err())
	}
	return result, err
}

// ListCollectionNames overrides mongo.Database.ListCollectionNames to log execution metrics.
func (dp *MongoDatabaseProxy) ListCollectionNames(ctx context.Context, filter interface{}, opts ...*options.ListCollectionsOptions) ([]string, error) {
	cmd := dp.mc.NewCmdExecInfo()
	defer dp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "listCollections", filter, bson.M{"db": dp.Database.Name()}
	result, err := dp.Database.ListCollectionNames(ctx, filter, opts...)
	cmd.CmdResponse = result
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result, err
}

// CreateCollection overrides mongo.Database.CreateCollection to log execution metrics.
func (dp *MongoDatabaseProxy) CreateCollection(ctx context.Context, name string, opts ...*options.CreateCollectionOptions) error {
	cmd := dp.mc.NewCmdExecInfo()
	defer dp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "createCollection", name, bson.M{"db": dp.Database.Name()}
	err := dp.Database.CreateCollection(ctx, name, opts...)
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return err
}

// CreateView overrides mongo.Database.CreateView to log execution metrics.
func (dp *MongoDatabaseProxy) CreateView(ctx context.Context, viewName, viewOn string, pipeline interface{}, opts ...*options.CreateViewOptions) error {
	cmd := dp.mc.NewCmdExecInfo()
	defer dp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "createView", bson.M{"name": viewName, "on": viewOn, "pipeline": pipeline}, bson.M{"db": dp.Database.Name()}
	err := dp.Database.CreateView(ctx, viewName, viewOn, pipeline, opts...)
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return err
}

// CollectionProxy is similar to mongo.Database.Collection, but returns a proxy that can be used as a replacement.
//
// See MongoCollectionProxy.
func (dp *MongoDatabaseProxy) CollectionProxy(name string, opts ...*options.CollectionOptions) *MongoCollectionProxy {
	return &MongoCollectionProxy{
		Collection: dp.Database.Collection(name, opts...),
		mc:         dp.mc,
		db:         dp.db,
		collection: name,
	}
}

/*----------------------------------------------------------------------*/

// MongoCollectionProxy is a proxy that can be used as replacement for mongo.Collection.
//
// This proxy overrides some functions from mongo.Collection and automatically logs the execution metrics.
//
// Available since v0.3.0
type MongoCollectionProxy struct {
	*mongo.Collection
	mc             *MongoConnect
	db, collection string
}

// BulkWrite overrides mongo.Collection.BulkWrite to log execution metrics.
func (cp *MongoCollectionProxy) BulkWrite(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
	cmd := cp.mc.NewCmdExecInfo()
	defer cp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "bulkWrite", models, bson.M{"db": cp.db, "collection": cp.collection}
	result, err := cp.Collection.BulkWrite(ctx, models, opts...)
	cmd.CmdResponse = result
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result, err
}

// InsertOne overrides mongo.Collection.InsertOne to log execution metrics.
func (cp *MongoCollectionProxy) InsertOne(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
	cmd := cp.mc.NewCmdExecInfo()
	defer cp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "insert", []interface{}{document}, bson.M{"db": cp.db, "collection": cp.collection}
	result, err := cp.Collection.InsertOne(ctx, document, opts...)
	if result != nil {
		cmd.CmdResponse = []interface{}{result.InsertedID}
	}
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result, err
}

// InsertMany overrides mongo.Collection.InsertMany to log execution metrics.
func (cp *MongoCollectionProxy) InsertMany(ctx context.Context, documents []interface{}, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
	cmd := cp.mc.NewCmdExecInfo()
	defer cp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "insert", documents, bson.M{"db": cp.db, "collection": cp.collection}
	result, err := cp.Collection.InsertMany(ctx, documents, opts...)
	if result != nil {
		cmd.CmdResponse = result.InsertedIDs
	}
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result, err
}

// DeleteOne overrides mongo.Collection.DeleteOne to log execution metrics.
func (cp *MongoCollectionProxy) DeleteOne(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	cmd := cp.mc.NewCmdExecInfo()
	defer cp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "delete", filter, bson.M{"db": cp.db, "collection": cp.collection}
	result, err := cp.Collection.DeleteOne(ctx, filter, opts...)
	cmd.CmdResponse = result
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result, err
}

// DeleteMany overrides mongo.Collection.DeleteMany to log execution metrics.
func (cp *MongoCollectionProxy) DeleteMany(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	cmd := cp.mc.NewCmdExecInfo()
	defer cp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "delete", filter, bson.M{"db": cp.db, "collection": cp.collection}
	result, err := cp.Collection.DeleteMany(ctx, filter, opts...)
	cmd.CmdResponse = result
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result, err
}

// UpdateByID overrides mongo.Collection.UpdateByID to log execution metrics.
func (cp *MongoCollectionProxy) UpdateByID(ctx context.Context, id interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	cmd := cp.mc.NewCmdExecInfo()
	defer cp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "update", bson.M{"id": id, "update": update}, bson.M{"db": cp.db, "collection": cp.collection}
	result, err := cp.Collection.UpdateByID(ctx, id, update, opts...)
	cmd.CmdResponse = result
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result, err
}

// UpdateOne overrides mongo.Collection.UpdateOne to log execution metrics.
func (cp *MongoCollectionProxy) UpdateOne(ctx context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	cmd := cp.mc.NewCmdExecInfo()
	defer cp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "update", bson.M{"filter": filter, "update": update}, bson.M{"db": cp.db, "collection": cp.collection}
	result, err := cp.Collection.UpdateOne(ctx, filter, update, opts...)
	cmd.CmdResponse = result
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result, err
}

// UpdateMany overrides mongo.Collection.UpdateMany to log execution metrics.
func (cp *MongoCollectionProxy) UpdateMany(ctx context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	cmd := cp.mc.NewCmdExecInfo()
	defer cp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "update", bson.M{"filter": filter, "update": update}, bson.M{"db": cp.db, "collection": cp.collection}
	result, err := cp.Collection.UpdateMany(ctx, filter, update, opts...)
	cmd.CmdResponse = result
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result, err
}

// ReplaceOne overrides mongo.Collection.ReplaceOne to log execution metrics.
func (cp *MongoCollectionProxy) ReplaceOne(ctx context.Context, filter interface{}, replacement interface{}, opts ...*options.ReplaceOptions) (*mongo.UpdateResult, error) {
	cmd := cp.mc.NewCmdExecInfo()
	defer cp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "update", bson.M{"filter": filter, "replacement": replacement}, bson.M{"db": cp.db, "collection": cp.collection}
	result, err := cp.Collection.ReplaceOne(ctx, filter, replacement, opts...)
	cmd.CmdResponse = result
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result, err
}

// Aggregate overrides mongo.Collection.Aggregate to log execution metrics.
func (cp *MongoCollectionProxy) Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (*mongo.Cursor, error) {
	cmd := cp.mc.NewCmdExecInfo()
	defer cp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "aggregate", pipeline, bson.M{"db": cp.db, "collection": cp.collection}
	result, err := cp.Collection.Aggregate(ctx, pipeline, opts...)
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result, err
}

// CountDocuments overrides mongo.Collection.CountDocuments to log execution metrics.
func (cp *MongoCollectionProxy) CountDocuments(ctx context.Context, filter interface{}, opts ...*options.CountOptions) (int64, error) {
	cmd := cp.mc.NewCmdExecInfo()
	defer cp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "aggregate", filter, bson.M{"db": cp.db, "collection": cp.collection}
	result, err := cp.Collection.CountDocuments(ctx, filter, opts...)
	cmd.CmdResponse = result
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result, err
}

// EstimatedDocumentCount overrides mongo.Collection.EstimatedDocumentCount to log execution metrics.
func (cp *MongoCollectionProxy) EstimatedDocumentCount(ctx context.Context, opts ...*options.EstimatedDocumentCountOptions) (int64, error) {
	cmd := cp.mc.NewCmdExecInfo()
	defer cp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdMeta = "count", bson.M{"db": cp.db, "collection": cp.collection}
	result, err := cp.Collection.EstimatedDocumentCount(ctx, opts...)
	cmd.CmdResponse = result
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result, err
}

// Distinct overrides mongo.Collection.Distinct to log execution metrics.
func (cp *MongoCollectionProxy) Distinct(ctx context.Context, fieldName string, filter interface{}, opts ...*options.DistinctOptions) ([]interface{}, error) {
	cmd := cp.mc.NewCmdExecInfo()
	defer cp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "distinct", bson.M{"field": fieldName, "filter": filter}, bson.M{"db": cp.db, "collection": cp.collection}
	result, err := cp.Collection.Distinct(ctx, fieldName, filter, opts...)
	cmd.CmdResponse = result
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result, err
}

// Find overrides mongo.Collection.Find to log execution metrics.
func (cp *MongoCollectionProxy) Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (cur *mongo.Cursor, err error) {
	cmd := cp.mc.NewCmdExecInfo()
	defer cp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "find", filter, bson.M{"db": cp.db, "collection": cp.collection}
	result, err := cp.Collection.Find(ctx, filter, opts...)
	if err == mongo.ErrNoDocuments {
		cmd.CmdResponse = bson.M{"error": mongo.ErrNoDocuments.Error()}
		cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, nil)
	} else {
		cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	}

	return result, err
}

// FindOne overrides mongo.Collection.FindOne to log execution metrics.
func (cp *MongoCollectionProxy) FindOne(ctx context.Context, filter interface{}, opts ...*options.FindOneOptions) *mongo.SingleResult {
	cmd := cp.mc.NewCmdExecInfo()
	defer cp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "find", filter, bson.M{"db": cp.db, "collection": cp.collection}
	result := cp.Collection.FindOne(ctx, filter, opts...)
	err := result.Err()
	if err == mongo.ErrNoDocuments {
		cmd.CmdResponse = bson.M{"error": mongo.ErrNoDocuments.Error()}
		cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, nil)
	} else {
		cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	}
	return result
}

// FindOneAndDelete overrides mongo.Collection.FindOneAndDelete to log execution metrics.
func (cp *MongoCollectionProxy) FindOneAndDelete(ctx context.Context, filter interface{}, opts ...*options.FindOneAndDeleteOptions) *mongo.SingleResult {
	cmd := cp.mc.NewCmdExecInfo()
	defer cp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "findAndModify", bson.M{"filter": filter, "cmd": "delete"}, bson.M{"db": cp.db, "collection": cp.collection}
	result := cp.Collection.FindOneAndDelete(ctx, filter, opts...)
	err := result.Err()
	if err == mongo.ErrNoDocuments {
		cmd.CmdResponse = bson.M{"error": mongo.ErrNoDocuments.Error()}
		cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, nil)
	} else {
		cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	}
	return result
}

// FindOneAndReplace overrides mongo.Collection.FindOneAndReplace to log execution metrics.
func (cp *MongoCollectionProxy) FindOneAndReplace(ctx context.Context, filter interface{}, replacement interface{}, opts ...*options.FindOneAndReplaceOptions) *mongo.SingleResult {
	cmd := cp.mc.NewCmdExecInfo()
	defer cp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "findAndModify", bson.M{"filter": filter, "cmd": "replace", "replacement": replacement}, bson.M{"db": cp.db, "collection": cp.collection}
	result := cp.Collection.FindOneAndReplace(ctx, filter, replacement, opts...)
	err := result.Err()
	if err == mongo.ErrNoDocuments {
		cmd.CmdResponse = bson.M{"error": mongo.ErrNoDocuments.Error()}
		cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, nil)
	} else {
		cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	}
	return result
}

// FindOneAndUpdate overrides mongo.Collection.FindOneAndUpdate to log execution metrics.
func (cp *MongoCollectionProxy) FindOneAndUpdate(ctx context.Context, filter interface{}, update interface{}, opts ...*options.FindOneAndUpdateOptions) *mongo.SingleResult {
	cmd := cp.mc.NewCmdExecInfo()
	defer cp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "findAndModify", bson.M{"filter": filter, "cmd": "update", "update": update}, bson.M{"db": cp.db, "collection": cp.collection}
	result := cp.Collection.FindOneAndUpdate(ctx, filter, update, opts...)
	err := result.Err()
	if err == mongo.ErrNoDocuments {
		cmd.CmdResponse = bson.M{"error": mongo.ErrNoDocuments.Error()}
		cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, nil)
	} else {
		cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	}
	return result
}

// Drop overrides mongo.Collection.Drop to log execution metrics.
func (cp *MongoCollectionProxy) Drop(ctx context.Context) error {
	cmd := cp.mc.NewCmdExecInfo()
	defer cp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "dropCollection", cp.collection, bson.M{"db": cp.db, "collection": cp.collection}
	err := cp.Collection.Drop(ctx)
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return err
}

// IndexesProxy is similar to mongo.Collection.Indexes, but returns a proxy that can be used as a replacement.
//
// See MongoIndexViewProxy.
func (cp *MongoCollectionProxy) IndexesProxy() MongoIndexViewProxy {
	return MongoIndexViewProxy{
		IndexView:  cp.Collection.Indexes(),
		mc:         cp.mc,
		db:         cp.db,
		collection: cp.collection,
	}
}

/*----------------------------------------------------------------------*/

// MongoIndexViewProxy is a proxy that can be used as replacement for mongo.IndexView.
//
// This proxy overrides some functions from mongo.IndexView and automatically logs the execution metrics.
//
// Available since v0.3.0
type MongoIndexViewProxy struct {
	mongo.IndexView
	mc             *MongoConnect
	db, collection string
}

// List overrides mongo.IndexView.List to log execution metrics.
func (ivp MongoIndexViewProxy) List(ctx context.Context, opts ...*options.ListIndexesOptions) (*mongo.Cursor, error) {
	cmd := ivp.mc.NewCmdExecInfo()
	defer ivp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdMeta = "listIndexes", bson.M{"db": ivp.db, "collection": ivp.collection}
	result, err := ivp.IndexView.List(ctx, opts...)
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	if err == nil {
		cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, result.Err())
	}
	return result, err
}

// ListSpecifications overrides mongo.IndexView.ListSpecifications to log execution metrics.
func (ivp MongoIndexViewProxy) ListSpecifications(ctx context.Context, opts ...*options.ListIndexesOptions) ([]*mongo.IndexSpecification, error) {
	cmd := ivp.mc.NewCmdExecInfo()
	defer ivp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdMeta = "listIndexes", bson.M{"db": ivp.db, "collection": ivp.collection}
	result, err := ivp.IndexView.ListSpecifications(ctx, opts...)
	cmd.CmdResponse = result
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result, err
}

// CreateOne overrides mongo.IndexView.CreateOne to log execution metrics.
func (ivp MongoIndexViewProxy) CreateOne(ctx context.Context, model mongo.IndexModel, opts ...*options.CreateIndexesOptions) (string, error) {
	names, err := ivp.CreateMany(ctx, []mongo.IndexModel{model}, opts...)
	if err != nil {
		return "", err
	}
	return names[0], nil
}

// CreateMany overrides mongo.IndexView.CreateMany to log execution metrics.
func (ivp MongoIndexViewProxy) CreateMany(ctx context.Context, models []mongo.IndexModel, opts ...*options.CreateIndexesOptions) ([]string, error) {
	cmd := ivp.mc.NewCmdExecInfo()
	defer ivp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "createIndexes", models, bson.M{"db": ivp.db, "collection": ivp.collection}
	result, err := ivp.IndexView.CreateMany(ctx, models, opts...)
	cmd.CmdResponse = result
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result, err
}

// DropOne overrides mongo.IndexView.DropOne to log execution metrics.
func (ivp MongoIndexViewProxy) DropOne(ctx context.Context, name string, opts ...*options.DropIndexesOptions) (bson.Raw, error) {
	cmd := ivp.mc.NewCmdExecInfo()
	defer ivp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "dropIndexes", name, bson.M{"db": ivp.db, "collection": ivp.collection}
	result, err := ivp.IndexView.DropOne(ctx, name, opts...)
	if result != nil {
		cmd.CmdResponse = result.String()
	}
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result, err
}

// DropAll overrides mongo.IndexView.DropAll to log execution metrics.
func (ivp MongoIndexViewProxy) DropAll(ctx context.Context, opts ...*options.DropIndexesOptions) (bson.Raw, error) {
	cmd := ivp.mc.NewCmdExecInfo()
	defer ivp.mc.LogMetrics(prom.MetricsCatAll, cmd)
	cmd.CmdName, cmd.CmdRequest, cmd.CmdMeta = "dropIndexes", "*", bson.M{"db": ivp.db, "collection": ivp.collection}
	result, err := ivp.IndexView.DropAll(ctx, opts...)
	if result != nil {
		cmd.CmdResponse = result.String()
	}
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result, err
}
