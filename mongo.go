package prom

import (
	"context"
	"encoding/json"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

/*
MongoConnect holds a MongoDB client (https://github.com/mongodb/mongo-go-driver) that can be shared within the application.
*/
type MongoConnect struct {
	url       string        // connection url, including authentication credentials
	db        string        // database name
	client    *mongo.Client // client instance
	timeoutMs int           // default timeout for db operations, in milliseconds
}

/*
NewMongoConnect constructs a new MongoConnect instance.

Parameters:

  - url             : connection url, including authentication credentials
  - db              : name of database to connect to
  - defaultTimeoutMs: default timeout for db operations, in milliseconds

Return: the MongoConnect instance and error (if any). Note:

  - In case of connection error: this function returns the MongoConnect instance and the error.
  - Other error: this function returns (nil, error)
*/
func NewMongoConnect(url, db string, defaultTimeoutMs int) (*MongoConnect, error) {
	if defaultTimeoutMs < 0 {
		defaultTimeoutMs = 0
	}
	m := &MongoConnect{
		url:       url,
		db:        db,
		timeoutMs: defaultTimeoutMs,
	}
	client, err := mongo.NewClient(options.Client().ApplyURI(m.url))
	if err != nil {
		return nil, err
	}
	m.client = client
	ctx, _ := m.NewBackgroundContext()
	return m, m.client.Connect(ctx)
}

/*
Disconnect closes all connections associated with the underlying MongoDB client.

Available since v0.1.0
*/
func (m *MongoConnect) Disconnect(ctx context.Context) error {
	if ctx == nil {
		ctx, _ = m.NewBackgroundContext()
	}
	return m.client.Disconnect(ctx)
}

/*
DecodeSingleResult transforms 'mongo.SingleResult' to 'bson.M'.
*/
func (m *MongoConnect) DecodeSingleResult(dbResult *mongo.SingleResult) (bson.M, error) {
	if dbResult.Err() != nil {
		return nil, dbResult.Err()
	}
	var row bson.M
	err := dbResult.Decode(&row)
	if err != nil && err != mongo.ErrNoDocuments {
		// error
		return nil, err
	}
	if err != nil && err == mongo.ErrNoDocuments {
		// no document found
		return nil, nil
	}
	return row, nil
}

/*
DecodeResultCallback loops through the cursor and, for each fetched document, passes it to the callback function.
Note:

	- docNum is 1-based, and scoped to the cursor context. This function does not close the cursor!
	- If callback function returns 'false', the loop will break and DecodeResultCallback returns.
*/
func (m *MongoConnect) DecodeResultCallback(ctx context.Context, cursor *mongo.Cursor, callback func(docNum int, doc bson.M, err error) bool) {
	for dNum := 1; cursor.Next(ctx); dNum++ {
		var d bson.M
		err := cursor.Decode(&d)
		if err != nil {
			if !callback(dNum, nil, err) {
				break
			}
		} else {
			if !callback(dNum, d, nil) {
				break
			}
		}
	}
}

/*
DecodeSingleResultRaw transforms 'mongo.SingleResult' to raw JSON data.

Availability: this method is available since v0.0.3.1
*/
func (m *MongoConnect) DecodeSingleResultRaw(dbResult *mongo.SingleResult) ([]byte, error) {
	doc, err := m.DecodeSingleResult(dbResult)
	if doc == nil || err != nil {
		return nil, err
	}
	return json.Marshal(doc)
}

/*
DecodeResultCallbackRaw loops through the cursor and, for each fetched document, passes it to the callback function.
Note:

	- docNum is 1-based, and scoped to the cursor context. This function does not close the cursor!
	- If callback function returns 'false', the loop will break and DecodeResultCallbackRaw returns.

Availability: this method is available since v0.0.3.1
*/
func (m *MongoConnect) DecodeResultCallbackRaw(ctx context.Context, cursor *mongo.Cursor, callback func(docNum int, doc []byte, err error) bool) {
	for dNum := 1; cursor.Next(ctx); dNum++ {
		var d bson.M
		err := cursor.Decode(&d)
		if err != nil {
			if !callback(dNum, nil, err) {
				break
			}
		} else {
			raw, err := json.Marshal(d)
			if !callback(dNum, raw, err) {
				break
			}
		}
	}
}

/*
NewBackgroundContext creates a new background context with specified timeout in milliseconds.
If there is no specified timeout, or timeout value is less than or equal to 0, the default timeout is used.
*/
func (m *MongoConnect) NewBackgroundContext(timeoutMs ...int) (context.Context, context.CancelFunc) {
	d := m.timeoutMs
	if len(timeoutMs) > 0 && timeoutMs[0] > 0 {
		d = timeoutMs[0]
	}
	return context.WithTimeout(context.Background(), time.Duration(d)*time.Millisecond)
}

/*
Ping tries to send a "ping" request to MongoDB server.
If there is no specified timeout, or timeout value is less than or equal to 0, the default timeout is used.
*/
func (m *MongoConnect) Ping(timeoutMs ...int) error {
	ctx, _ := m.NewBackgroundContext(timeoutMs...)
	return m.client.Ping(ctx, readpref.Primary())
}

/*
IsConnected returns true if the connection to MongoDB has established.
*/
func (m *MongoConnect) IsConnected() bool {
	err := m.Ping()
	return err == nil
}

/*
GetMongoClient returns the underlying MongoDB client instance.
*/
func (m *MongoConnect) GetMongoClient() *mongo.Client {
	return m.client
}

/*
GetDatabase returns the database object attached to this MongoConnect.
*/
func (m *MongoConnect) GetDatabase(opts ...*options.DatabaseOptions) *mongo.Database {
	return m.client.Database(m.db, opts...)
}

/*
HasDatabase checks if a database exists on MongoDB server.
*/
func (m *MongoConnect) HasDatabase(dbName string, opts ...*options.ListDatabasesOptions) (bool, error) {
	dbList, err := m.client.ListDatabaseNames(nil, bson.M{"name": dbName}, opts...)
	if err != nil {
		return false, err
	}
	return len(dbList) > 0, nil
}

/*
GetCollection returns the collection object specified by 'collectionName'.
*/
func (m *MongoConnect) GetCollection(collectionName string, opts ...*options.CollectionOptions) *mongo.Collection {
	db := m.GetDatabase()
	return db.Collection(collectionName, opts...)
}

/*
HasCollection checks if a collection exists in the database.
*/
func (m *MongoConnect) HasCollection(collectionName string, opts ...*options.ListCollectionsOptions) (bool, error) {
	ctx, _ := m.NewBackgroundContext()
	collectionList, err := m.GetDatabase().ListCollections(ctx, bson.M{"name": collectionName}, opts...)
	defer collectionList.Close(ctx)
	if err != nil {
		return false, err
	}
	if collectionList.Err() != nil {
		return false, collectionList.Err()
	}
	return collectionList.Next(ctx), nil
}

/*
CreateCollection creates a collection specified by 'collectionName'
*/
func (m *MongoConnect) CreateCollection(collectionName string) (*mongo.SingleResult, error) {
	db := m.GetDatabase()
	ctx, _ := m.NewBackgroundContext()
	return db.RunCommand(ctx, bson.M{"create": collectionName}), nil
}

/*
CreateIndexes creates indexes on the specified collection.

Example:

	collectionName := "my_table"
	indexes := []interface{}{
		map[string]interface{}{
			"key": map[string]interface{}{
				"field_1": 1, // ascending index
			},
			"name":   "uidx_1",
			"unique": true,
		},
		map[string]interface{}{
			"key": map[string]interface{}{
				"field_2": -1, // descending index
			},
			"name": "idx_2",
		},
	}
	dbResult, err := m.CreateIndexes(collectionName, indexes)
*/
func (m *MongoConnect) CreateIndexes(collectionName string, indexes []interface{}) (*mongo.SingleResult, error) {
	db := m.GetDatabase()
	ctx, _ := m.NewBackgroundContext()
	return db.RunCommand(ctx, bson.M{"createIndexes": collectionName, "indexes": indexes}), nil
}
