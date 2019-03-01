package prom

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Prom for the official Go driver for MongoDB (https://github.com/mongodb/mongo-go-driver)

/*
MongoConnect holds a MongoDB client that can be shared within the application.
*/
type MongoConnect struct {
	url       string        // connection url, including authentication credentials
	db        string        // database name
	client    *mongo.Client // client instance
	timeoutMs time.Duration // default timeout for db operations, in milliseconds
}

/*
NewMongoConnect constructs a new MongoConnect instance.

Parameters:

  - url             : connection url, including authentication credentials
  - db              : name of database to connect to
  - defaultTimeoutMs: efault timeout for db operations, in milliseconds

Return: the MongoConnect instance and connection error (if any). Note: this function always return a MongoConnect instance.
In case of connection error, call TryConnect(...) to reestablish the connection.
*/
func NewMongoConnect(url, db string, defaultTimeoutMs int64) (*MongoConnect, error) {
	m := &MongoConnect{
		url:       url,
		db:        db,
		timeoutMs: time.Duration(defaultTimeoutMs),
	}
	return m, m.TryConnect()
}

/*
DecodeSingleResult is a helper function to transform 'mongo.SingleResult' to 'bson.M'.
*/
func DecodeSingleResult(dbResult *mongo.SingleResult) (*bson.M, error) {
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
	return &row, nil
}

/*
DecodeResultCallback loops through the cursor and, for each fetched document, passes it to the callback function.
Note: docNum is 1-based, and scoped to the cursor context. This function does not close the cursor!
*/
func DecodeResultCallback(ctx context.Context, cursor *mongo.Cursor, callback func(docNum int, doc *bson.M, err error)) {
	dNum := 1
	for cursor.Next(ctx) {
		var d bson.M
		err := cursor.Decode(&d)
		if err != nil {
			callback(dNum, nil, err)
		} else {
			callback(dNum, &d, nil)
		}
	}
}

/*----------------------------------------------------------------------*/

/*
NewBackgroundContext creates a new background context with specified timeout in milliseconds.
If there is no specified timeout, or timeout value is less than or equal to 0, the default timeout is used.
*/
func (m *MongoConnect) NewBackgroundContext(timeoutMs ...int64) (context.Context, context.CancelFunc) {
	d := m.timeoutMs
	if len(timeoutMs) > 0 && timeoutMs[0] > 0 {
		d = time.Duration(timeoutMs[0])
	}
	return context.WithTimeout(context.Background(), d*time.Millisecond)
}

var mongoMutex = &sync.Mutex{}

/*
TryConnect tries to establish connection to MongoDB server or replica set specified in the url supplied from NewMongoConnect.
If there is no specified timeout, or timeout value is less than or equal to 0, the default timeout is used.
*/
func (m *MongoConnect) TryConnect(timeoutMs ...int64) error {
	if m.client == nil {
		mongoMutex.Lock()
		defer mongoMutex.Unlock()
		if m.client == nil {
			ctx, _ := m.NewBackgroundContext()
			c, err := mongo.Connect(ctx, options.Client().ApplyURI(m.url))
			if err != nil {
				return err
			}
			m.client = c
		}
	}
	return nil
}

/*
IsConnected returns true if the connection to MongoDB has established. If not, call TryConnect(...) to connect.
*/
func (m *MongoConnect) IsConnected() bool {
	return m.client != nil
}

/*
GetMongoClient returns the underlying MongoDB client instance.
This function will try to establish connection to MongoDB server/relica set if there is no active one.
*/
func (m *MongoConnect) GetMongoClient() (*mongo.Client, error) {
	if m.client == nil {
		err := m.TryConnect()
		if err != nil {
			return nil, err
		}
	}
	return m.client, nil
}

/*
GetDatabase returns the database object attached to this MongoConnect.
*/
func (m *MongoConnect) GetDatabase(opts ...*options.DatabaseOptions) (*mongo.Database, error) {
	if m.client == nil {
		err := m.TryConnect()
		if err != nil {
			return nil, err
		}
	}
	return m.client.Database(m.db, opts...), nil
}

/*
GetCollection returns the collection object specified by 'collectionName'.
*/
func (m *MongoConnect) GetCollection(collectionName string, opts ...*options.CollectionOptions) (*mongo.Collection, error) {
	db, err := m.GetDatabase()
	if err != nil {
		return nil, err
	}
	return db.Collection(collectionName, opts...), nil
}

/*
CreateCollection creates a collection specified by 'collectionName'
*/
func (m *MongoConnect) CreateCollection(collectionName string) (*mongo.SingleResult, error) {
	db, err := m.GetDatabase()
	if err != nil {
		return nil, err
	}
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
			"name": "idx_sid",
		},
	}
	dbResult, err := m.CreateIndexes(collectionName, indexes)
*/
func (m *MongoConnect) CreateIndexes(collectionName string, indexes []interface{}) (*mongo.SingleResult, error) {
	db, err := m.GetDatabase()
	if err != nil {
		return nil, err
	}
	ctx, _ := m.NewBackgroundContext()
	return db.RunCommand(ctx, bson.M{"createIndexes": collectionName, "indexes": indexes}), nil
}

// /*
// InsertOne inserts one single document to the specified collection.
// */
// func (m *MongoConnect) InsertOne(ctx context.Context, collectionName string, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
// 	collection, err := m.GetCollection(collectionName)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if ctx == nil {
// 		ctx, _ = m.NewBackgroundContext()
// 	}
// 	return collection.InsertOne(ctx, document, opts...)
// }
