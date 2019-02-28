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
GetDatabase returns the database object attached to this MongoConnect.
*/
func (m *MongoConnect) GetDatabase(opts ...*options.DatabaseOptions) *mongo.Database {
	return m.client.Database(m.db, opts...)
}

/*
GetCollection returns the collection object specified by 'collectionName'.
*/
func (m *MongoConnect) GetCollection(collectionName string, opts ...*options.CollectionOptions) *mongo.Collection {
	return m.GetDatabase().Collection(collectionName, opts...)
}
