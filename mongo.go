package prom

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/consu/semita"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"reflect"
	"strconv"
	"time"
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
	if client, err := mongo.NewClient(options.Client().ApplyURI(m.url)); err != nil {
		return nil, err
	} else {
		m.client = client
		ctx, _ := m.NewContext()
		return m, m.client.Connect(ctx)
	}
}

/*
Close closes all connections associated with the underlying MongoDB client.

Available: since v0.2.0
*/
func (m *MongoConnect) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}

/*
Disconnect is alias of Close.

Deprecated: since v0.2.0, use Close instead.
*/
func (m *MongoConnect) Disconnect(ctx context.Context) error {
	return m.Close(ctx)
}

/*
DecodeSingleResult transforms 'mongo.SingleResult' to 'bson.M'.
*/
func (m *MongoConnect) DecodeSingleResult(dbResult *mongo.SingleResult) (bson.M, error) {
	var row bson.M
	if err := dbResult.Decode(&row); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
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
		if err := cursor.Decode(&d); err != nil {
			if !callback(dNum, nil, err) {
				break
			}
		} else if !callback(dNum, d, nil) {
			break
		}
	}
}

/*
DecodeSingleResultRaw transforms 'mongo.SingleResult' to raw JSON data.

Available: since v0.0.3.1
*/
func (m *MongoConnect) DecodeSingleResultRaw(dbResult *mongo.SingleResult) ([]byte, error) {
	if doc, err := m.DecodeSingleResult(dbResult); doc == nil || err != nil {
		return nil, err
	} else {
		return json.Marshal(doc)
	}
}

/*
DecodeResultCallbackRaw loops through the cursor and, for each fetched document, passes it to the callback function.
Note:

	- docNum is 1-based, and scoped to the cursor context. This function does not close the cursor!
	- If callback function returns 'false', the loop will break and DecodeResultCallbackRaw returns.

Available: since v0.0.3.1
*/
func (m *MongoConnect) DecodeResultCallbackRaw(ctx context.Context, cursor *mongo.Cursor, callback func(docNum int, doc []byte, err error) bool) {
	for dNum := 1; cursor.Next(ctx); dNum++ {
		var d bson.M
		if err := cursor.Decode(&d); err != nil {
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
NewBackgroundContext is alias of NewContext.

Deprecated: since v0.2.0, use NewContext instead.
*/
func (m *MongoConnect) NewBackgroundContext(timeoutMs ...int) (context.Context, context.CancelFunc) {
	return m.NewContext(timeoutMs...)
}

/*
NewContext creates a new context with specified timeout in milliseconds.
If there is no specified timeout, or timeout value is less than or equal to 0, the default timeout is used.

Available: since v0.2.0
*/
func (m *MongoConnect) NewContext(timeoutMs ...int) (context.Context, context.CancelFunc) {
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
func (m *MongoConnect) Ping(ctx context.Context) error {
	return m.client.Ping(ctx, readpref.Primary())
}

/*
IsConnected returns true if the connection to MongoDB has established.
*/
func (m *MongoConnect) IsConnected() bool {
	return m.Ping(nil) == nil
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
	ctx, _ := m.NewContext()
	if dbList, err := m.client.ListDatabaseNames(ctx, bson.M{"name": dbName}, opts...); err != nil {
		return false, err
	} else {
		return len(dbList) > 0, nil
	}
}

/*
GetCollection returns the collection object specified by 'collectionName'.
*/
func (m *MongoConnect) GetCollection(collectionName string, opts ...*options.CollectionOptions) *mongo.Collection {
	return m.GetDatabase().Collection(collectionName, opts...)
}

/*
HasCollection checks if a collection exists in the database.
*/
func (m *MongoConnect) HasCollection(collectionName string, opts ...*options.ListCollectionsOptions) (bool, error) {
	ctx, _ := m.NewContext()
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
	ctx, _ := m.NewContext()
	return m.GetDatabase().RunCommand(ctx, bson.M{"create": collectionName}), nil
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
			"name"  : "uidx_1",
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

Deprecated: since v0.2.1, use CreateCollectionIndexes.
*/
func (m *MongoConnect) CreateIndexes(collectionName string, indexes []interface{}) (*mongo.SingleResult, error) {
	ctx, _ := m.NewContext()
	return m.GetDatabase().RunCommand(ctx, bson.M{"createIndexes": collectionName, "indexes": indexes}), nil
}

/*
CreateCollectionIndexes creates indexes on the specified collection. The names of the created indexes are returned.

Example (index definition can be a map, or a mongo.IndexModel):

	collectionName := "my_table"
	indexes := []interface{}{
		map[string]interface{}{
			"key": map[string]interface{}{
				"field_1": 1, // ascending index
			},
			"name"  : "uidx_1",
			"unique": true,
		},
		mongo.IndexModel{
			Keys: map[string]interface{}{
				"field_2": -1, // descending index
			},
			Options: &options.IndexOptions{
				Name  : &name,
				Unique: &isUnique,
			},
		},
	}
	indexesNames, err := m.CreateCollectionIndexes(collectionName, indexes)

Available: since v0.2.1
*/
func (m *MongoConnect) CreateCollectionIndexes(collectionName string, indexes []interface{}) ([]string, error) {
	indexModels := make([]mongo.IndexModel, 0)
	for _, index := range indexes {
		if indexModel := toIndexModel(index); indexModel == nil {
			return nil, errors.New("cannot convert index definition to mongo.IndexModel")
		} else {
			indexModels = append(indexModels, *indexModel)
		}
	}
	ctx, _ := m.NewContext()
	return m.GetCollection(collectionName).Indexes().CreateMany(ctx, indexModels)
}

func toIndexModel(index interface{}) *mongo.IndexModel {
	typ := reflect.TypeOf(index)
	if typ.Kind() == reflect.Struct && typ.Name() == "mongo.IndexModel" {
		indexModel, _ := reddo.Convert(index, typ)
		im := indexModel.(mongo.IndexModel)
		return &im
	}
	s := semita.NewSemita(index)
	var err error

	// extract "Keys"
	var Keys interface{}
	for _, k := range []string{"keys", "key", "Keys", "Key"} {
		Keys, err = s.GetValue(k)
		if err == nil && Keys != nil {
			break
		}
	}
	if err != nil || Keys == nil {
		return nil
	}

	// extract unique
	var Unique = false
	var _uninque interface{}
	for _, k := range []string{"unique", "Unique"} {
		_uninque, err = s.GetValueOfType(k, reddo.TypeBool)
		if err == nil && _uninque != nil {
			break
		}
	}
	if err != nil || _uninque == nil {
		Unique = false
	} else {
		Unique = _uninque.(bool)
	}

	// extract name
	var Name = ""
	var _name interface{}
	for _, k := range []string{"name", "Name"} {
		_name, err = s.GetValueOfType(k, reddo.TypeString)
		if err == nil && _name != nil {
			break
		}
	}
	if err != nil || _name == nil {
		if Unique {
			Name = "uidx_" + strconv.FormatInt(time.Now().UnixNano(), 10)
		} else {
			Name = "idx_" + strconv.FormatInt(time.Now().UnixNano(), 10)
		}
	} else {
		Name = _name.(string)
	}

	return &mongo.IndexModel{Keys: Keys, Options: &options.IndexOptions{Name: &Name, Unique: &Unique}}
}
