package prom

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/consu/semita"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// MongoConnect holds a MongoDB client (https://github.com/mongodb/mongo-go-driver) that can be shared within the application.
type MongoConnect struct {
	url           string            // connection url, including authentication credentials
	db            string            // database name
	client        *mongo.Client     // instance of MongoDB client
	clientProxy   *MongoClientProxy // (since v0.3.0) wrapper around the real MongoDB client
	timeoutMs     int               // default timeout for db operations, in milliseconds
	poolOpts      *MongoPoolOpts    // MongoDB connection pool options
	metricsLogger IMetricsLogger    // (since v0.3.0) if non-nil, AwsDynamodbConnect automatically logs executing commands.
}

// MongoPoolOpts holds options to configure MongoDB connection pool.
//
// Available: since v0.2.8
type MongoPoolOpts struct {
	// Timeout for establishing connection to server.
	// Set zero or negative value to use default value.
	ConnectTimeout time.Duration
	// Timeout for socket reads/writes
	// Set zero or negative value to use default value.
	SocketTimeout time.Duration
	// Timeout for waiting for available server to execute an operation.
	// Set zero or negative value to use default value.
	ServerSelectionTimeout time.Duration

	// Maximum number of connections.
	// Set zero or negative value to use default value.
	MaxPoolSize int
	// Minimum number of idle connections. Default value is 1.
	MinPoolSize int
}

var (
	// MongoPoolOptsLongDistance is MongoPoolOpts configured for the case client and MongoDB are far apart.
	// available since v0.2.13
	MongoPoolOptsLongDistance = &MongoPoolOpts{
		ConnectTimeout:         3000 * time.Millisecond,
		SocketTimeout:          5000 * time.Millisecond,
		ServerSelectionTimeout: 10000 * time.Millisecond,
	}

	// MongoPoolOptsFailFast is MongoPoolOpts configured for the case client and MongoDB are close, and we want to fail fast when error.
	// available since v0.2.13
	MongoPoolOptsFailFast = &MongoPoolOpts{
		ConnectTimeout:         50 * time.Millisecond,
		SocketTimeout:          250 * time.Millisecond,
		ServerSelectionTimeout: 1000 * time.Millisecond,
	}

	// MongoPoolOptsGeneral is MongoPoolOpts configured for general use cases.
	// available since v0.2.13
	MongoPoolOptsGeneral = &MongoPoolOpts{
		ConnectTimeout:         1000 * time.Millisecond,
		SocketTimeout:          3000 * time.Millisecond,
		ServerSelectionTimeout: 5000 * time.Millisecond,
	}

	defaultMongoPoolOpts = MongoPoolOptsLongDistance
)

// NewMongoConnect constructs a new MongoConnect instance.
//
// Parameters: see NewMongoConnectWithPoolOptions
//
// Return: see NewMongoConnectWithPoolOptions
func NewMongoConnect(url, db string, defaultTimeoutMs int) (*MongoConnect, error) {
	return NewMongoConnectWithPoolOptions(url, db, defaultTimeoutMs, defaultMongoPoolOpts)
}

// NewMongoConnectWithPoolOptions constructs a new MongoConnect instance.
//
// Parameters:
//   - url             : connection url, including authentication credentials
//   - db              : name of database to connect to
//   - defaultTimeoutMs: default timeout for db operations, in milliseconds
//   - poolOpts        : MongoDB connection pool settings
//
// Return: the MongoConnect instance and error (if any). Note:
//   - In case of connection error: this function returns the MongoConnect instance and the error.
//   - Other error: this function returns (nil, error)
//
// Available since v0.2.8
func NewMongoConnectWithPoolOptions(url, db string, defaultTimeoutMs int, poolOpts *MongoPoolOpts) (*MongoConnect, error) {
	if defaultTimeoutMs < 0 {
		defaultTimeoutMs = 0
	}
	if poolOpts == nil {
		poolOpts = defaultMongoPoolOpts
	}
	m := &MongoConnect{
		url:           url,
		db:            db,
		timeoutMs:     defaultTimeoutMs,
		poolOpts:      poolOpts,
		metricsLogger: NewMemoryStoreMetricsLogger(1028),
	}
	return m, m.Init()
}

// Init should be called to initialize the MongoConnect instance before use.
//
// Available since v0.2.8
func (m *MongoConnect) Init() error {
	if m.client != nil {
		return nil
	}
	opts := options.Client().ApplyURI(m.url)
	if m.poolOpts != nil {
		if m.poolOpts.ConnectTimeout > 0 {
			opts.SetConnectTimeout(m.poolOpts.ConnectTimeout)
		}
		if m.poolOpts.SocketTimeout > 0 {
			opts.SetSocketTimeout(m.poolOpts.SocketTimeout)
		}
		if m.poolOpts.ServerSelectionTimeout > 0 {
			opts.SetServerSelectionTimeout(m.poolOpts.ServerSelectionTimeout)
		}

		if m.poolOpts.MaxPoolSize > 0 {
			opts.SetMaxPoolSize(uint64(m.poolOpts.MaxPoolSize))
		}
		if m.poolOpts.MinPoolSize > 0 {
			opts.SetMinPoolSize(uint64(m.poolOpts.MinPoolSize))
		} else {
			opts.SetMinPoolSize(1)
		}
	}
	client, err := mongo.NewClient(opts)
	if err != nil {
		return err
	}
	m.client = client
	m.clientProxy = &MongoClientProxy{Client: client, mc: m}
	return m.client.Connect(m.NewContext())
}

// RegisterMetricsLogger associate an IMetricsLogger instance with this AwsDynamodbConnect.
// If non-nil, AwsDynamodbConnect automatically logs executing commands.
//
// Available since v0.3.0
func (m *MongoConnect) RegisterMetricsLogger(metricsLogger IMetricsLogger) *MongoConnect {
	m.metricsLogger = metricsLogger
	return m
}

// MetricsLogger returns the associated IMetricsLogger instance.
func (m *MongoConnect) MetricsLogger() IMetricsLogger {
	return m.metricsLogger
}

// NewCmdExecInfo is convenient function to create a new CmdExecInfo instance.
//
// The returned CmdExecInfo has its 'id' and 'begin-time' fields initialized.
//
// Available since v0.3.0
func (m *MongoConnect) NewCmdExecInfo() *CmdExecInfo {
	return &CmdExecInfo{
		Id:        newId(),
		BeginTime: time.Now(),
		Cost:      -1,
	}
}

// LogMetrics is convenient function to put the CmdExecInfo to the metrics log.
//
// This function is silently no-op of the input if nil or there is no associated metrics logger.
//
// Available since v0.3.0
func (m *MongoConnect) LogMetrics(category string, cmd *CmdExecInfo) error {
	if cmd != nil && m.metricsLogger != nil {
		return m.metricsLogger.Put(category, cmd)
	}
	return nil
}

// Metrics is convenient function to capture the snapshot of command execution metrics.
//
// This function is silently no-op of there is no associated metrics logger.
//
// Available since v0.3.0
func (m *MongoConnect) Metrics(category string, opts ...MetricsOpts) (*Metrics, error) {
	if m.metricsLogger != nil {
		return m.metricsLogger.Metrics(category, opts...)
	}
	return nil, nil
}

// GetUrl returns MongoDB connection url setting.
//
// Available since v0.2.8
func (m *MongoConnect) GetUrl() string {
	return m.url
}

// SetUrl sets MongoDB connection url setting.
// Note: the change does not take effect if called after Init has been called.
//
// Available since v0.2.8
func (m *MongoConnect) SetUrl(url string) *MongoConnect {
	m.url = url
	return m
}

// GetDb returns name of MongoDB database to connect.
//
// Available since v0.2.8
func (m *MongoConnect) GetDb() string {
	return m.db
}

// SetDb sets MongoDB database to connect.
// Note: the change does not take effect if called after Init has been called.
//
// Available since v0.2.8
func (m *MongoConnect) SetDb(db string) *MongoConnect {
	m.db = db
	return m
}

// GetTimeoutMs returns default timeout value (in milliseconds).
//
// Available since v0.2.8
func (m *MongoConnect) GetTimeoutMs() int {
	return m.timeoutMs
}

// SetTimeoutMs sets default timeout value (in milliseconds).
//
// Available since v0.2.8
func (m *MongoConnect) SetTimeoutMs(timeoutMs int) *MongoConnect {
	m.timeoutMs = timeoutMs
	return m
}

// GetMongoPoolOpts returns MongoDB connection pool configurations.
//
// Available since v0.2.8
func (m *MongoConnect) GetMongoPoolOpts() *MongoPoolOpts {
	return m.poolOpts
}

// SetMongoPoolOpts sets MongoDB connection pool configurations.
// Note: the change does not take effect if called after Init has been called.
//
// Available since v0.2.8
func (m *MongoConnect) SetMongoPoolOpts(opts *MongoPoolOpts) *MongoConnect {
	m.poolOpts = opts
	return m
}

// NewContext creates a new context with specified timeout in milliseconds.
// If there is no specified timeout, or timeout value is less than or equal to 0, the default timeout is used.
//
// Available: since v0.2.0
// (since v0.2.8) this function return only context.Context. Use NewContextWithCancel if context.CancelFunc is needed.
func (m *MongoConnect) NewContext(timeoutMs ...int) context.Context {
	ctx, _ := m.NewContextWithCancel(timeoutMs...)
	return ctx
}

// NewContextIfNil creates a new context with specified timeout in milliseconds if the supplied ctx is nil. Otherwise,
// ctx is returned as-is.
//
// If there is no specified timeout, or timeout value is less than or equal to 0, the default timeout is used.
//
// Available: since v0.2.8
func (m *MongoConnect) NewContextIfNil(ctx context.Context, timeoutMs ...int) context.Context {
	if ctx == nil {
		ctx = m.NewContext(timeoutMs...)
	}
	return ctx
}

// NewContextWithCancel is similar to NewContext, but it returns a pair (context.Context, context.CancelFunc).
//
// Available: since v0.2.8
func (m *MongoConnect) NewContextWithCancel(timeoutMs ...int) (context.Context, context.CancelFunc) {
	d := m.timeoutMs
	if len(timeoutMs) > 0 && timeoutMs[0] > 0 {
		d = timeoutMs[0]
	}
	return context.WithTimeout(context.Background(), time.Duration(d)*time.Millisecond)
}

// Close closes all connections associated with the underlying MongoDB client.
//
// Available: since v0.2.0
func (m *MongoConnect) Close(ctx context.Context) error {
	return m.client.Disconnect(m.NewContextIfNil(ctx))
}

// DecodeSingleResult transforms 'mongo.SingleResult' to 'bson.M'.
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

// DecodeResultCallback loops through the cursor and, for each fetched document, passes it to the callback function.
//
// Note:
// 	- docNum is 1-based, and scoped to the cursor context. This function does not close the cursor!
// 	- If callback function returns 'false', the loop will break and DecodeResultCallback returns.
func (m *MongoConnect) DecodeResultCallback(ctx context.Context, cursor *mongo.Cursor, callback func(docNum int, doc bson.M, err error) bool) {
	for dNum := 1; cursor.Next(m.NewContextIfNil(ctx)); dNum++ {
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

// DecodeSingleResultRaw transforms 'mongo.SingleResult' to raw JSON data.
//
// Available: since v0.0.3.1
func (m *MongoConnect) DecodeSingleResultRaw(dbResult *mongo.SingleResult) ([]byte, error) {
	doc, err := m.DecodeSingleResult(dbResult)
	if doc == nil || err != nil {
		return nil, err
	}
	return json.Marshal(doc)
}

// DecodeResultCallbackRaw loops through the cursor and, for each fetched document, passes it to the callback function.
//
// Note:
// 	- docNum is 1-based, and scoped to the cursor context. This function does not close the cursor!
// 	- If callback function returns 'false', the loop will break and DecodeResultCallbackRaw returns.
//
// Available: since v0.0.3.1
func (m *MongoConnect) DecodeResultCallbackRaw(ctx context.Context, cursor *mongo.Cursor, callback func(docNum int, doc []byte, err error) bool) {
	for dNum := 1; cursor.Next(m.NewContextIfNil(ctx)); dNum++ {
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

// Ping tries to send a "ping" request to MongoDB server.
func (m *MongoConnect) Ping(ctx context.Context) error {
	return m.GetMongoClientProxy().Ping(m.NewContextIfNil(ctx), readpref.Primary())
}

// IsConnected returns true if the connection to MongoDB has established.
func (m *MongoConnect) IsConnected() bool {
	return m.Ping(nil) == nil
}

// GetMongoClient returns the underlying MongoDB client instance.
func (m *MongoConnect) GetMongoClient() *mongo.Client {
	return m.client
}

// GetMongoClientProxy is similar to GetMongoClient, but returns a proxy that can be used as a replacement.
//
// Available since v0.3.0
func (m *MongoConnect) GetMongoClientProxy() *MongoClientProxy {
	if m.clientProxy == nil {
		m.clientProxy = &MongoClientProxy{Client: m.client, mc: m}
	}
	return m.clientProxy
}

// GetDatabase returns the database object attached to this MongoConnect.
func (m *MongoConnect) GetDatabase(opts ...*options.DatabaseOptions) *mongo.Database {
	return m.client.Database(m.db, opts...)
}

// GetDatabaseProxy is similar to GetDatabase, but returns a proxy that can be used as a replacement.
//
// Available since v0.3.0
func (m *MongoConnect) GetDatabaseProxy(opts ...*options.DatabaseOptions) *MongoDatabaseProxy {
	return m.GetMongoClientProxy().DatabaseProxy(m.db, opts...)
}

// HasDatabase checks if a database exists on MongoDB server.
func (m *MongoConnect) HasDatabase(dbName string, opts ...*options.ListDatabasesOptions) (bool, error) {
	dbList, err := m.GetMongoClientProxy().ListDatabaseNames(m.NewContext(), bson.M{"name": dbName}, opts...)
	return len(dbList) > 0, err
}

// GetCollection returns the collection object specified by 'collectionName'.
func (m *MongoConnect) GetCollection(collectionName string, opts ...*options.CollectionOptions) *mongo.Collection {
	return m.GetDatabase().Collection(collectionName, opts...)
}

// GetCollectionProxy is similar to GetCollection, but returns a proxy that can be used as a replacement.
//
// Available since v0.3.0
func (m *MongoConnect) GetCollectionProxy(collectionName string, opts ...*options.CollectionOptions) *MongoCollectionProxy {
	return m.GetDatabaseProxy().CollectionProxy(collectionName, opts...)
}

// HasCollection checks if a collection exists in the database.
func (m *MongoConnect) HasCollection(collectionName string, opts ...*options.ListCollectionsOptions) (bool, error) {
	ctx := m.NewContext()
	collectionList, err := m.GetDatabaseProxy().ListCollections(ctx, bson.M{"name": collectionName}, opts...)
	defer collectionList.Close(ctx)
	if err == nil {
		err = collectionList.Err()
	}
	if err != nil {
		return false, err
	}
	return collectionList.Next(ctx), nil
}

// CreateCollection is convenient function to create a collection in the current database specified by 'collectionName'.
func (m *MongoConnect) CreateCollection(collectionName string, opts ...*options.CreateCollectionOptions) error {
	return m.GetDatabaseProxy().CreateCollection(m.NewContext(), collectionName, opts...)
}

// DropCollection is convenient function to drop a collection in the current database specified by 'collectionName'.
//
// Available since v0.3.0
func (m *MongoConnect) DropCollection(collectionName string) error {
	return m.GetCollectionProxy(collectionName).Drop(m.NewContext())
}

// CreateView is convenient function to create a view in the current database.
//
// Note: view, once created, is a "virtual" collection. A view can be dropped by calling DropCollection and its existence
// can be verified by calling HasCollection. See https://www.mongodb.com/docs/manual/core/views/
//
// Available since v0.3.0
func (m *MongoConnect) CreateView(viewName, collectionName string, pipeline interface{}, opts ...*options.CreateViewOptions) error {
	return m.GetDatabaseProxy().CreateView(m.NewContext(), viewName, collectionName, pipeline, opts...)
}

/*
CreateCollectionIndexes creates indexes on the specified collection in the current database. The names of the created indexes are returned.

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

See https://www.mongodb.com/docs/manual/indexes/

Available: since v0.2.1
*/
func (m *MongoConnect) CreateCollectionIndexes(collectionName string, indexes []interface{}) ([]string, error) {
	indexModels := make([]mongo.IndexModel, 0)
	for _, index := range indexes {
		indexModel := toIndexModel(index)
		if indexModel == nil {
			err := errors.New("cannot convert index definition to mongo.IndexModel")
			return nil, err
		}
		indexModels = append(indexModels, *indexModel)
	}
	result, err := m.GetCollectionProxy(collectionName).IndexesProxy().CreateMany(m.NewContext(), indexModels)
	return result, err
}

func toIndexModel(index interface{}) *mongo.IndexModel {
	if indexModel, ok := index.(mongo.IndexModel); ok {
		return &indexModel
	}
	if indexModel, ok := index.(*mongo.IndexModel); ok {
		return indexModel
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
