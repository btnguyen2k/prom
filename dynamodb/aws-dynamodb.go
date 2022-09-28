package dynamodb

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/btnguyen2k/prom"
)

const (
	// AwsDynamodbNoIndex indicates that no index will be used
	AwsDynamodbNoIndex = ""

	// AwsAttrTypeString is alias name of AWS DynamoDB attribute type "string"
	AwsAttrTypeString = "S"

	// AwsAttrTypeNumber is alias name of AWS DynamoDB attribute type "number"
	AwsAttrTypeNumber = "N"

	// AwsAttrTypeBinary is alias name of AWS DynamoDB attribute type "binary"
	AwsAttrTypeBinary = "B"

	// AwsKeyTypePartition is alias name of AWS DynamoDB key type "partition"
	AwsKeyTypePartition = "HASH"

	// AwsKeyTypeSort is alias name of AWS DynamoDB key type "sort/range"
	AwsKeyTypeSort = "RANGE"
)

var (
	// ErrTimeout is returned by AwsDynamodbWaitForTableStatus or AwsDynamodbWaitForGsiStatus to indicate that timeout occurred before we reach the desired status.
	ErrTimeout = errors.New("timeout while waiting for status")
)

func _awsDynamodbInSlide(item string, slide []string) bool {
	for _, s := range slide {
		if item == s {
			return true
		}
	}
	return false
}

// AwsDynamodbWaitForGsiStatus periodically checks if table's GSI status reaches a desired value, or timeout.
//   - statusList: list of desired statuses. This function returns nil if one of the desired statuses is reached.
//   - delay: sleep for this amount of time after each status check. Supplied value of 0 or negative means 'no sleep'.
//   - timeout: the total time should not exceed this amount. If timeout occur, this function returns ErrTimeout. Supplied value of 0 or negative means 'no timeout'!
//
// Available since v0.2.14
func AwsDynamodbWaitForGsiStatus(adc *AwsDynamodbConnect, tableName, gsiName string, statusList []string, delay, timeout time.Duration) error {
	start := time.Now()
	for status, err := adc.GetGlobalSecondaryIndexStatus(nil, tableName, gsiName); ; status, err = adc.GetGlobalSecondaryIndexStatus(nil, tableName, gsiName) {
		if err != nil {
			return err
		}
		if _awsDynamodbInSlide(status, statusList) {
			return nil
		}
		if delay.Milliseconds() > 0 {
			time.Sleep(delay)
		}
		if timeout.Milliseconds() > 0 && time.Now().Sub(start) > timeout {
			return ErrTimeout
		}
	}
	return nil
}

// AwsDynamodbWaitForTableStatus periodically checks if table's status reaches a desired value, or timeout.
//   - statusList: list of desired statuses. This function returns nil if one of the desired statuses is reached.
//   - delay: sleep for this amount of time after each status check. Supplied value of 0 or negative means 'no sleep'.
//   - timeout: the total time should not exceed this amount. If timeout occur, this function returns ErrTimeout. Supplied value of 0 or negative means 'no timeout'!
//
// Available since v0.2.14
func AwsDynamodbWaitForTableStatus(adc *AwsDynamodbConnect, tableName string, statusList []string, delay, timeout time.Duration) error {
	start := time.Now()
	for status, err := adc.GetTableStatus(nil, tableName); ; status, err = adc.GetTableStatus(nil, tableName) {
		if err != nil {
			return err
		}
		if _awsDynamodbInSlide(status, statusList) {
			return nil
		}
		if delay > 0 {
			time.Sleep(delay)
		}
		if timeout > 0 && time.Now().Sub(start) > timeout {
			return ErrTimeout
		}
	}
	return nil
}

// AwsDynamodbItem defines a generic structure for DynamoDB item.
type AwsDynamodbItem map[string]interface{}

// AwsDynamodbNameAndType defines a generic name & type pair.
type AwsDynamodbNameAndType struct{ Name, Type string }

// AwsDynamodbItemCallback defines callback interface for "scan"/"query" operation.
//
// If callback function returns false or error, the scan/query process will stop (even if there are still more items).
type AwsDynamodbItemCallback func(item AwsDynamodbItem, lastEvaluatedKey map[string]*dynamodb.AttributeValue) (bool, error)

// AwsQueryOpt provides additional options to AwsDynamodbConnect.QueryItems and AwsDynamodbConnect.QueryItemsWithCallback.
//
// Available since v0.2.15
type AwsQueryOpt struct {
	ScanIndexBackward *bool // if set to true, scan the index backward
}

func awsDynamodbToProvisionedThroughput(rcu, wcu int64) *dynamodb.ProvisionedThroughput {
	return &dynamodb.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(rcu), WriteCapacityUnits: aws.Int64(wcu)}
}

func awsDynamodbToAttributeDefinitions(attrDefs []AwsDynamodbNameAndType) []*dynamodb.AttributeDefinition {
	attrs := make([]*dynamodb.AttributeDefinition, 0)
	for _, v := range attrDefs {
		attr := &dynamodb.AttributeDefinition{AttributeName: aws.String(v.Name), AttributeType: aws.String(v.Type)}
		attrs = append(attrs, attr)
	}
	return attrs
}

func awsDynamodbToKeySchemaElement(keyDefs []AwsDynamodbNameAndType) []*dynamodb.KeySchemaElement {
	keySchema := make([]*dynamodb.KeySchemaElement, 0)
	for _, v := range keyDefs {
		attr := &dynamodb.KeySchemaElement{AttributeName: aws.String(v.Name), KeyType: aws.String(v.Type)}
		keySchema = append(keySchema, attr)
	}
	return keySchema
}

func awsDynamodbToItem(input map[string]*dynamodb.AttributeValue) (AwsDynamodbItem, error) {
	if input == nil {
		return nil, nil
	}
	item := AwsDynamodbItem{}
	return item, dynamodbattribute.UnmarshalMap(input, &item)
}

func awsDynamodbMakeKey(keyFilter map[string]interface{}) map[string]*dynamodb.AttributeValue {
	key := make(map[string]*dynamodb.AttributeValue)
	for k, v := range keyFilter {
		key[k] = AwsDynamodbToAttributeValue(v)
	}
	return key
}

// AwsDynamodbToAttributeValue converts a Go value to DynamoDB's attribute value.
func AwsDynamodbToAttributeValue(v interface{}) *dynamodb.AttributeValue {
	av, err := dynamodbattribute.Marshal(v)
	if err != nil {
		return nil
	}
	return av
}

// AwsDynamodbToAttributeSet converts a Go value to DynamoDB's set.
func AwsDynamodbToAttributeSet(v interface{}) *dynamodb.AttributeValue {
	if av, ok := v.(*dynamodb.AttributeValue); ok {
		return av
	}
	if av, ok := v.(dynamodb.AttributeValue); ok {
		return &av
	}

	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		sv, _ := reddo.ToString(v)
		return &dynamodb.AttributeValue{NS: []*string{aws.String(sv)}}
	case reflect.Float32, reflect.Float64:
		return &dynamodb.AttributeValue{NS: []*string{aws.String(strconv.FormatFloat(rv.Float(), 'f', -1, 64))}}
	case reflect.String:
		return &dynamodb.AttributeValue{SS: []*string{aws.String(rv.String())}}
	case reflect.Array, reflect.Slice:
		switch rv.Type().Elem().Kind() {
		case reflect.Uint8:
			// "slice of bytes" means "binary string []byte"
			return &dynamodb.AttributeValue{BS: [][]byte{rv.Bytes()}}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			av := make([]*string, 0)
			for i, n := 0, rv.Len(); i < n; i++ {
				sv, _ := reddo.ToString(rv.Index(i).Interface())
				av = append(av, aws.String(sv))
			}
			return &dynamodb.AttributeValue{NS: av}
		case reflect.Float32, reflect.Float64:
			av := make([]*string, 0)
			for i, n := 0, rv.Len(); i < n; i++ {
				av = append(av, aws.String(strconv.FormatFloat(rv.Index(i).Float(), 'f', -1, 64)))
			}
			return &dynamodb.AttributeValue{NS: av}
		case reflect.String:
			av := make([]*string, 0)
			for i, n := 0, rv.Len(); i < n; i++ {
				av = append(av, aws.String(rv.Index(i).String()))
			}
			return &dynamodb.AttributeValue{SS: av}
		case reflect.Array, reflect.Slice:
			// binary set
			av := make([][]byte, 0)
			for i, n := 0, rv.Len(); i < n; i++ {
				el := rv.Index(i)
				if el.Type().Elem().Kind() == reflect.Uint8 {
					av = append(av, el.Bytes())
				}
			}
			return &dynamodb.AttributeValue{BS: av}
		}
	}
	return nil
}

// IsAwsError returns true if err is an awserr.Error and its code equals to awsErrCode.
//
// Available: since v0.2.5
func IsAwsError(err error, awsErrCode string) bool {
	if aerr, ok := err.(awserr.Error); ok && aerr.Code() == awsErrCode {
		return true
	}
	return false
}

// AwsIgnoreErrorIfMatched returns nil if err is an awserr.Error and its code is in the "exclude" list.
func AwsIgnoreErrorIfMatched(err error, excludeCodeList ...string) error {
	if aerr, ok := err.(awserr.Error); ok {
		for _, errCode := range excludeCodeList {
			if aerr.Code() == errCode {
				return nil
			}
		}
	}
	return err
}

// AwsIgnoreTransactErrorIfMatched returns nil if err is a *dynamodb.TransactionCanceledException and its reason's code is in the "exclude" list.
//
// Available since v0.3.0
func AwsIgnoreTransactErrorIfMatched(err error, excludeCodeList ...string) error {
	excludeCodeMap := make(map[string]bool)
	for _, errCode := range excludeCodeList {
		excludeCodeMap[errCode] = true
	}
	if aerr, ok := err.(*dynamodb.TransactionCanceledException); ok {
		for _, reason := range aerr.CancellationReasons {
			code := ""
			if reason.Code != nil {
				code = *reason.Code
			}
			if !excludeCodeMap[code] {
				return err
			}
		}
		return nil
	}
	return err
}

// AwsDynamodbExistsAllBuilder builds an expression.ConditionBuilder where all attributes must exist.
func AwsDynamodbExistsAllBuilder(attrs []string) *expression.ConditionBuilder {
	if attrs == nil || len(attrs) == 0 {
		return nil
	}
	builder := expression.Name(attrs[0]).AttributeExists()
	for _, attr := range attrs[1:] {
		builder = builder.And(expression.Name(attr).AttributeExists())
	}
	return &builder
}

// AwsDynamodbNotExistsAllBuilder builds a expression.ConditionBuilder where all attributes must not exist.
func AwsDynamodbNotExistsAllBuilder(attrs []string) *expression.ConditionBuilder {
	if attrs == nil || len(attrs) == 0 {
		return nil
	}
	builder := expression.Name(attrs[0]).AttributeNotExists()
	for _, attr := range attrs[1:] {
		builder = builder.And(expression.Name(attr).AttributeNotExists())
	}
	return &builder
}

// AwsDynamodbEqualsBuilder builds a expression.ConditionBuilder with condition attr1=value1 AND attr1=value1 AND...
//
// Parameters:
//   - condition: format {attribute-name:attribute-value}
func AwsDynamodbEqualsBuilder(condition map[string]interface{}) *expression.ConditionBuilder {
	if condition == nil || len(condition) == 0 {
		return nil
	}
	var builder *expression.ConditionBuilder
	for k, v := range condition {
		cb := expression.Name(k).Equal(expression.Value(v))
		if builder != nil {
			cb = builder.And(cb)
		}
		builder = &cb
	}
	return builder
}

/*----------------------------------------------------------------------*/

// AwsDynamodbConnect holds an AWS DynamoDB client (https://github.com/aws/aws-sdk-go/tree/master/service/dynamodb) that can be shared within the application.
type AwsDynamodbConnect struct {
	config            *aws.Config        // aws config instance
	session           *session.Session   // aws session insntance
	db                *dynamodb.DynamoDB // aws dynamodb instance
	dbProxy           *DynamoDbProxy     // (since v0.3.0) wrapper around the real aws dynamodb instance
	timeoutMs         int                // timeout in milliseconds
	ownDb, ownSession bool
	metricsLogger     prom.IMetricsLogger // (since v0.3.0) if non-nil, AwsDynamodbConnect automatically logs executing commands.
}

// NewAwsDynamodbConnect constructs a new AwsDynamodbConnect instance.
//
// Parameters:
//   - cfg             : aws.Config instance
//   - sess            : session.Session instance
//   - db              :  dynamodb.DynamoDB instance
//   - defaultTimeoutMs: default timeout for db operations, in milliseconds
//
// Return: the AwsDynamodbConnect instance and error (if any). Note:
//   - if db is nil, the instance is built from session
//   - if session is nil, the instance is built from config
//   - at least one of {config, session, db} must not be nil
//   - (since v0.3.0) an in-memory implementation of IMetricsLogger is registered with the instance
func NewAwsDynamodbConnect(cfg *aws.Config, sess *session.Session, db *dynamodb.DynamoDB, defaultTimeoutMs int) (*AwsDynamodbConnect, error) {
	if cfg == nil && sess == nil && db == nil {
		return nil, errors.New("at least one of {config, session, db} must not be nil")
	}
	if defaultTimeoutMs < 0 {
		defaultTimeoutMs = 0
	}
	adc := &AwsDynamodbConnect{
		config:        cfg,
		session:       sess,
		db:            db,
		timeoutMs:     defaultTimeoutMs,
		metricsLogger: prom.NewMemoryStoreMetricsLogger(1028),
	}
	if adc.db == nil {
		if adc.session == nil {
			if mysess, err := session.NewSession(adc.config); err == nil {
				adc.session = mysess
				adc.ownSession = true
			} else {
				return nil, err
			}
		}
		if mydb := dynamodb.New(adc.session); mydb != nil {
			adc.db = mydb
			adc.ownDb = true
		} else {
			return nil, errors.New("cannot create DynamoDB client instance")
		}
	}
	return adc, adc.Init()
}

// Init should be called to initialize the AwsDynamodbConnect instance before use.
//
// Available since v0.3.0
func (adc *AwsDynamodbConnect) Init() error {
	adc.dbProxy = &DynamoDbProxy{DynamoDB: adc.db, adc: adc}
	return nil
}

// RegisterMetricsLogger associate an IMetricsLogger instance with this AwsDynamodbConnect.
// If non-nil, AwsDynamodbConnect automatically logs executing commands.
//
// Available since v0.3.0
func (adc *AwsDynamodbConnect) RegisterMetricsLogger(metricsLogger prom.IMetricsLogger) *AwsDynamodbConnect {
	adc.metricsLogger = metricsLogger
	return adc
}

// MetricsLogger returns the associated IMetricsLogger instance.
func (adc *AwsDynamodbConnect) MetricsLogger() prom.IMetricsLogger {
	return adc.metricsLogger
}

// NewCmdExecInfo is convenient function to create a new CmdExecInfo instance.
//
// The returned CmdExecInfo has its 'id' and 'begin-time' fields initialized.
//
// Available since v0.3.0
func (adc *AwsDynamodbConnect) NewCmdExecInfo() *prom.CmdExecInfo {
	return &prom.CmdExecInfo{
		Id:        prom.NewId(),
		BeginTime: time.Now(),
		Cost:      -1,
	}
}

// LogMetrics is convenient function to put the CmdExecInfo to the metrics log.
//
// This function is silently no-op of the input if nil or there is no associated metrics logger.
//
// Available since v0.3.0
func (adc *AwsDynamodbConnect) LogMetrics(category string, cmd *prom.CmdExecInfo) error {
	if cmd != nil && adc.metricsLogger != nil {
		return adc.metricsLogger.Put(category, cmd)
	}
	return nil
}

// Metrics is convenient function to capture the snapshot of command execution metrics.
//
// This function is silently no-op of there is no associated metrics logger.
//
// Available since v0.3.0
func (adc *AwsDynamodbConnect) Metrics(category string, opts ...prom.MetricsOpts) (*prom.Metrics, error) {
	if adc.metricsLogger != nil {
		return adc.metricsLogger.Metrics(category, opts...)
	}
	return nil, nil
}

// Close frees all resources and closes all connection associated with this AwsDynamodbConnect.
func (adc *AwsDynamodbConnect) Close() error {
	return nil
}

// GetDb returns the underlying dynamodb.DynamoDB instance.
func (adc *AwsDynamodbConnect) GetDb() *dynamodb.DynamoDB {
	return adc.db
}

// GetDbProxy is similar to GetDb, but returns a proxy that can be used as a replacement.
//
// Available since v0.3.0
func (adc *AwsDynamodbConnect) GetDbProxy() *DynamoDbProxy {
	if adc.dbProxy == nil {
		adc.dbProxy = &DynamoDbProxy{DynamoDB: adc.db, adc: adc}
	}
	return adc.dbProxy
}

// NewContext creates a new context with specified timeout in milliseconds.
// If there is no specified timeout, or timeout value is less than or equal to 0, the default timeout is used.
func (adc *AwsDynamodbConnect) NewContext(timeoutMs ...int) (aws.Context, context.CancelFunc) {
	d := adc.timeoutMs
	if len(timeoutMs) > 0 && timeoutMs[0] > 0 {
		d = timeoutMs[0]
	}
	return context.WithTimeout(context.Background(), time.Duration(d)*time.Millisecond)
}

// ListTables returns all visible tables.
//
// Parameters:
//   - ctx: (optional) used for request cancellation
func (adc *AwsDynamodbConnect) ListTables(ctx aws.Context) ([]string, error) {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	input := &dynamodb.ListTablesInput{}
	result := make([]string, 0)
	err := adc.GetDbProxy().ListTablesPagesWithContext(ctx, input, func(page *dynamodb.ListTablesOutput, _ bool) bool {
		if page != nil {
			for _, tblName := range page.TableNames {
				result = append(result, *tblName)
			}
		}
		return true
	})
	return result, err
}

// HasTable checks if a table exists.
//
// Parameters:
//   - ctx: (optional) used for request cancellation
func (adc *AwsDynamodbConnect) HasTable(ctx aws.Context, table string) (bool, error) {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	input := &dynamodb.ListTablesInput{}
	result := false
	err := adc.GetDbProxy().ListTablesPagesWithContext(ctx, input, func(page *dynamodb.ListTablesOutput, _ bool) bool {
		if page != nil {
			for _, tblName := range page.TableNames {
				if table == *tblName {
					result = true
					return false
				}
			}
		}
		return true
	})
	return result, err
}

// DeleteTable deletes an existing table.
//
// Parameters:
//   - ctx: (optional) used for request cancellation
//
// Notes:
// 	- DynamoDB table is deleted asynchronously. Use GetTableStatus to check table's existence.
// 	- This function ignores error if table does not exist.
func (adc *AwsDynamodbConnect) DeleteTable(ctx aws.Context, table string) error {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	_, err := adc.GetDbProxy().DeleteTableWithContext(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(table)})
	return AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceNotFoundException)
}

// CreateTable create a new table without index.
//
// Parameters:
//   - ctx     : (optional) used for request cancellation
//   - table   : name of the table to be created
//   - rcu     : ReadCapacityUnits (0 means PAY_PER_REQUEST)
//   - wcu     : WriteCapacityUnits (0 means PAY_PER_REQUEST)
//   - attrDefs: table attributes, where attribute-type is either "S", "N" or "B"
//   - pkDefs  : primary key definitions, where key-type is either "HASH" or "RANGE"
//
// Note: DynamoDB table is created asynchronously. Use GetTableStatus to check table's existence.
func (adc *AwsDynamodbConnect) CreateTable(ctx aws.Context, table string, rcu, wcu int64, attrDefs, pkDefs []AwsDynamodbNameAndType) error {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions:  awsDynamodbToAttributeDefinitions(attrDefs),
		KeySchema:             awsDynamodbToKeySchemaElement(pkDefs),
		ProvisionedThroughput: awsDynamodbToProvisionedThroughput(rcu, wcu),
		TableName:             aws.String(table),
	}
	_, err := adc.GetDbProxy().CreateTableWithContext(ctx, input)
	return err
}

// GetTableStatus fetches and returns table status.
//
// Parameters:
//   - ctx: (optional) used for request cancellation
//
// Note: If table does not exist, this function returns "", nil
func (adc *AwsDynamodbConnect) GetTableStatus(ctx aws.Context, table string) (string, error) {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	input := &dynamodb.DescribeTableInput{TableName: aws.String(table)}
	status, err := adc.GetDbProxy().DescribeTableWithContext(ctx, input)
	err = AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceNotFoundException)
	if status != nil && status.Table != nil && status.Table.TableStatus != nil {
		return *status.Table.TableStatus, err
	}
	return "", err
}

// GetGlobalSecondaryIndexStatus fetches and returns a table's GSI status.
//
// Parameters:
//   - ctx: (optional) used for request cancellation
//
// Note: If index does not exist, this function returns "", nil
func (adc *AwsDynamodbConnect) GetGlobalSecondaryIndexStatus(ctx aws.Context, table, indexName string) (string, error) {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	input := &dynamodb.DescribeTableInput{TableName: aws.String(table)}
	status, err := adc.GetDbProxy().DescribeTableWithContext(ctx, input)
	err = AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceNotFoundException)
	if status != nil && status.Table != nil && status.Table.GlobalSecondaryIndexes != nil {
		for _, gsi := range status.Table.GlobalSecondaryIndexes {
			if *gsi.IndexName == indexName {
				return *gsi.IndexStatus, err
			}
		}
	}
	return "", err
}

// BuildCreateGlobalSecondaryIndexAction is a convenient function to build a dynamodb.CreateGlobalSecondaryIndexAction instance.
//
// Parameters:
//   - indexName     : name of the index to be created
//   - projectionType: specify attributes that are copied from the table into the index. These are in addition to the primary key attributes and index key attributes, which are automatically projected.
//   - rcu           : ReadCapacityUnits (0 means PAY_PER_REQUEST)
//   - wcu           : WriteCapacityUnits (0 means PAY_PER_REQUEST)
//   - keyAttrs      : GSI key schema, where key-type is either "HASH" or "RANGE"
//
// Available since v0.3.0
func (adc *AwsDynamodbConnect) BuildCreateGlobalSecondaryIndexAction(indexName, projectionType string, rcu, wcu int64, keyAttrs []AwsDynamodbNameAndType) *dynamodb.CreateGlobalSecondaryIndexAction {
	return &dynamodb.CreateGlobalSecondaryIndexAction{
		IndexName:             aws.String(indexName),
		KeySchema:             awsDynamodbToKeySchemaElement(keyAttrs),
		Projection:            &dynamodb.Projection{ProjectionType: aws.String(projectionType)},
		ProvisionedThroughput: awsDynamodbToProvisionedThroughput(rcu, wcu),
	}
}

// CreateGlobalSecondaryIndex creates a Global Secondary Index on a specified table.
//
// Parameters:
//   - ctx      : (optional) used for request cancellation
//   - table    : name of the table
//   - indexName: name of the index to be created
//   - rcu      : ReadCapacityUnits (0 means PAY_PER_REQUEST)
//   - wcu      : WriteCapacityUnits (0 means PAY_PER_REQUEST)
//   - attrDefs : GSI attributes, where attribute-type is either "S", "N" or "B"
//   - keyAttrs : GSI key schema, where key-type is either "HASH" or "RANGE"
//
// Note:
//   - DynamoDB GSI is created asynchronously. Use GetGlobalSecondaryIndexStatus to check GSI's existence.
//   - GSI is created with projection type dynamodb.ProjectionTypeKeysOnly. Use CreateGlobalSecondaryIndexWithAction to create GSI with customized options.
func (adc *AwsDynamodbConnect) CreateGlobalSecondaryIndex(ctx aws.Context, table, indexName string, rcu, wcu int64, attrDefs, keyAttrs []AwsDynamodbNameAndType) error {
	action := adc.BuildCreateGlobalSecondaryIndexAction(indexName, dynamodb.ProjectionTypeKeysOnly, rcu, wcu, keyAttrs)
	return adc.CreateGlobalSecondaryIndexWithAction(ctx, table, attrDefs, action)
}

// CreateGlobalSecondaryIndexWithAction creates a Global Secondary Index on a specified table.
//
// Available since v0.3.0
func (adc *AwsDynamodbConnect) CreateGlobalSecondaryIndexWithAction(ctx aws.Context, tableName string, attrDefs []AwsDynamodbNameAndType, action *dynamodb.CreateGlobalSecondaryIndexAction) error {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	gscIndexes := []*dynamodb.GlobalSecondaryIndexUpdate{{Create: action}}
	input := &dynamodb.UpdateTableInput{
		AttributeDefinitions:        awsDynamodbToAttributeDefinitions(attrDefs),
		GlobalSecondaryIndexUpdates: gscIndexes,
		TableName:                   aws.String(tableName),
	}
	_, err := adc.GetDbProxy().UpdateTableWithContext(ctx, input)
	return err
}

// DeleteGlobalSecondaryIndex deletes a Global Secondary Index on a specified table.
//
// Parameters:
//   - ctx: (optional) used for request cancellation
//
// Notes:
// 	- DynamoDB GSI is deleted asynchronously. Use GetGlobalSecondaryIndexStatus to check index's existence.
// 	- This function ignores error if index does not exist.
func (adc *AwsDynamodbConnect) DeleteGlobalSecondaryIndex(ctx aws.Context, table, indexName string) error {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	action := &dynamodb.DeleteGlobalSecondaryIndexAction{IndexName: aws.String(indexName)}
	input := &dynamodb.UpdateTableInput{
		GlobalSecondaryIndexUpdates: []*dynamodb.GlobalSecondaryIndexUpdate{{Delete: action}},
		TableName:                   aws.String(table),
	}
	_, err := adc.GetDbProxy().UpdateTableWithContext(ctx, input)
	return AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceNotFoundException)
}

// BuildPutItemInput is a helper function to build dynamodb.PutItemInput.
//
// Available: since v0.2.6
func (adc *AwsDynamodbConnect) BuildPutItemInput(table string, item map[string]*dynamodb.AttributeValue, condition *expression.ConditionBuilder) (*dynamodb.PutItemInput, error) {
	input := &dynamodb.PutItemInput{
		Item:                   item,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:              aws.String(table),
	}
	if condition != nil {
		conditionExp, err := expression.NewBuilder().WithCondition(*condition).Build()
		if err != nil {
			return nil, err
		}
		input.ConditionExpression = conditionExp.Condition()
		input.ExpressionAttributeNames = conditionExp.Names()
		input.ExpressionAttributeValues = conditionExp.Values()
	}
	return input, nil
}

// PutItemWithInput executes DynamoDB "put-item" operation.
//
// Available: since v0.2.6
func (adc *AwsDynamodbConnect) PutItemWithInput(ctx aws.Context, input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	return adc.GetDbProxy().PutItemWithContext(ctx, input)
}

// PutItemRaw inserts a new item to table or replace an existing one.
//
// Parameters:
//   - ctx      : (optional) used for request cancellation
//   - table    : name of the table
//   - item     : item to be inserted
//   - condition: (optional) a condition that must be satisfied before writing item
func (adc *AwsDynamodbConnect) PutItemRaw(ctx aws.Context, table string, item map[string]*dynamodb.AttributeValue, condition *expression.ConditionBuilder) (*dynamodb.PutItemOutput, error) {
	input, err := adc.BuildPutItemInput(table, item, condition)
	if err != nil {
		return nil, err
	}
	return adc.PutItemWithInput(ctx, input)
}

// PutItemIfNotExistRaw inserts a new item to table only if it does not exist.
//
// Parameters:
//   - ctx    : (optional) used for request cancellation
//   - table  : name of the table
//   - item   : item to be inserted
//   - pkAttrs: primary key attribute names
//
// Note: (since v0.2.7) if item already existed, this function return (nil, nil)
func (adc *AwsDynamodbConnect) PutItemIfNotExistRaw(ctx aws.Context, table string, item map[string]*dynamodb.AttributeValue, pkAttrs []string) (*dynamodb.PutItemOutput, error) {
	result, err := adc.PutItemRaw(ctx, table, item, AwsDynamodbNotExistsAllBuilder(pkAttrs))
	if IsAwsError(err, dynamodb.ErrCodeConditionalCheckFailedException) {
		return nil, nil
	}
	return result, err
}

// PutItem inserts a new item to table or replace an existing one.
//
// Parameters:
//   - ctx      : (optional) used for request cancellation
//   - table    : name of the table
//   - item     : item to be inserted (a map or struct), will be converted to map[string]*dynamodb.AttributeValue via dynamodbattribute.MarshalMap(item)
//   - condition: (optional) a condition that must be satisfied before writing item
func (adc *AwsDynamodbConnect) PutItem(ctx aws.Context, table string, item interface{}, condition *expression.ConditionBuilder) (*dynamodb.PutItemOutput, error) {
	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return nil, err
	}
	return adc.PutItemRaw(ctx, table, av, condition)
}

// PutItemIfNotExist inserts a new item to table only if it does not exist.
//
// Parameters:
//   - ctx    : (optional) used for request cancellation
//   - table  : name of the table
//   - item   : item to be inserted (a map or struct), will be converted to map[string]*dynamodb.AttributeValue via dynamodbattribute.MarshalMap(item)
//   - pkAttrs: primary key attribute names
//
// Note: (since v0.2.7) if item already existed, this function return (nil, nil)
func (adc *AwsDynamodbConnect) PutItemIfNotExist(ctx aws.Context, table string, item interface{}, pkAttrs []string) (*dynamodb.PutItemOutput, error) {
	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return nil, err
	}
	return adc.PutItemIfNotExistRaw(ctx, table, av, pkAttrs)
}

// BuildDeleteItemInput is a helper function to build dynamodb.DeleteItemInput.
//
// Available: since v0.2.6
func (adc *AwsDynamodbConnect) BuildDeleteItemInput(table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder) (*dynamodb.DeleteItemInput, error) {
	key := awsDynamodbMakeKey(keyFilter)
	input := &dynamodb.DeleteItemInput{
		Key:                    key,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:              aws.String(table),
	}
	if condition != nil {
		conditionExp, err := expression.NewBuilder().WithCondition(*condition).Build()
		if err != nil {
			return nil, err
		}
		input.ExpressionAttributeNames = conditionExp.Names()
		input.ExpressionAttributeValues = conditionExp.Values()
		input.ConditionExpression = conditionExp.Condition()
	}
	return input, nil
}

// DeleteItemWithInput executes DynamoDB "delete-item" operation.
//
// Available: since v0.2.6
func (adc *AwsDynamodbConnect) DeleteItemWithInput(ctx aws.Context, input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	return adc.GetDbProxy().DeleteItemWithContext(ctx, input)
}

// DeleteItem removes a single item from specified table.
//
// Parameters:
//   - ctx      : (optional) used for request cancellation
//   - table    : name of the table
//   - keyFilter: map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes
//   - condition: (optional) a condition that must be satisfied before removing item
func (adc *AwsDynamodbConnect) DeleteItem(ctx aws.Context, table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder) (*dynamodb.DeleteItemOutput, error) {
	input, err := adc.BuildDeleteItemInput(table, keyFilter, condition)
	if err != nil {
		return nil, err
	}
	return adc.DeleteItemWithInput(ctx, input)
}

// BuildGetItemInput is a helper function to build dynamodb.GetItemInput.
//
// Notes:
//   - All projected attributes will be fetched.
//   - ConsistentRead is not set.
//
// Available: since v0.2.6
func (adc *AwsDynamodbConnect) BuildGetItemInput(table string, keyFilter map[string]interface{}) (*dynamodb.GetItemInput, error) {
	return &dynamodb.GetItemInput{
		Key:                    awsDynamodbMakeKey(keyFilter),
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:              aws.String(table),
	}, nil
}

// GetItemWithInput executes DynamoDB "get-item" operation.
//
// Available: since v0.2.6
func (adc *AwsDynamodbConnect) GetItemWithInput(ctx aws.Context, input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	return adc.GetDbProxy().GetItemWithContext(ctx, input)
}

// GetItem fetches a single item from specified table.
//
// Parameters:
//   - ctx      : (optional) used for request cancellation
//   - table    : name of the table
//   - keyFilter: map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes
//
// Notes:
//   - If item does not exist, this function returns (nil, nil).
//   - All projected attributes will be fetched.
//   - ConsistentRead is not set.
func (adc *AwsDynamodbConnect) GetItem(ctx aws.Context, table string, keyFilter map[string]interface{}) (AwsDynamodbItem, error) {
	input, err := adc.BuildGetItemInput(table, keyFilter)
	if err != nil {
		return nil, err
	}
	dbresult, err := adc.GetItemWithInput(ctx, input)
	if err != nil || dbresult.Item == nil {
		return nil, AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceNotFoundException)
	}
	return awsDynamodbToItem(dbresult.Item)
}

// BuildUpdateItemInput is a helper function to build dynamodb.UpdateItemInput.
//
// Parameters:
//   - table                 : name of the table
//   - keyFilter             : map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes
//   - condition             : (optional) a condition that must be satisfied before updating item
//   - attrsToRemove         : for REMOVE expression: completely remove attributes from matched items
//   - attrsAndValuesToSet   : for SET expression: completely override attribute values of matched items (new attributes are added if not exist)
//   - attrsAndValuesToAdd   : for ADD expression: if the argument value is a number, increment/decrement the existing attribute value;
//                             if the argument value is a set, it is added to the existing set
//   - attrsAndValuesToDelete: for DELETE expression: the argument value must be a set, it is subtracted from the existing set
//
// Note: at least one of attrsToRemove, attrsAndValuesToSet, attrsAndValuesToAdd, attrsAndValuesToDelete must be provided.
//
// See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
//
// Available: since v0.2.6
func (adc *AwsDynamodbConnect) BuildUpdateItemInput(table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder,
	attrsToRemove []string, attrsAndValuesToSet, attrsAndValuesToAdd, attrsAndValuesToDelete map[string]interface{}) (*dynamodb.UpdateItemInput, error) {
	key := awsDynamodbMakeKey(keyFilter)

	var updateBuilder expression.UpdateBuilder
	if attrsToRemove != nil {
		for _, attr := range attrsToRemove {
			updateBuilder = updateBuilder.Remove(expression.Name(attr))
		}
	}
	if attrsAndValuesToSet != nil {
		for attr, value := range attrsAndValuesToSet {
			updateBuilder = updateBuilder.Set(expression.Name(attr), expression.Value(value))
		}
	}
	if attrsAndValuesToAdd != nil {
		for attr, value := range attrsAndValuesToAdd {
			updateBuilder = updateBuilder.Add(expression.Name(attr), expression.Value(value))
		}
	}
	if attrsAndValuesToDelete != nil {
		for attr, value := range attrsAndValuesToDelete {
			updateBuilder = updateBuilder.Delete(expression.Name(attr), expression.Value(value))
		}
	}

	builder := expression.NewBuilder().WithUpdate(updateBuilder)
	if condition != nil {
		builder = builder.WithCondition(*condition)
	}
	updateBuilderExp, err := builder.Build()
	if err != nil {
		return nil, err
	}

	return &dynamodb.UpdateItemInput{
		ConditionExpression:       updateBuilderExp.Condition(),
		ExpressionAttributeNames:  updateBuilderExp.Names(),
		ExpressionAttributeValues: updateBuilderExp.Values(),
		Key:                       key,
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:                 aws.String(table),
		UpdateExpression:          updateBuilderExp.Update(),
	}, nil
}

// UpdateItemWithInput executes DynamoDB "update-item" operation.
//
// Available: since v0.2.6
func (adc *AwsDynamodbConnect) UpdateItemWithInput(ctx aws.Context, input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	return adc.GetDbProxy().UpdateItemWithContext(ctx, input)
}

// UpdateItem performs operation remove/set/add value/delete values from item's attributes.
//
// Parameters:
//   - ctx                   : (optional) used for request cancellation
//   - table                 : name of the table
//   - keyFilter             : map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes
//   - condition             : (optional) a condition that must be satisfied before updating item
//   - attrsToRemove         : for REMOVE expression: completely remove attributes from matched items
//   - attrsAndValuesToSet   : for SET expression: completely override attribute values of matched items (new attributes are added if not exist)
//   - attrsAndValuesToAdd   : for ADD expression: if the argument value is a number, increment/decrement the existing attribute value;
//                             if the argument value is a set, it is added to the existing set
//   - attrsAndValuesToDelete: for DELETE expression: the argument value must be a set, it is subtracted from the existing set
//
// Note: at least one of attrsToRemove, attrsAndValuesToSet, attrsAndValuesToAdd, attrsAndValuesToDelete must be provided.
//
// See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
func (adc *AwsDynamodbConnect) UpdateItem(ctx aws.Context, table string,
	keyFilter map[string]interface{}, condition *expression.ConditionBuilder,
	attrsToRemove []string, attrsAndValuesToSet, attrsAndValuesToAdd, attrsAndValuesToDelete map[string]interface{}) (*dynamodb.UpdateItemOutput, error) {
	input, err := adc.BuildUpdateItemInput(table, keyFilter, condition, attrsToRemove, attrsAndValuesToSet, attrsAndValuesToAdd, attrsAndValuesToDelete)
	if err != nil {
		return nil, err
	}
	return adc.UpdateItemWithInput(ctx, input)
}

// RemoveAttributes removes one or more attributes from matched items.
//
// Parameters:
//   - ctx          : (optional) used for request cancellation
//   - table        : name of the table
//   - keyFilter    : map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes
//   - condition    : (optional) a condition that must be satisfied before updating item
//   - attrsToRemove: list of attributes to remove
//
// This function invokes UpdateItem's REMOVE expression and passes the argument "attrsToRemove" along the function call.
func (adc *AwsDynamodbConnect) RemoveAttributes(ctx aws.Context, table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsToRemove []string) (*dynamodb.UpdateItemOutput, error) {
	return adc.UpdateItem(ctx, table, keyFilter, condition, attrsToRemove, nil, nil, nil)
}

// SetAttributes sets value of one or more attributes of matched items.
//
// Parameters:
//   - ctx                : (optional) used for request cancellation
//   - table              : name of the table
//   - keyFilter          : map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes
//   - condition          : (optional) a condition that must be satisfied before updating item
//   - attrsAndValuesToSet: list of attributes and values to set
//
// This function invokes UpdateItem's SET expression and passes the argument "attrsAndValuesToSet" along the function call.
func (adc *AwsDynamodbConnect) SetAttributes(ctx aws.Context, table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsAndValuesToSet map[string]interface{}) (*dynamodb.UpdateItemOutput, error) {
	return adc.UpdateItem(ctx, table, keyFilter, condition, nil, attrsAndValuesToSet, nil, nil)
}

// AddValuesToAttributes adds values to one or more attributes of matched items.
//
// Parameters:
//   - ctx                : (optional) used for request cancellation
//   - table              : name of the table
//   - keyFilter          : map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes
//   - condition          : (optional) a condition that must be satisfied before updating item
//   - attrsAndValuesToAdd: list of attributes and values to add
//
// This function invokes UpdateItem's ADD expression and passes the argument "attrsAndValuesToAdd" along the function call.
func (adc *AwsDynamodbConnect) AddValuesToAttributes(ctx aws.Context, table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsAndValues map[string]interface{}) (*dynamodb.UpdateItemOutput, error) {
	return adc.UpdateItem(ctx, table, keyFilter, condition, nil, nil, attrsAndValues, nil)
}

// AddValuesToSet adds entry to one or more set-type attributes of matched items.
//
// Parameters:
//   - ctx                : (optional) used for request cancellation
//   - table              : name of the table
//   - keyFilter          : map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes
//   - condition          : (optional) a condition that must be satisfied before updating item
//   - attrsAndValuesToAdd: attributes and values to add
//
// This function is exclusive for set-type attributes. It invokes UpdateItem's ADD expression and passes the argument "attrsAndValuesToAdd" along the function call.
func (adc *AwsDynamodbConnect) AddValuesToSet(ctx aws.Context, table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsAndValuesToAdd map[string]interface{}) (*dynamodb.UpdateItemOutput, error) {
	avcopy := make(map[string]interface{})
	for k, v := range attrsAndValuesToAdd {
		avcopy[k] = AwsDynamodbToAttributeSet(v)
	}
	return adc.UpdateItem(ctx, table, keyFilter, condition, nil, nil, avcopy, nil)
}

// DeleteValuesFromSet deletes entry from one or more set-type attributes of matched items.
//
// Parameters:
//   - ctx                   : (optional) used for request cancellation
//   - table                 : name of the table
//   - keyFilter             : map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes
//   - condition             : (optional) a condition that must be satisfied before updating item
//   - attrsAndValuesToDelete: attributes and values to delete
//
// This function is exclusive for set-type attributes. It invokes UpdateItem's DELETE expression and passes the argument "attrsAndValuesToDelete" along the function call.
func (adc *AwsDynamodbConnect) DeleteValuesFromSet(ctx aws.Context, table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsAndValuesToDelete map[string]interface{}) (*dynamodb.UpdateItemOutput, error) {
	avcopy := make(map[string]interface{})
	for k, v := range attrsAndValuesToDelete {
		avcopy[k] = AwsDynamodbToAttributeSet(v)
	}
	return adc.UpdateItem(ctx, table, keyFilter, condition, nil, nil, nil, avcopy)
}

// BuildScanInput is a helper function to build dynamodb.ScanInput.
//
// Notes: default options
//   - Only projected attributes will be fetched.
//   - ConsistentRead is not set.
//   - Limit number of processed items to 100.
//
// Available: since v0.2.6
func (adc *AwsDynamodbConnect) BuildScanInput(table string, filter *expression.ConditionBuilder, indexName string, exclusiveStartKey map[string]*dynamodb.AttributeValue) (*dynamodb.ScanInput, error) {
	input := &dynamodb.ScanInput{
		ExclusiveStartKey:      exclusiveStartKey,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:              aws.String(table),
		Limit:                  aws.Int64(100),
	}
	if filter != nil {
		filterExp, err := expression.NewBuilder().WithFilter(*filter).Build()
		if err != nil {
			return nil, err
		}
		input.ExpressionAttributeNames = filterExp.Names()
		input.ExpressionAttributeValues = filterExp.Values()
		input.FilterExpression = filterExp.Filter()
	}
	var useIndex = indexName != ""
	if useIndex {
		input.IndexName = aws.String(indexName)
		// input.Select = aws.String("ALL_ATTRIBUTES")
	}
	return input, nil
}

// ScanWithInputCallback executes DynamoDB "scan" operation.
//
// Note: This function may not fetch all item's attributes when using secondary index and not all attributes are projected to the index.
//
// Available: since v0.2.6
func (adc *AwsDynamodbConnect) ScanWithInputCallback(ctx aws.Context, input *dynamodb.ScanInput, callback AwsDynamodbItemCallback) error {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	for {
		dbresult, err := adc.GetDbProxy().ScanWithContext(ctx, input)
		if err != nil {
			return err
		}
		for _, item := range dbresult.Items {
			myitem, err := awsDynamodbToItem(item)
			if err != nil {
				return err
			}
			ok, err := callback(myitem, dbresult.LastEvaluatedKey)
			if !ok || err != nil {
				return err
			}
		}
		if dbresult.LastEvaluatedKey == nil {
			return nil
		}
		input.ExclusiveStartKey = dbresult.LastEvaluatedKey
	}
}

// ScanWithInput executes DynamoDB "scan" operation.
//
// Note: This function may not fetch all item's attributes when using secondary index and not all attributes are projected to the index.
//
// Available: since v0.2.6
func (adc *AwsDynamodbConnect) ScanWithInput(ctx aws.Context, input *dynamodb.ScanInput) ([]AwsDynamodbItem, error) {
	result := make([]AwsDynamodbItem, 0)
	var callback AwsDynamodbItemCallback = func(item AwsDynamodbItem, _ map[string]*dynamodb.AttributeValue) (bool, error) {
		if item != nil {
			result = append(result, item)
		}
		return true, nil
	}
	return result, adc.ScanWithInputCallback(ctx, input, callback)
}

// ScanItemsWithCallback fetches multiple items from specified table using "scan" operation.
//
// Parameters:
//   - ctx              : (optional) used for request cancellation
//   - table            : name of the table to be scanned
//   - filter           : (optional) used to filter scanned items
//   - indexName        : if non-empty, use this secondary index to scan (local or global)
//   - exclusiveStartKey: (optional) skip items till this key (used for paging)
//   - callback         : callback function
//
// Notes:
//   - This function may not fetch all item's attributes when using secondary index and not all attributes are projected to the index.
//   - All projected attributes will be fetched.
//   - ConsistentRead is not set.
func (adc *AwsDynamodbConnect) ScanItemsWithCallback(ctx aws.Context, table string, filter *expression.ConditionBuilder, indexName string, exclusiveStartKey map[string]*dynamodb.AttributeValue, callback AwsDynamodbItemCallback) error {
	input, err := adc.BuildScanInput(table, filter, indexName, exclusiveStartKey)
	if err != nil {
		return err
	}
	return adc.ScanWithInputCallback(ctx, input, callback)
}

// ScanItems fetches multiple items from specified table using "scan" operation.
//
// Parameters:
//   - ctx              : (optional) used for request cancellation
//   - table            : name of the table to be scanned
//   - filter           : (optional) used to filter scanned items
//   - indexName        : if non-empty, use this secondary index to scan (local or global)
//
// Notes:
//   - This function may not fetch all item's attributes when using secondary index and not all attributes are projected to the index.
//   - All projected attributes will be fetched.
//   - ConsistentRead is not set.
func (adc *AwsDynamodbConnect) ScanItems(ctx aws.Context, table string, filter *expression.ConditionBuilder, indexName string) ([]AwsDynamodbItem, error) {
	input, err := adc.BuildScanInput(table, filter, indexName, nil)
	if err != nil {
		return nil, err
	}
	return adc.ScanWithInput(ctx, input)
}

// BuildQueryInput is a helper function to build dynamodb.QueryInput.
//
// Notes:
//   - Only projected attributes will be fetched.
//   - ConsistentRead is not set.
//   - Limit number of processed items to 100.
//
// Available: since v0.2.6
func (adc *AwsDynamodbConnect) BuildQueryInput(table string, keyFilter, nonKeyFilter *expression.ConditionBuilder, indexName string, exclusiveStartKey map[string]*dynamodb.AttributeValue) (*dynamodb.QueryInput, error) {
	input := &dynamodb.QueryInput{
		ExclusiveStartKey:      exclusiveStartKey,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:              aws.String(table),
		Limit:                  aws.Int64(100),
	}
	if keyFilter != nil || nonKeyFilter != nil {
		builder := expression.NewBuilder()
		if keyFilter != nil {
			builder = builder.WithCondition(*keyFilter)
		}
		if nonKeyFilter != nil {
			builder = builder.WithFilter(*nonKeyFilter)
		}
		filterExp, err := builder.Build()
		if err != nil {
			return nil, err
		}
		input.ExpressionAttributeNames = filterExp.Names()
		input.ExpressionAttributeValues = filterExp.Values()
		if keyFilter != nil {
			input.KeyConditionExpression = filterExp.Condition()
		}
		if nonKeyFilter != nil {
			input.FilterExpression = filterExp.Filter()
		}
	}
	var useIndex = indexName != ""
	if useIndex {
		input.IndexName = aws.String(indexName)
	}
	return input, nil
}

// QueryWithInputCallback executes DynamoDB "query" operation.
//
// Note: This function may not fetch all item's attributes when using secondary index and not all attributes are projected to the index.
//
// Available: since v0.2.6
func (adc *AwsDynamodbConnect) QueryWithInputCallback(ctx aws.Context, input *dynamodb.QueryInput, callback AwsDynamodbItemCallback) error {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	for {
		dbresult, err := adc.GetDbProxy().QueryWithContext(ctx, input)
		if err != nil {
			return err
		}
		for _, item := range dbresult.Items {
			myitem, err := awsDynamodbToItem(item)
			if err != nil {
				return err
			}
			ok, err := callback(myitem, dbresult.LastEvaluatedKey)
			if !ok || err != nil {
				return err
			}
		}
		if dbresult.LastEvaluatedKey == nil {
			return nil
		}
		input.ExclusiveStartKey = dbresult.LastEvaluatedKey
	}
}

// QueryWithInput executes DynamoDB "query" operation.
//
// Note: This function may not fetch all item's attributes when using secondary index and not all attributes are projected to the index.
//
// Available: since v0.2.6
func (adc *AwsDynamodbConnect) QueryWithInput(ctx aws.Context, input *dynamodb.QueryInput) ([]AwsDynamodbItem, error) {
	result := make([]AwsDynamodbItem, 0)
	var callback AwsDynamodbItemCallback = func(item AwsDynamodbItem, _ map[string]*dynamodb.AttributeValue) (bool, error) {
		if item != nil {
			result = append(result, item)
		}
		return true, nil
	}
	return result, adc.QueryWithInputCallback(ctx, input, callback)
}

// QueryItemsWithCallback fetches multiple items from specified table using "query" operation.
//
// Parameters:
//   - ctx              : (optional) used for request cancellation
//   - table            : name of the table to be scanned
//   - keyFilter        : used to filter items on primary key attributes
//   - nonKeyFilter     : used to filter items on non-primary key attributes before returning result
//   - indexName        : if non-empty, use this secondary index to query (local or global)
//   - exclusiveStartKey: (optional) skip items till this key (used for paging)
//   - callback         : callback function
//   - opts             : additional query options (since v0.2.15)
//
// Notes:
//   - This function may not fetch all item's attributes when using secondary index and not all attributes are projected to the index.
//   - All projected attributes will be fetched.
//   - ConsistentRead is not set.
func (adc *AwsDynamodbConnect) QueryItemsWithCallback(ctx aws.Context, table string, keyFilter, nonKeyFilter *expression.ConditionBuilder,
	indexName string, exclusiveStartKey map[string]*dynamodb.AttributeValue, callback AwsDynamodbItemCallback,
	opts ...AwsQueryOpt) error {
	scanBackward := false
	for _, opt := range opts {
		if opt.ScanIndexBackward != nil {
			scanBackward = *opt.ScanIndexBackward
		}
	}
	input, err := adc.BuildQueryInput(table, keyFilter, nonKeyFilter, indexName, exclusiveStartKey)
	if err != nil {
		return err
	}
	if scanBackward {
		input.ScanIndexForward = aws.Bool(false)
	}
	return adc.QueryWithInputCallback(ctx, input, callback)
}

// QueryItems fetches multiple items from specified table using "query" operation.
//
// Parameters:
//   - ctx              : (optional) used for request cancellation
//   - table            : name of the table to be scanned
//   - keyFilter        : used to filter items on primary key attributes
//   - nonKeyFilter     : used to filter items on non-primary key attributes before returning result
//   - indexName        : if non-empty, use this secondary index to scan (local or global)
//   - opts             : additional query options (since v0.2.15)
//
// Notes:
//   - This function may not fetch all item's attributes when using secondary index and not all attributes are projected to the index.
//   - All projected attributes will be fetched.
//   - ConsistentRead is not set.
func (adc *AwsDynamodbConnect) QueryItems(ctx aws.Context, table string, keyFilter, nonKeyFilter *expression.ConditionBuilder,
	indexName string, opts ...AwsQueryOpt) ([]AwsDynamodbItem, error) {
	scanBackward := false
	for _, opt := range opts {
		if opt.ScanIndexBackward != nil {
			scanBackward = *opt.ScanIndexBackward
		}
	}
	input, err := adc.BuildQueryInput(table, keyFilter, nonKeyFilter, indexName, nil)
	if err != nil {
		return nil, err
	}
	if scanBackward {
		input.ScanIndexForward = aws.Bool(false)
	}
	return adc.QueryWithInput(ctx, input)
}

/*----------------------------------------------------------------------*/

// BuildTxPut builds a 'dynamodb.TransactWriteItem' with "insert or replace" operation.
//
// Parameters: see PutItem
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) BuildTxPut(table string, item interface{}, condition *expression.ConditionBuilder) (*dynamodb.TransactWriteItem, error) {
	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return nil, err
	}
	return adc.BuildTxPutRaw(table, av, condition)
}

// BuildTxPutRaw builds a 'dynamodb.TransactWriteItem' with "insert or replace" operation.
//
// Parameters: see PutItemRaw
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) BuildTxPutRaw(table string, item map[string]*dynamodb.AttributeValue, condition *expression.ConditionBuilder) (*dynamodb.TransactWriteItem, error) {
	tx := &dynamodb.Put{
		Item:      item,
		TableName: aws.String(table),
	}
	if condition != nil {
		conditionExp, err := expression.NewBuilder().WithCondition(*condition).Build()
		if err != nil {
			return nil, err
		}
		tx.ConditionExpression = conditionExp.Condition()
		tx.ExpressionAttributeNames = conditionExp.Names()
		tx.ExpressionAttributeValues = conditionExp.Values()
	}
	return &dynamodb.TransactWriteItem{Put: tx}, nil
}

// BuildTxPutIfNotExist builds a 'dynamodb.TransactWriteItem' with "insert if not exist" operation.
//
// Parameters: see PutItemIfNotExist
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) BuildTxPutIfNotExist(table string, item interface{}, pkAttrs []string) (*dynamodb.TransactWriteItem, error) {
	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return nil, err
	}
	return adc.BuildTxPutIfNotExistRaw(table, av, pkAttrs)
}

// BuildTxPutIfNotExistRaw builds a 'dynamodb.TransactWriteItem' with "insert if not exist" operation.
//
// Parameters: see PutItemIfNotExistRaw
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) BuildTxPutIfNotExistRaw(table string, item map[string]*dynamodb.AttributeValue, pkAttrs []string) (*dynamodb.TransactWriteItem, error) {
	return adc.BuildTxPutRaw(table, item, AwsDynamodbNotExistsAllBuilder(pkAttrs))
}

// BuildTxDelete builds a 'dynamodb.TransactWriteItem' with "delete" operation.
//
// Parameters: see DeleteItem
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) BuildTxDelete(table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder) (*dynamodb.TransactWriteItem, error) {
	tx := &dynamodb.Delete{
		Key:       awsDynamodbMakeKey(keyFilter),
		TableName: aws.String(table),
	}
	if condition != nil {
		conditionExp, err := expression.NewBuilder().WithCondition(*condition).Build()
		if err != nil {
			return nil, err
		}
		tx.ExpressionAttributeNames = conditionExp.Names()
		tx.ExpressionAttributeValues = conditionExp.Values()
		tx.ConditionExpression = conditionExp.Condition()
	}
	return &dynamodb.TransactWriteItem{Delete: tx}, nil
}

// BuildTxUpdateRaw builds a 'dynamodb.TransactWriteItem' with "update" operation.
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) BuildTxUpdateRaw(table string, key map[string]*dynamodb.AttributeValue, updateExpression expression.Expression) (*dynamodb.TransactWriteItem, error) {
	return &dynamodb.TransactWriteItem{Update: &dynamodb.Update{
		ConditionExpression:       updateExpression.Condition(),
		ExpressionAttributeNames:  updateExpression.Names(),
		ExpressionAttributeValues: updateExpression.Values(),
		Key:                       key,
		TableName:                 aws.String(table),
		UpdateExpression:          updateExpression.Update(),
	}}, nil
}

// BuildTxUpdate builds a 'dynamodb.TransactWriteItem' with "update" operation.
//
// Parameters: see UpdateItem
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) BuildTxUpdate(table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder,
	attrsToRemove []string, attrsAndValuesToSet, attrsAndValuesToAdd, attrsAndValuesToDelete map[string]interface{}) (*dynamodb.TransactWriteItem, error) {
	key := awsDynamodbMakeKey(keyFilter)

	var updateBuilder expression.UpdateBuilder
	if attrsToRemove != nil {
		for _, attr := range attrsToRemove {
			updateBuilder = updateBuilder.Remove(expression.Name(attr))
		}
	}
	if attrsAndValuesToSet != nil {
		for attr, value := range attrsAndValuesToSet {
			updateBuilder = updateBuilder.Set(expression.Name(attr), expression.Value(value))
		}
	}
	if attrsAndValuesToAdd != nil {
		for attr, value := range attrsAndValuesToAdd {
			updateBuilder = updateBuilder.Add(expression.Name(attr), expression.Value(value))
		}
	}
	if attrsAndValuesToDelete != nil {
		for attr, value := range attrsAndValuesToDelete {
			updateBuilder = updateBuilder.Delete(expression.Name(attr), expression.Value(value))
		}
	}

	builder := expression.NewBuilder().WithUpdate(updateBuilder)
	if condition != nil {
		builder = builder.WithCondition(*condition)
	}
	updateBuilderExp, err := builder.Build()
	if err != nil {
		return nil, err
	}
	return adc.BuildTxUpdateRaw(table, key, updateBuilderExp)
}

// BuildTxRemoveAttributes builds a 'dynamodb.TransactWriteItem' that removes one or more attributes from matched items.
//
// Parameters: see RemoveAttributes
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) BuildTxRemoveAttributes(table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsToRemove []string) (*dynamodb.TransactWriteItem, error) {
	return adc.BuildTxUpdate(table, keyFilter, condition, attrsToRemove, nil, nil, nil)
}

// BuildTxSetAttributes builds a 'dynamodb.TransactWriteItem' that sets value of one or more attributes of matched items.
//
// Parameters: see SetAttributes
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) BuildTxSetAttributes(table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsAndValuesToSet map[string]interface{}) (*dynamodb.TransactWriteItem, error) {
	return adc.BuildTxUpdate(table, keyFilter, condition, nil, attrsAndValuesToSet, nil, nil)
}

// BuildTxAddValuesToAttributes builds a 'dynamodb.TransactWriteItem' that adds values to one or more attributes of matched items.
//
// Parameters: see AddValuesToAttributes
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) BuildTxAddValuesToAttributes(table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsAndValuesToAdd map[string]interface{}) (*dynamodb.TransactWriteItem, error) {
	return adc.BuildTxUpdate(table, keyFilter, condition, nil, nil, attrsAndValuesToAdd, nil)
}

// BuildTxAddValuesToSet builds a 'dynamodb.TransactWriteItem' that adds entry to one or more set-type attributes of matched items.
//
// Parameters: see AddValuesToSet
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) BuildTxAddValuesToSet(table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsAndValuesToAdd map[string]interface{}) (*dynamodb.TransactWriteItem, error) {
	avcopy := make(map[string]interface{})
	for k, v := range attrsAndValuesToAdd {
		avcopy[k] = AwsDynamodbToAttributeSet(v)
	}
	return adc.BuildTxUpdate(table, keyFilter, condition, nil, nil, avcopy, nil)
}

// BuildTxDeleteValuesFromSet builds a 'dynamodb.TransactWriteItem' that deletes entry from one or more set-type attributes of matched items.
//
// Parameters: see DeleteValuesFromSet
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) BuildTxDeleteValuesFromSet(table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsAndValuesToDelete map[string]interface{}) (*dynamodb.TransactWriteItem, error) {
	avcopy := make(map[string]interface{})
	for k, v := range attrsAndValuesToDelete {
		avcopy[k] = AwsDynamodbToAttributeSet(v)
	}
	return adc.BuildTxUpdate(table, keyFilter, condition, nil, nil, nil, avcopy)
}

// BuildTxGet builds a 'dynamodb.TransactGetItem' with "get item" operation.
//
// Parameters: see GetItem
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) BuildTxGet(table string, keyFilter map[string]interface{}) (*dynamodb.TransactGetItem, error) {
	key := awsDynamodbMakeKey(keyFilter)
	return &dynamodb.TransactGetItem{Get: &dynamodb.Get{
		Key:       key,
		TableName: aws.String(table),
	}}, nil
}

// WrapTxWriteItems is convenient function to execute a "write-items" transaction.
//
// Available since v0.3.0
func (adc *AwsDynamodbConnect) WrapTxWriteItems(ctx aws.Context, clientRequestToken string, items ...*dynamodb.TransactWriteItem) (*dynamodb.TransactWriteItemsOutput, error) {
	input := &dynamodb.TransactWriteItemsInput{
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TransactItems:          items,
	}
	if clientRequestToken != "" {
		input.ClientRequestToken = aws.String(clientRequestToken)
	}
	return adc.ExecTxWriteItems(ctx, input)
}

// ExecTxWriteItems executes a "write-items" transaction.
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) ExecTxWriteItems(ctx aws.Context, input *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	return adc.GetDbProxy().TransactWriteItemsWithContext(ctx, input)
}

// WrapTxGetItems is convenient function to execute a "get-items" transaction.
//
// Available since v0.3.0
func (adc *AwsDynamodbConnect) WrapTxGetItems(ctx aws.Context, items ...*dynamodb.TransactGetItem) ([]AwsDynamodbItem, error) {
	input := &dynamodb.TransactGetItemsInput{
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TransactItems:          items,
	}
	dbresult, err := adc.ExecTxGetItems(ctx, input)
	if err != nil || dbresult.Responses == nil {
		return nil, AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceNotFoundException)
	}
	result := make([]AwsDynamodbItem, 0, len(dbresult.Responses))
	for _, row := range dbresult.Responses {
		item, err := awsDynamodbToItem(row.Item)
		if err != nil {
			return nil, err
		}
		if item != nil {
			result = append(result, item)
		}
	}
	return result, nil
}

// ExecTxGetItems executes a "get-items" transaction.
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) ExecTxGetItems(ctx aws.Context, input *dynamodb.TransactGetItemsInput) (*dynamodb.TransactGetItemsOutput, error) {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	return adc.GetDbProxy().TransactGetItemsWithContext(ctx, input)
}
