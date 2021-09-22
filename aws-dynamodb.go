package prom

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
)

// // AwsDynamodbConsistentReadLevel specifies the level of read consistency.
// //
// // Available: since v0.2.6
// type AwsDynamodbConsistentReadLevel int
//
// // Predefined levels of read consistency.
// //
// // Available: since v0.2.6
// const (
// 	AwsDynamodbConsistentReadLevelNone AwsDynamodbConsistentReadLevel = iota
// )

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
		if delay.Milliseconds() > 0 {
			time.Sleep(delay)
		}
		if timeout.Milliseconds() > 0 && time.Now().Sub(start) > timeout {
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
	item := AwsDynamodbItem{}
	err := dynamodbattribute.UnmarshalMap(input, &item)
	return item, err
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

// AwsIgnoreErrorIfMatched returns nil if err is an awserr.Error and its code equals to excludeCode.
func AwsIgnoreErrorIfMatched(err error, excludeCode string) error {
	if aerr, ok := err.(awserr.Error); ok && aerr.Code() == excludeCode {
		return nil
	}
	return err
}

// AwsDynamodbExistsAllBuilder builds a expression.ConditionBuilder where all attributes must exist.
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

// AwsDynamodbConnect holds a AWS DynamoDB client (https://github.com/aws/aws-sdk-go/tree/master/service/dynamodb) that can be shared within the application.
type AwsDynamodbConnect struct {
	config            *aws.Config        // aws config instance
	session           *session.Session   // aws session insntance
	db                *dynamodb.DynamoDB // aws dynamodb instance
	timeoutMs         int                // timeout in milliseconds
	ownDb, ownSession bool
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
//   - if db is nil, it will be built from session
//   - if session is nil, it sill be built from config
//   - at least one of {config, session, db} must not be nil
func NewAwsDynamodbConnect(cfg *aws.Config, sess *session.Session, db *dynamodb.DynamoDB, defaultTimeoutMs int) (*AwsDynamodbConnect, error) {
	if cfg == nil && sess == nil && db == nil {
		return nil, errors.New("at least one of {config, session, db} must not be nil")
	}
	if defaultTimeoutMs < 0 {
		defaultTimeoutMs = 0
	}
	adc := &AwsDynamodbConnect{
		config:    cfg,
		session:   sess,
		db:        db,
		timeoutMs: defaultTimeoutMs,
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
	return adc, nil
}

// Close frees all resources and closes all connection associated with this AwsDynamodbConnect.
func (adc *AwsDynamodbConnect) Close() error {
	return nil
}

// GetDb returns the underlying dynamodb.DynamoDB instance.
func (adc *AwsDynamodbConnect) GetDb() *dynamodb.DynamoDB {
	return adc.db
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
	for {
		dbresult, err := adc.db.ListTablesWithContext(ctx, input)
		if err != nil {
			return result, err
		}
		for _, n := range dbresult.TableNames {
			result = append(result, *n)
		}
		if dbresult.LastEvaluatedTableName == nil {
			break
		}
		// assign the last read table-name as the start for our next call to the ListTables function
		// the maximum number of table names returned in a call is 100 (default), which requires us to make
		// multiple calls to the ListTables function to retrieve all table names
		input.ExclusiveStartTableName = dbresult.LastEvaluatedTableName
	}
	return result, nil
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
	for {
		dbresult, err := adc.db.ListTablesWithContext(ctx, input)
		if err != nil {
			return false, err
		}
		for _, n := range dbresult.TableNames {
			if table == *n {
				return true, nil
			}
		}
		if dbresult.LastEvaluatedTableName == nil {
			break
		}
		// assign the last read table-name as the start for our next call to the ListTables function
		// the maximum number of table names returned in a call is 100 (default), which requires us to make
		// multiple calls to the ListTables function to retrieve all table names
		input.ExclusiveStartTableName = dbresult.LastEvaluatedTableName
	}
	return false, nil
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
	_, err := adc.db.DeleteTableWithContext(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(table)})
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
	_, err := adc.db.CreateTableWithContext(ctx, input)
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
	status, err := adc.db.DescribeTableWithContext(ctx, input)
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
	status, err := adc.db.DescribeTableWithContext(ctx, input)
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
// Note: DynamoDB GSI is created asynchronously. Use GetGlobalSecondaryIndexStatus to check GSI's existence.
func (adc *AwsDynamodbConnect) CreateGlobalSecondaryIndex(ctx aws.Context, table, indexName string, rcu, wcu int64, attrDefs, keyAttrs []AwsDynamodbNameAndType) error {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	action := &dynamodb.CreateGlobalSecondaryIndexAction{
		IndexName:             aws.String(indexName),
		KeySchema:             awsDynamodbToKeySchemaElement(keyAttrs),
		Projection:            &dynamodb.Projection{ProjectionType: aws.String("KEYS_ONLY")},
		ProvisionedThroughput: awsDynamodbToProvisionedThroughput(rcu, wcu),
	}
	gscIndexes := []*dynamodb.GlobalSecondaryIndexUpdate{{Create: action}}
	input := &dynamodb.UpdateTableInput{
		AttributeDefinitions:        awsDynamodbToAttributeDefinitions(attrDefs),
		GlobalSecondaryIndexUpdates: gscIndexes,
		TableName:                   aws.String(table),
	}
	_, err := adc.db.UpdateTableWithContext(ctx, input)
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
	_, err := adc.db.UpdateTableWithContext(ctx, input)
	return AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceNotFoundException)
}

// BuildPutItemInput is a helper function to build dynamodb.PutItemInput.
//
// Available: since v0.2.6
func (adc *AwsDynamodbConnect) BuildPutItemInput(table string, item map[string]*dynamodb.AttributeValue, condition *expression.ConditionBuilder) (*dynamodb.PutItemInput, error) {
	input := &dynamodb.PutItemInput{
		Item:                   item,
		ReturnConsumedCapacity: aws.String("INDEXES"),
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
	return adc.db.PutItemWithContext(ctx, input)
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

// PutItemRawIfNotExist inserts a new item to table only if it does not exist.
//
// Parameters:
//   - ctx    : (optional) used for request cancellation
//   - table  : name of the table
//   - item   : item to be inserted
//   - pkAttrs: primary key attribute names
func (adc *AwsDynamodbConnect) PutItemRawIfNotExist(ctx aws.Context, table string, item map[string]*dynamodb.AttributeValue, pkAttrs []string) (*dynamodb.PutItemOutput, error) {
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
// Note:
//   - (since v0.2.7) if item already existed, this function return (nil, nil)
func (adc *AwsDynamodbConnect) PutItemIfNotExist(ctx aws.Context, table string, item interface{}, pkAttrs []string) (*dynamodb.PutItemOutput, error) {
	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return nil, err
	}
	return adc.PutItemRawIfNotExist(ctx, table, av, pkAttrs)
}

// BuildDeleteItemInput is a helper function to build dynamodb.DeleteItemInput.
//
// Available: since v0.2.6
func (adc *AwsDynamodbConnect) BuildDeleteItemInput(table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder) (*dynamodb.DeleteItemInput, error) {
	key := awsDynamodbMakeKey(keyFilter)
	input := &dynamodb.DeleteItemInput{
		Key:                    key,
		ReturnConsumedCapacity: aws.String("INDEXES"),
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
	return adc.db.DeleteItemWithContext(ctx, input)
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
		ReturnConsumedCapacity: aws.String("INDEXES"),
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
	return adc.db.GetItemWithContext(ctx, input)
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
//   - attrsToRemove         : list of attributes to remove
//   - attrsAndValuesToSet   : list of attributes and values to set
//   - attrsAndValuesToAdd   : list of attributes and values to add
//   - attrsAndValuesToDelete: list of attributes and values to delete
//
// Note: at least one of attrsToRemove, attrsAndValuesToSet, attrsAndValuesToAdd, attrsAndValuesToDelete must be provided
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
		ReturnConsumedCapacity:    aws.String("INDEXES"),
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
	return adc.db.UpdateItemWithContext(ctx, input)
}

// UpdateItem performs operation remove/set/add value/delete values from item's attributes.
//
// Parameters:
//   - ctx                   : (optional) used for request cancellation
//   - table                 : name of the table
//   - keyFilter             : map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes
//   - condition             : (optional) a condition that must be satisfied before updating item
//   - attrsToRemove         : list of attributes to remove
//   - attrsAndValuesToSet   : list of attributes and values to set
//   - attrsAndValuesToAdd   : list of attributes and values to add
//   - attrsAndValuesToDelete: list of attributes and values to delete
//
// Note: at least one of attrsToRemove, attrsAndValuesToSet, attrsAndValuesToAdd, attrsAndValuesToDelete must be provided
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

// RemoveAttributes removes one or more attributes from an item.
//
// Parameters:
//   - ctx      : (optional) used for request cancellation
//   - table    : name of the table
//   - keyFilter: map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes
//   - condition: (optional) a condition that must be satisfied before updating item
//   - attrs    : list of attributes to remove
//
// See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
func (adc *AwsDynamodbConnect) RemoveAttributes(ctx aws.Context, table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrs []string) (*dynamodb.UpdateItemOutput, error) {
	return adc.UpdateItem(ctx, table, keyFilter, condition, attrs, nil, nil, nil)
}

// SetAttributes sets one or more attributes of an item.
//
// Parameters:
//   - ctx           : (optional) used for request cancellation
//   - table         : name of the table
//   - keyFilter     : map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes
//   - condition     : (optional) a condition that must be satisfied before updating item
//   - attrsAndValues: list of attributes and values to set
//
// See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
func (adc *AwsDynamodbConnect) SetAttributes(ctx aws.Context, table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsAndValues map[string]interface{}) (*dynamodb.UpdateItemOutput, error) {
	return adc.UpdateItem(ctx, table, keyFilter, condition, nil, attrsAndValues, nil, nil)
}

// AddValuesToAttributes adds values to one or more attributes of an item.
//
// Parameters:
//   - ctx           : (optional) used for request cancellation
//   - table         : name of the table
//   - keyFilter     : map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes
//   - condition     : (optional) a condition that must be satisfied before updating item
//   - attrsAndValues: list of attributes and values to add
//
// See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
//
// Note:
//   - "add" is math operation in this context, hence the target attribute and the value to add must be numbers
//   - value can be added to top-level as well as nested attributes
//   - currently can not add value to a set using this function. To add value to a set, use AddValuesToSet. See: https://github.com/aws/aws-sdk-go/issues/1990
func (adc *AwsDynamodbConnect) AddValuesToAttributes(ctx aws.Context, table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsAndValues map[string]interface{}) (*dynamodb.UpdateItemOutput, error) {
	return adc.UpdateItem(ctx, table, keyFilter, condition, nil, nil, attrsAndValues, nil)
}

func (adc *AwsDynamodbConnect) doAddOrDeleteSetValues(ctx aws.Context, table string,
	keyFilter map[string]interface{}, condition *expression.ConditionBuilder,
	attrsAndValues map[string]interface{},
	doAdd bool) (*dynamodb.UpdateItemOutput, error) {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	key := awsDynamodbMakeKey(keyFilter)
	avs := make(map[string]*dynamodb.AttributeValue)
	var updateBuilder expression.UpdateBuilder
	for attr, value := range attrsAndValues {
		if doAdd {
			updateBuilder = updateBuilder.Add(expression.Name(attr), expression.Value(value))
		} else {
			updateBuilder = updateBuilder.Delete(expression.Name(attr), expression.Value(value))
		}
		avs[attr] = AwsDynamodbToAttributeSet(value)
	}
	builder := expression.NewBuilder().WithUpdate(updateBuilder)
	if condition != nil {
		builder = builder.WithCondition(*condition)
	}
	updateBuilderExp, err := builder.Build()
	if err != nil {
		return nil, err
	}
	names := updateBuilderExp.Names()
	values := updateBuilderExp.Values()
	for attr, value := range avs {
		for k, v := range names {
			if *v == attr {
				values[":"+k[1:]] = value
				break
			}
		}
	}
	input := &dynamodb.UpdateItemInput{
		ConditionExpression:       updateBuilderExp.Condition(),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
		Key:                       key,
		ReturnConsumedCapacity:    aws.String("INDEXES"),
		TableName:                 aws.String(table),
		UpdateExpression:          updateBuilderExp.Update(),
	}
	return adc.db.UpdateItemWithContext(ctx, input)
}

// AddValuesToSet adds values to set attributes of an item.
//
// Parameters:
//   - ctx           : (optional) used for request cancellation
//   - table         : name of the table
//   - keyFilter     : map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes
//   - condition     : (optional) a condition that must be satisfied before updating item
//   - attrsAndValues: list of attributes and values to add
//
// See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
func (adc *AwsDynamodbConnect) AddValuesToSet(ctx aws.Context, table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsAndValues map[string]interface{}) (*dynamodb.UpdateItemOutput, error) {
	return adc.doAddOrDeleteSetValues(ctx, table, keyFilter, condition, attrsAndValues, true)
}

// DeleteValuesFromSet deletes values from one or more set attributes of an item.
//
// Parameters:
//   - ctx           : (optional) used for request cancellation
//   - table         : name of the table
//   - keyFilter     : map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes
//   - condition     : (optional) a condition that must be satisfied before updating item
//   - attrsAndValues: list of attributes and values to delete
//
// See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
func (adc *AwsDynamodbConnect) DeleteValuesFromSet(ctx aws.Context, table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsAndValues map[string]interface{}) (*dynamodb.UpdateItemOutput, error) {
	return adc.doAddOrDeleteSetValues(ctx, table, keyFilter, condition, attrsAndValues, false)
}

// BuildScanInput is a helper function to build dynamodb.ScanInput.
//
// Notes:
//   - All projected attributes will be fetched.
//   - ConsistentRead is not set.
//
// Available: since v0.2.6
func (adc *AwsDynamodbConnect) BuildScanInput(table string, filter *expression.ConditionBuilder, indexName string, exclusiveStartKey map[string]*dynamodb.AttributeValue) (*dynamodb.ScanInput, error) {
	input := &dynamodb.ScanInput{
		ExclusiveStartKey:      exclusiveStartKey,
		ReturnConsumedCapacity: aws.String("INDEXES"),
		TableName:              aws.String(table),
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
		dbresult, err := adc.db.ScanWithContext(ctx, input)
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
//   - All projected attributes will be fetched.
//   - ConsistentRead is not set.
//
// Available: since v0.2.6
func (adc *AwsDynamodbConnect) BuildQueryInput(table string, keyFilter, nonKeyFilter *expression.ConditionBuilder, indexName string, exclusiveStartKey map[string]*dynamodb.AttributeValue) (*dynamodb.QueryInput, error) {
	input := &dynamodb.QueryInput{
		ExclusiveStartKey:      exclusiveStartKey,
		ReturnConsumedCapacity: aws.String("INDEXES"),
		TableName:              aws.String(table),
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
		dbresult, err := adc.db.QueryWithContext(ctx, input)
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
//   - table            : name of the table to be queried
//   - keyFilter        : used to filter items on primary key attributes
//   - nonKeyFilter     : used to filter items on non-primary key attributes before returning result
//   - indexName        : if non-empty, use this secondary index to query (local or global)
//   - exclusiveStartKey: (optional) skip items till this key (used for paging)
//   - callback         : callback function
//
// Notes:
//   - This function may not fetch all item's attributes when using secondary index and not all attributes are projected to the index.
//   - All projected attributes will be fetched.
//   - ConsistentRead is not set.
func (adc *AwsDynamodbConnect) QueryItemsWithCallback(ctx aws.Context, table string, keyFilter, nonKeyFilter *expression.ConditionBuilder, indexName string, exclusiveStartKey map[string]*dynamodb.AttributeValue, callback AwsDynamodbItemCallback) error {
	input, err := adc.BuildQueryInput(table, keyFilter, nonKeyFilter, indexName, exclusiveStartKey)
	if err != nil {
		return err
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
//
// Notes:
//   - This function may not fetch all item's attributes when using secondary index and not all attributes are projected to the index.
//   - All projected attributes will be fetched.
//   - ConsistentRead is not set.
func (adc *AwsDynamodbConnect) QueryItems(ctx aws.Context, table string, keyFilter, nonKeyFilter *expression.ConditionBuilder, indexName string) ([]AwsDynamodbItem, error) {
	input, err := adc.BuildQueryInput(table, keyFilter, nonKeyFilter, indexName, nil)
	if err != nil {
		return nil, err
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
	put := &dynamodb.Put{
		Item:      item,
		TableName: aws.String(table),
	}
	if condition != nil {
		conditionExp, err := expression.NewBuilder().WithCondition(*condition).Build()
		if err != nil {
			return nil, err
		}
		put.ConditionExpression = conditionExp.Condition()
		put.ExpressionAttributeNames = conditionExp.Names()
		put.ExpressionAttributeValues = conditionExp.Values()
	}
	return &dynamodb.TransactWriteItem{Put: put}, nil
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
	return adc.BuildTxPutRawIfNotExist(table, av, pkAttrs)
}

// BuildTxPutRawIfNotExist builds a 'dynamodb.TransactWriteItem' with "insert if not exist" operation.
//
// Parameters: see PutItemRawIfNotExist
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) BuildTxPutRawIfNotExist(table string, item map[string]*dynamodb.AttributeValue, pkAttrs []string) (*dynamodb.TransactWriteItem, error) {
	return adc.BuildTxPutRaw(table, item, AwsDynamodbNotExistsAllBuilder(pkAttrs))
}

// BuildTxDelete builds a 'dynamodb.TransactWriteItem' with "delete" operation.
//
// Parameters: see DeleteItem
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) BuildTxDelete(table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder) (*dynamodb.TransactWriteItem, error) {
	delete := &dynamodb.Delete{
		Key:       awsDynamodbMakeKey(keyFilter),
		TableName: aws.String(table),
	}
	if condition != nil {
		conditionExp, err := expression.NewBuilder().WithCondition(*condition).Build()
		if err != nil {
			return nil, err
		}
		delete.ExpressionAttributeNames = conditionExp.Names()
		delete.ExpressionAttributeValues = conditionExp.Values()
		delete.ConditionExpression = conditionExp.Condition()
	}
	return &dynamodb.TransactWriteItem{Delete: delete}, nil
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

// BuildTxRemoveAttributes builds a 'dynamodb.TransactWriteItem' with "remote attributes" operation.
//
// Parameters: see RemoveAttributes
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) BuildTxRemoveAttributes(table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrs []string) (*dynamodb.TransactWriteItem, error) {
	return adc.BuildTxUpdate(table, keyFilter, condition, attrs, nil, nil, nil)
}

// BuildTxSetAttributes builds a 'dynamodb.TransactWriteItem' with "set attributes" operation.
//
// Parameters: see SetAttributes
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) BuildTxSetAttributes(table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsAndValues map[string]interface{}) (*dynamodb.TransactWriteItem, error) {
	return adc.BuildTxUpdate(table, keyFilter, condition, nil, attrsAndValues, nil, nil)
}

// BuildTxAddValuesToAttributes builds a 'dynamodb.TransactWriteItem' with "add values to attributes" operation.
//
// Parameters: see AddValuesToAttributes
//
// Note: currently can not add value to a set using this function. To add value to a set, use BuildTxAddValuesToSet. See: https://github.com/aws/aws-sdk-go/issues/1990
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) BuildTxAddValuesToAttributes(table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsAndValues map[string]interface{}) (*dynamodb.TransactWriteItem, error) {
	return adc.BuildTxUpdate(table, keyFilter, condition, nil, nil, attrsAndValues, nil)
}

func (adc *AwsDynamodbConnect) buildTxAddOrDeleteSetValues(table string, keyFilter map[string]interface{},
	condition *expression.ConditionBuilder, attrsAndValues map[string]interface{}, doAdd bool) (*dynamodb.TransactWriteItem, error) {
	key := awsDynamodbMakeKey(keyFilter)
	avs := make(map[string]*dynamodb.AttributeValue)
	var updateBuilder expression.UpdateBuilder
	for attr, value := range attrsAndValues {
		if doAdd {
			updateBuilder = updateBuilder.Add(expression.Name(attr), expression.Value(value))
		} else {
			updateBuilder = updateBuilder.Delete(expression.Name(attr), expression.Value(value))
		}
		avs[attr] = AwsDynamodbToAttributeSet(value)
	}
	builder := expression.NewBuilder().WithUpdate(updateBuilder)
	if condition != nil {
		builder = builder.WithCondition(*condition)
	}
	updateBuilderExp, err := builder.Build()
	if err != nil {
		return nil, err
	}
	names := updateBuilderExp.Names()
	values := updateBuilderExp.Values()
	for attr, value := range avs {
		for k, v := range names {
			if *v == attr {
				values[":"+k[1:]] = value
				break
			}
		}
	}
	return &dynamodb.TransactWriteItem{Update: &dynamodb.Update{
		ConditionExpression:       updateBuilderExp.Condition(),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
		Key:                       key,
		TableName:                 aws.String(table),
		UpdateExpression:          updateBuilderExp.Update(),
	}}, nil
}

// BuildTxAddValuesToSet builds a 'dynamodb.TransactWriteItem' with "add values to set attributes" operation.
//
// Parameters: see AddValuesToSet
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) BuildTxAddValuesToSet(table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsAndValues map[string]interface{}) (*dynamodb.TransactWriteItem, error) {
	return adc.buildTxAddOrDeleteSetValues(table, keyFilter, condition, attrsAndValues, true)
}

// BuildTxDeleteValuesFromSet builds a 'dynamodb.TransactWriteItem' with "add values to set attributes" operation.
//
// Parameters: see DeleteValuesFromSet
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) BuildTxDeleteValuesFromSet(table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsAndValues map[string]interface{}) (*dynamodb.TransactWriteItem, error) {
	return adc.buildTxAddOrDeleteSetValues(table, keyFilter, condition, attrsAndValues, false)
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

// ExecTxWriteItems executes a "write-items" transaction.
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) ExecTxWriteItems(ctx aws.Context, input *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	return adc.GetDb().TransactWriteItemsWithContext(ctx, input)
}

// ExecTxGetItems executes a "get-items" transaction.
//
// Available: since v0.2.4
func (adc *AwsDynamodbConnect) ExecTxGetItems(ctx aws.Context, input *dynamodb.TransactGetItemsInput) (*dynamodb.TransactGetItemsOutput, error) {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	return adc.GetDb().TransactGetItemsWithContext(ctx, input)
}
