package prom

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/btnguyen2k/consu/reddo"
	"github.com/pkg/errors"
	"reflect"
	"strconv"
	"time"
)

const (
	// AwsDynamodbNoIndex indicates that no index will be used
	AwsDynamodbNoIndex = ""
)

/*
AwsDynamodbItem defines a generic structure for DynamoDB item.
*/
type AwsDynamodbItem map[string]interface{}

/*
AwsDynamodbNameAndType defines a generic name & type pair.
*/
type AwsDynamodbNameAndType struct{ Name, Type string }

/*
AwsDynamodbItemCallback defines callback interface for "scan"/"query" operation.

If callback function returns false or error, the scan/query process will stop (even if there are still more items).
*/
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

/*
AwsDynamodbToAttributeValue converts a Go value to DynamoDB's attribute value.
*/
func AwsDynamodbToAttributeValue(v interface{}) *dynamodb.AttributeValue {
	if av, err := dynamodbattribute.Marshal(v); err != nil {
		panic(err)
	} else {
		return av
	}
}

/*
AwsDynamodbToAttributeSet converts a Go value to DynamoDB's set.
*/
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

/*
AwsIgnoreErrorIfMatched returns nil if err is an awserr.Error and its code equals to excludeCode.
*/
func AwsIgnoreErrorIfMatched(err error, excludeCode string) error {
	if err == nil {
		return nil
	}
	if aerr, ok := err.(awserr.Error); ok && aerr.Code() == excludeCode {
		return nil
	}
	return err
}

/*
AwsDynamodbExistsAllBuilder builds a expression.ConditionBuilder where all attributes must exist.
*/
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

/*
AwsDynamodbNotExistsAllBuilder builds a expression.ConditionBuilder where all attributes must not exist.
*/
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

/*
AwsDynamodbEqualsBuilder builds a expression.ConditionBuilder with condition attr1=value1 AND attr1=value1 AND...

Parameters:

  - condition: format {attribute-name:attribute-value}
*/
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

/*
AwsDynamodbConnect holds a AWS DynamoDB client (https://github.com/aws/aws-sdk-go/tree/master/service/dynamodb) that can be shared within the application.
*/
type AwsDynamodbConnect struct {
	config            *aws.Config        // aws config instance
	session           *session.Session   // aws session insntance
	db                *dynamodb.DynamoDB // aws dynamodb instance
	timeoutMs         int                // timeout in milliseconds
	ownDb, ownSession bool
}

/*
NewAwsDynamodbConnect constructs a new AwsDynamodbConnect instance.

Parameters:

  - cfg             : aws.Config instance
  - sess            : session.Session instance
  - db              :  dynamodb.DynamoDB instance
  - defaultTimeoutMs: default timeout for db operations, in milliseconds

Return: the AwsDynamodbConnect instance and error (if any). Note:

  - if db is nil, it will be built from session
  - if session is nil, it sill be built from config
  - at least one of {config, session, db} must not be nil
*/
func NewAwsDynamodbConnect(cfg *aws.Config, sess *session.Session, db *dynamodb.DynamoDB, defaultTimeoutMs int) (*AwsDynamodbConnect, error) {
	if cfg == nil && sess == nil && db == nil {
		return nil, errors.Errorf("At least one of {config, session, db} must not be nil.")
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
			return nil, errors.Errorf("Cannot create DynamoDB client instance.")
		}
	}
	return adc, nil
}

/*
Close frees all resources and closes all connection associated with this AwsDynamodbConnect.
*/
func (adc *AwsDynamodbConnect) Close() error {
	return nil
}

/*
GetDb returns the underlying dynamodb.DynamoDB instance.
*/
func (adc *AwsDynamodbConnect) GetDb() *dynamodb.DynamoDB {
	return adc.db
}

/*
NewContext creates a new context with specified timeout in milliseconds.
If there is no specified timeout, or timeout value is less than or equal to 0, the default timeout is used.
*/
func (adc *AwsDynamodbConnect) NewContext(timeoutMs ...int) (aws.Context, context.CancelFunc) {
	d := adc.timeoutMs
	if len(timeoutMs) > 0 && timeoutMs[0] > 0 {
		d = timeoutMs[0]
	}
	return context.WithTimeout(context.Background(), time.Duration(d)*time.Millisecond)
}

/*
ListTables returns all visible tables.

Parameters:

  - ctx: (optional) used for request cancellation
*/
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

/*
HasTable checks if a table exists.

Parameters:

  - ctx: (optional) used for request cancellation
*/
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

/*
DeleteTable deletes an existing table.

Parameters:

  - ctx: (optional) used for request cancellation

Note: DynamoDB table is deleted asynchronously. Use GetTableStatus to check table's existence.
*/
func (adc *AwsDynamodbConnect) DeleteTable(ctx aws.Context, table string) error {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	_, err := adc.db.DeleteTableWithContext(ctx, &dynamodb.DeleteTableInput{TableName: aws.String(table)})
	return AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceNotFoundException)
}

/*
CreateTable create a new table without index.

Parameters:

  - ctx: (optional) used for request cancellation
  - table   : name of the table to be created
  - rcu     : ReadCapacityUnits (0 means PAY_PER_REQUEST)
  - wcu     : WriteCapacityUnits (0 means PAY_PER_REQUEST)
  - attrDefs: table attributes, where attribute-type is either "S", "N" or "B"
  - pkDefs  : primary key definitions, where key-type is either "HASH" or "RANGE"

Note: DynamoDB table is created asynchronously. Use GetTableStatus to check table's existence.
*/
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
	return AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceInUseException)
}

/*
GetTableStatus fetches and returns table status.

Parameters:

  - ctx: (optional) used for request cancellation

Notes:

  - If table does not exist, this function returns "", nil
*/
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

/*
GetGlobalSecondaryIndexStatus fetches and returns a table's GSI status.

Parameters:

  - ctx: (optional) used for request cancellation

Notes:

  - If index does not exist, this function returns "", nil
*/
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

/*
CreateGlobalSecondaryIndex creates a Global Secondary Index on a specified table.

Parameters:

  - ctx: (optional) used for request cancellation
  - table    : name of the table
  - indexName: name of the index to be created
  - rcu      : ReadCapacityUnits (0 means PAY_PER_REQUEST)
  - wcu      : WriteCapacityUnits (0 means PAY_PER_REQUEST)
  - attrDefs : GSI attributes, where attribute-type is either "S", "N" or "B"
  - keyAttrs : GSI key schema, where key-type is either "HASH" or "RANGE"

Note: DynamoDB GSI is created asynchronously. Use GetGlobalSecondaryIndexStatus to check GSI's existence.
*/
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

/*
DeleteGlobalSecondaryIndex deletes a Global Secondary Index on a specified table.

Parameters:

  - ctx: (optional) used for request cancellation

Note: DynamoDB GSI is deleted asynchronously. Use GetGlobalSecondaryIndexStatus to check GSI's existence.
*/
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

/*
PutItemRaw inserts a new item to table or replace an existing one.

Parameters:

  - ctx      : (optional) used for request cancellation
  - table    : name of the table
  - item     : item to be inserted
  - condition: (optional) a condition that must be satisfied before writing item
*/
func (adc *AwsDynamodbConnect) PutItemRaw(ctx aws.Context, table string, item map[string]*dynamodb.AttributeValue, condition *expression.ConditionBuilder) (*dynamodb.PutItemOutput, error) {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
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
	dbresult, err := adc.db.PutItemWithContext(ctx, input)
	return dbresult, AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeConditionalCheckFailedException)
}

/*
PutItemRawIfNotExist inserts a new item to table only if it does not exist.

Parameters:

  - ctx    : (optional) used for request cancellation
  - table  : name of the table
  - item     : item to be inserted
  - pkAttrs: primary key attribute names
*/
func (adc *AwsDynamodbConnect) PutItemRawIfNotExist(ctx aws.Context, table string, item map[string]*dynamodb.AttributeValue, pkAttrs []string) (*dynamodb.PutItemOutput, error) {
	return adc.PutItemRaw(ctx, table, item, AwsDynamodbNotExistsAllBuilder(pkAttrs))
}

/*
PutItem inserts a new item to table or replace an existing one.

Parameters:

  - ctx: (optional) used for request cancellation
  - table    : name of the table
  - item     : item to be inserted (a map or struct), will be converted to map[string]*dynamodb.AttributeValue via dynamodbattribute.MarshalMap(item)
  - condition: (optional) a condition that must be satisfied before writing item
*/
func (adc *AwsDynamodbConnect) PutItem(ctx aws.Context, table string, item interface{}, condition *expression.ConditionBuilder) (*dynamodb.PutItemOutput, error) {
	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return nil, err
	}
	return adc.PutItemRaw(ctx, table, av, condition)
}

/*
PutItemIfNotExist inserts a new item to table only if it does not exist.

Parameters:

  - ctx: (optional) used for request cancellation
  - table  : name of the table
  - item     : item to be inserted (a map or struct), will be converted to map[string]*dynamodb.AttributeValue via dynamodbattribute.MarshalMap(item)
  - pkAttrs: primary key attribute names
*/
func (adc *AwsDynamodbConnect) PutItemIfNotExist(ctx aws.Context, table string, item interface{}, pkAttrs []string) (*dynamodb.PutItemOutput, error) {
	return adc.PutItem(ctx, table, item, AwsDynamodbNotExistsAllBuilder(pkAttrs))
}

/*
DeleteItem removes a single item from specified table.

Parameters:

  - ctx: (optional) used for request cancellation
  - table    : name of the table
  - keyFilter: map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes
  - condition: (optional) a condition that must be satisfied before removing item
*/
func (adc *AwsDynamodbConnect) DeleteItem(ctx aws.Context, table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder) (*dynamodb.DeleteItemOutput, error) {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
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
	dbresult, err := adc.db.DeleteItemWithContext(ctx, input)
	if aerr, ok := err.(awserr.Error); ok && aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
		return dbresult, nil
	}
	return dbresult, err
}

/*
GetItem fetches a single item from specified table.

Parameters:

  - ctx: (optional) used for request cancellation
  - table    : name of the table
  - keyFilter: map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes

Notes:

  - If item does not exist, this function returns (nil, nil).
  - This function fetches all attributes of items.
  - ConsistentRead is not used.
*/
func (adc *AwsDynamodbConnect) GetItem(ctx aws.Context, table string, keyFilter map[string]interface{}) (AwsDynamodbItem, error) {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	key := awsDynamodbMakeKey(keyFilter)
	dbresult, err := adc.db.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		Key:                    key,
		ReturnConsumedCapacity: aws.String("INDEXES"),
		TableName:              aws.String(table),
	})
	if err != nil || dbresult.Item == nil {
		return nil, AwsIgnoreErrorIfMatched(err, dynamodb.ErrCodeResourceNotFoundException)
	}
	return awsDynamodbToItem(dbresult.Item)
}

func (adc *AwsDynamodbConnect) doUpdateItem(ctx aws.Context, table string, key map[string]*dynamodb.AttributeValue, updateExpression expression.Expression) (*dynamodb.UpdateItemOutput, error) {
	input := &dynamodb.UpdateItemInput{
		ConditionExpression:       updateExpression.Condition(),
		ExpressionAttributeNames:  updateExpression.Names(),
		ExpressionAttributeValues: updateExpression.Values(),
		Key:                       key,
		ReturnConsumedCapacity:    aws.String("INDEXES"),
		TableName:                 aws.String(table),
		UpdateExpression:          updateExpression.Update(),
	}
	dbresult, err := adc.db.UpdateItemWithContext(ctx, input)
	if aerr, ok := err.(awserr.Error); ok && aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
		return dbresult, nil
	}
	return dbresult, err
}

/*
UpdateItem performs operation remove/set/add value/delete values from item's attributes.

Parameters:

  - ctx                   : (optional) used for request cancellation
  - table                 : name of the table
  - keyFilter             : map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes
  - condition             : (optional) a condition that must be satisfied before updating item
  - attrsToRemove         : list of attributes to remove
  - attrsAndValuesToSet   : list of attributes and values to set
  - attrsAndValuesToAdd   : list of attributes and values to add
  - attrsAndValuesToDelete: list of attributes and values to delete

Notes: at least one of attrsToRemove, attrsAndValuesToSet, attrsAndValuesToAdd, attrsAndValuesToDelete must be provided

See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
*/
func (adc *AwsDynamodbConnect) UpdateItem(ctx aws.Context, table string,
	keyFilter map[string]interface{}, condition *expression.ConditionBuilder,
	attrsToRemove []string, attrsAndValuesToSet, attrsAndValuesToAdd, attrsAndValuesToDelete map[string]interface{}) (*dynamodb.UpdateItemOutput, error) {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
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
	return adc.doUpdateItem(ctx, table, key, updateBuilderExp)
}

/*
RemoveAttributes removes one or more attributes from an item.

Parameters:

  - ctx      : (optional) used for request cancellation
  - table    : name of the table
  - keyFilter: map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes
  - condition: (optional) a condition that must be satisfied before updating item
  - attrs    : list of attributes to remove

See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
*/
func (adc *AwsDynamodbConnect) RemoveAttributes(ctx aws.Context, table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrs []string) (*dynamodb.UpdateItemOutput, error) {
	return adc.UpdateItem(ctx, table, keyFilter, condition, attrs, nil, nil, nil)
}

/*
SetAttributes sets one or more attributes of an item.

Parameters:

  - ctx           : (optional) used for request cancellation
  - table         : name of the table
  - keyFilter     : map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes
  - condition     : (optional) a condition that must be satisfied before updating item
  - attrsAndValues: list of attributes and values to set

See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
*/
func (adc *AwsDynamodbConnect) SetAttributes(ctx aws.Context, table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsAndValues map[string]interface{}) (*dynamodb.UpdateItemOutput, error) {
	return adc.UpdateItem(ctx, table, keyFilter, condition, nil, attrsAndValues, nil, nil)
}

/*
AddValuesToAttributes adds values to one or more attributes of an item.

Parameters:

  - ctx           : (optional) used for request cancellation
  - table         : name of the table
  - keyFilter     : map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes
  - condition     : (optional) a condition that must be satisfied before updating item
  - attrsAndValues: list of attributes and values to add

See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html

Note: currently can not add value to a set using this function. To add value to a set, use AddValuesToSet. See: https://github.com/aws/aws-sdk-go/issues/1990
*/
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
	dbresult, err := adc.db.UpdateItemWithContext(ctx, input)
	if aerr, ok := err.(awserr.Error); ok && aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
		return dbresult, nil
	}
	return dbresult, err
}

/*
AddValuesToSet adds values to set attributes of an item.

Parameters:

  - ctx           : (optional) used for request cancellation
  - table         : name of the table
  - keyFilter     : map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes
  - condition     : (optional) a condition that must be satisfied before updating item
  - attrsAndValues: list of attributes and values to add

See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
*/
func (adc *AwsDynamodbConnect) AddValuesToSet(ctx aws.Context, table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsAndValues map[string]interface{}) (*dynamodb.UpdateItemOutput, error) {
	return adc.doAddOrDeleteSetValues(ctx, table, keyFilter, condition, attrsAndValues, true)
}

/*
DeleteValuesFromAttributes deletes values from one or more set attributes of an item.

Parameters:

  - ctx           : (optional) used for request cancellation
  - table         : name of the table
  - keyFilter     : map of {primary-key-attribute-name:attribute-value}, must include all primary key's attributes
  - condition     : (optional) a condition that must be satisfied before updating item
  - attrsAndValues: list of attributes and values to delete

See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
*/
func (adc *AwsDynamodbConnect) DeleteValuesFromAttributes(ctx aws.Context, table string, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsAndValues map[string]interface{}) (*dynamodb.UpdateItemOutput, error) {
	return adc.doAddOrDeleteSetValues(ctx, table, keyFilter, condition, attrsAndValues, false)
}

/*
ScanItemsWithCallback fetches multiple items from specified table using "scan" operation.

Parameters:

  - ctx              : (optional) used for request cancellation
  - table            : name of the table to be scanned
  - filter           : (optional) used to filter scanned items
  - indexName        : if non-empty, use this secondary index to scan (local or global)
  - exclusiveStartKey: (optional) skip items till this key (used for paging)
  - callback         : callback function

Notes:

  - This function may not fetch all item's attributes when using secondary index and not all attributes are projected to the index.
  - ConsistentRead is not used.
*/
func (adc *AwsDynamodbConnect) ScanItemsWithCallback(ctx aws.Context, table string, filter *expression.ConditionBuilder, indexName string, exclusiveStartKey map[string]*dynamodb.AttributeValue, callback AwsDynamodbItemCallback) error {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	input := &dynamodb.ScanInput{
		ExclusiveStartKey:      exclusiveStartKey,
		ReturnConsumedCapacity: aws.String("INDEXES"),
		TableName:              aws.String(table),
	}
	if filter != nil {
		filterExp, err := expression.NewBuilder().WithFilter(*filter).Build()
		if err != nil {
			return err
		}
		input.ExpressionAttributeNames = filterExp.Names()
		input.ExpressionAttributeValues = filterExp.Values()
		input.FilterExpression = filterExp.Filter()
	}
	var useIndex = indexName != ""
	if useIndex {
		input.IndexName = aws.String(indexName)
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

/*
ScanItems fetches multiple items from specified table using "scan" operation.

Parameters:

  - ctx              : (optional) used for request cancellation
  - table            : name of the table to be scanned
  - filter           : (optional) used to filter scanned items
  - indexName        : if non-empty, use this secondary index to scan (local or global)

Notes:

  - This function may not fetch all item's attributes when using secondary index and not all attributes are projected to the index.
  - ConsistentRead is not used.
*/
func (adc *AwsDynamodbConnect) ScanItems(ctx aws.Context, table string, filter *expression.ConditionBuilder, indexName string) ([]AwsDynamodbItem, error) {
	result := make([]AwsDynamodbItem, 0)
	var callback AwsDynamodbItemCallback = func(item AwsDynamodbItem, _ map[string]*dynamodb.AttributeValue) (bool, error) {
		if item != nil {
			result = append(result, item)
		}
		return true, nil
	}
	err := adc.ScanItemsWithCallback(ctx, table, filter, indexName, nil, callback)
	return result, err
}

/*
QueryItemsWithCallback fetches multiple items from specified table using "query" operation.

Parameters:

  - ctx              : (optional) used for request cancellation
  - table            : name of the table to be queried
  - keyFilter        : used to filter items on primary key attributes
  - nonKeyFilter     : used to filter items on non-primary key attributes before returning result
  - indexName        : if non-empty, use this secondary index to query (local or global)
  - exclusiveStartKey: (optional) skip items till this key (used for paging)
  - callback         : callback function

Notes:

  - This function may not fetch all item's attributes when using secondary index and not all attributes are projected to the index.
  - ConsistentRead is not used.
*/
func (adc *AwsDynamodbConnect) QueryItemsWithCallback(ctx aws.Context, table string, keyFilter, nonKeyFilter *expression.ConditionBuilder, indexName string, exclusiveStartKey map[string]*dynamodb.AttributeValue, callback AwsDynamodbItemCallback) error {
	if ctx == nil {
		ctx, _ = adc.NewContext()
	}
	builder := expression.NewBuilder().WithCondition(*keyFilter)
	if nonKeyFilter != nil {
		builder = builder.WithFilter(*nonKeyFilter)
	}
	filterExp, err := builder.Build()
	if err != nil {
		return err
	}
	input := &dynamodb.QueryInput{
		ExpressionAttributeNames:  filterExp.Names(),
		ExpressionAttributeValues: filterExp.Values(),
		ExclusiveStartKey:         exclusiveStartKey,
		ReturnConsumedCapacity:    aws.String("INDEXES"),
		KeyConditionExpression:    filterExp.Condition(),
		TableName:                 aws.String(table),
	}
	if nonKeyFilter != nil {
		input.FilterExpression = filterExp.Filter()
	}
	var useIndex = indexName != ""
	if useIndex {
		input.IndexName = aws.String(indexName)
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

/*
QueryItems fetches multiple items from specified table using "query" operation.

Parameters:

  - ctx              : (optional) used for request cancellation
  - table            : name of the table to be scanned
  - keyFilter        : used to filter items on primary key attributes
  - nonKeyFilter     : used to filter items on non-primary key attributes before returning result
  - indexName        : if non-empty, use this secondary index to scan (local or global)

Notes:

  - This function may not fetch all item's attributes when using secondary index and not all attributes are projected to the index.
  - ConsistentRead is not used.
*/
func (adc *AwsDynamodbConnect) QueryItems(ctx aws.Context, table string, keyFilter, nonKeyFilter *expression.ConditionBuilder, indexName string) ([]AwsDynamodbItem, error) {
	result := make([]AwsDynamodbItem, 0)
	var callback AwsDynamodbItemCallback = func(item AwsDynamodbItem, _ map[string]*dynamodb.AttributeValue) (bool, error) {
		var err error
		if item != nil {
			result = append(result, item)
		}
		return true, err
	}
	err := adc.QueryItemsWithCallback(ctx, table, keyFilter, nonKeyFilter, indexName, nil, callback)
	return result, err
}
