package prom

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"go.mongodb.org/mongo-driver/bson"
)

func _awsDynamodbParameterize(item map[string]interface{}) (string, []*dynamodb.AttributeValue, error) {
	valueList := make([]interface{}, 0, len(item))
	placeholderList := make([]string, 0, len(item))
	for k, v := range item {
		valueList = append(valueList, v)
		placeholderList = append(placeholderList, "'"+k+"': ?")
	}
	params, err := dynamodbattribute.MarshalList(valueList)
	return strings.Join(placeholderList, ","), params, err
}

var _awsDynamodbTestItem = bson.M{
	"username": "btnguyen2k",
	"an":       []float64{1.0, 2.0, 3.0},
	"as":       []string{"1", "2", "3"},
	"a":        []interface{}{},
	"email":    "me@domain.com",
	"m":        map[string]interface{}{},
}

func TestDynamoDbProxy_BatchExecuteStatement(t *testing.T) {
	testName := "TestDynamoDbProxy_BatchExecuteStatement"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	placeholderStm, params, err := _awsDynamodbParameterize(_awsDynamodbTestItem)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	input := &dynamodb.BatchExecuteStatementInput{
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		Statements: []*dynamodb.BatchStatementRequest{
			{
				Parameters: params,
				Statement:  aws.String(fmt.Sprintf("INSERT INTO \"%s\" VALUE {%s}", testDynamodbTableName, placeholderStm)),
			},
		},
	}
	if _, err = db.BatchExecuteStatement(input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbBatchExecStm, nil, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_BatchExecuteStatementWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_BatchExecuteStatementWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	placeholderStm, params, err := _awsDynamodbParameterize(_awsDynamodbTestItem)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	input := &dynamodb.BatchExecuteStatementInput{
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		Statements: []*dynamodb.BatchStatementRequest{
			{
				Parameters: params,
				Statement:  aws.String(fmt.Sprintf("INSERT INTO \"%s\" VALUE {%s}", testDynamodbTableName, placeholderStm)),
			},
		},
	}
	if _, err = db.BatchExecuteStatementWithContext(context.TODO(), input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbBatchExecStm, nil, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_BatchGetItem(t *testing.T) {
	testName := "TestDynamoDbProxy_BatchGetItem"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	keyFilter := bson.M{"username": "btnguyen2k", "email": "me@domain.com"}
	input := &dynamodb.BatchGetItemInput{
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		RequestItems:           map[string]*dynamodb.KeysAndAttributes{testDynamodbTableName: {Keys: []map[string]*dynamodb.AttributeValue{awsDynamodbMakeKey(keyFilter)}}},
	}
	if _, err := db.BatchGetItem(input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbBatchGetItem, nil, MetricsCatAll, MetricsCatDQL)
}

func TestDynamoDbProxy_BatchGetItemWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_BatchGetItemWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	keyFilter := bson.M{"username": "btnguyen2k", "email": "me@domain.com"}
	input := &dynamodb.BatchGetItemInput{
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		RequestItems:           map[string]*dynamodb.KeysAndAttributes{testDynamodbTableName: {Keys: []map[string]*dynamodb.AttributeValue{awsDynamodbMakeKey(keyFilter)}}},
	}
	if _, err := db.BatchGetItemWithContext(context.TODO(), input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbBatchGetItem, nil, MetricsCatAll, MetricsCatDQL)
}

func TestDynamoDbProxy_BatchGetItemPages(t *testing.T) {
	testName := "TestDynamoDbProxy_BatchGetItemPages"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	keyFilter := bson.M{"username": "btnguyen2k", "email": "me@domain.com"}
	input := &dynamodb.BatchGetItemInput{
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		RequestItems:           map[string]*dynamodb.KeysAndAttributes{testDynamodbTableName: {Keys: []map[string]*dynamodb.AttributeValue{awsDynamodbMakeKey(keyFilter)}}},
	}
	if err := db.BatchGetItemPages(input, func(output *dynamodb.BatchGetItemOutput, b bool) bool {
		return true
	}); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbBatchGetItem, nil, MetricsCatAll, MetricsCatDQL)
}

func TestDynamoDbProxy_BatchGetItemPagesWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_BatchGetItemPagesWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	keyFilter := bson.M{"username": "btnguyen2k", "email": "me@domain.com"}
	input := &dynamodb.BatchGetItemInput{
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		RequestItems:           map[string]*dynamodb.KeysAndAttributes{testDynamodbTableName: {Keys: []map[string]*dynamodb.AttributeValue{awsDynamodbMakeKey(keyFilter)}}},
	}
	if err := db.BatchGetItemPagesWithContext(context.TODO(), input, func(output *dynamodb.BatchGetItemOutput, b bool) bool {
		return true
	}); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbBatchGetItem, nil, MetricsCatAll, MetricsCatDQL)
}

func TestDynamoDbProxy_BatchWriteItem(t *testing.T) {
	testName := "TestDynamoDbProxy_BatchWriteItem"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	keyFilter := bson.M{"username": "thanhn", "email": "me@domain.com"}
	item, _ := dynamodbattribute.MarshalMap(_awsDynamodbTestItem)
	input := &dynamodb.BatchWriteItemInput{
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		RequestItems: map[string][]*dynamodb.WriteRequest{
			testDynamodbTableName: {
				{DeleteRequest: &dynamodb.DeleteRequest{Key: awsDynamodbMakeKey(keyFilter)}},
				{PutRequest: &dynamodb.PutRequest{Item: item}},
			},
		},
	}
	if _, err := db.BatchWriteItem(input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbBatchWriteItem, nil, MetricsCatAll, MetricsCatDML)
}

func TestDynamoDbProxy_BatchWriteItemWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_BatchWriteItemWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	keyFilter := bson.M{"username": "thanhn", "email": "me@domain.com"}
	item, _ := dynamodbattribute.MarshalMap(_awsDynamodbTestItem)
	input := &dynamodb.BatchWriteItemInput{
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		RequestItems: map[string][]*dynamodb.WriteRequest{
			testDynamodbTableName: {
				{DeleteRequest: &dynamodb.DeleteRequest{Key: awsDynamodbMakeKey(keyFilter)}},
				{PutRequest: &dynamodb.PutRequest{Item: item}},
			},
		},
	}
	if _, err := db.BatchWriteItemWithContext(context.TODO(), input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbBatchWriteItem, nil, MetricsCatAll, MetricsCatDML)
}

func TestDynamoDbProxy_CreateBackup(t *testing.T) {
	testName := "TestDynamoDbProxy_CreateBackup"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.CreateBackupInput{BackupName: aws.String("backup_" + testDynamodbTableName), TableName: aws.String(testDynamodbTableName)}
	if _, err := db.CreateBackup(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// TODO: the Docker version does not support CreateBackup operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbCreateBackup, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_CreateBackupWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_CreateBackupWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.CreateBackupInput{BackupName: aws.String("backup_" + testDynamodbTableName), TableName: aws.String(testDynamodbTableName)}
	if _, err := db.CreateBackupWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// TODO: the Docker version does not support CreateBackup operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbCreateBackup, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_CreateGlobalTable(t *testing.T) {
	testName := "TestDynamoDbProxy_CreateGlobalTable"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.CreateGlobalTableInput{
		GlobalTableName: aws.String(testDynamodbTableName),
		ReplicationGroup: []*dynamodb.Replica{
			{RegionName: aws.String("DUMMY")},
		},
	}
	if _, err := db.CreateGlobalTable(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support CreateGlobalTable operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbCreateGlobalTable, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatDDL)
}

func TestDynamoDbProxy_CreateGlobalTableWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_CreateGlobalTableWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.CreateGlobalTableInput{
		GlobalTableName: aws.String(testDynamodbTableName),
		ReplicationGroup: []*dynamodb.Replica{
			{RegionName: aws.String("DUMMY")},
		},
	}
	if _, err := db.CreateGlobalTableWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support CreateGlobalTable operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbCreateGlobalTable, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatDDL)
}

func TestDynamoDbProxy_CreateTable(t *testing.T) {
	testName := "TestDynamoDbProxy_CreateGlobalTableWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	adc.DeleteTable(nil, testDynamodbTableName)
	db := adc.GetDbProxy()
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions:  awsDynamodbToAttributeDefinitions([]AwsDynamodbNameAndType{{"username", AwsAttrTypeString}, {"email", AwsAttrTypeString}}),
		KeySchema:             awsDynamodbToKeySchemaElement([]AwsDynamodbNameAndType{{"username", AwsKeyTypePartition}, {"email", AwsKeyTypeSort}}),
		ProvisionedThroughput: awsDynamodbToProvisionedThroughput(1, 1),
		TableName:             aws.String(testDynamodbTableName),
	}
	if _, err := db.CreateTable(input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbCreateTable, nil, MetricsCatAll, MetricsCatDDL)
}

func TestDynamoDbProxy_CreateTableWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_CreateTableWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	adc.DeleteTable(nil, testDynamodbTableName)
	db := adc.GetDbProxy()
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions:  awsDynamodbToAttributeDefinitions([]AwsDynamodbNameAndType{{"username", AwsAttrTypeString}, {"email", AwsAttrTypeString}}),
		KeySchema:             awsDynamodbToKeySchemaElement([]AwsDynamodbNameAndType{{"username", AwsKeyTypePartition}, {"email", AwsKeyTypeSort}}),
		ProvisionedThroughput: awsDynamodbToProvisionedThroughput(1, 1),
		TableName:             aws.String(testDynamodbTableName),
	}
	if _, err := db.CreateTableWithContext(context.TODO(), input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbCreateTable, nil, MetricsCatAll, MetricsCatDDL)
}

func TestDynamoDbProxy_DeleteBackup(t *testing.T) {
	testName := "TestDynamoDbProxy_CreateTableWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DeleteBackupInput{
		BackupArn: aws.String("arn:aws:dynamodb:dummy:1234567890:table/backup_" + testDynamodbTableName),
	}
	if _, err := db.DeleteBackup(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support DeleteBackup operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDeleteBackup, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DeleteBackupWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_DeleteBackupWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DeleteBackupInput{
		BackupArn: aws.String("arn:aws:dynamodb:dummy:1234567890:table/backup_" + testDynamodbTableName),
	}
	if _, err := db.DeleteBackupWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support DeleteBackup operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDeleteBackup, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DeleteItem(t *testing.T) {
	testName := "TestDynamoDbProxy_DeleteItem"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	keyFilter := bson.M{"username": "btnguyen2k", "email": "me@domain.com"}
	input := &dynamodb.DeleteItemInput{
		Key:                    awsDynamodbMakeKey(keyFilter),
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:              aws.String(testDynamodbTableName),
	}
	if _, err := db.DeleteItem(input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDeleteItem, nil, MetricsCatAll, MetricsCatDML)
}

func TestDynamoDbProxy_DeleteItemWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_DeleteItemWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	keyFilter := bson.M{"username": "btnguyen2k", "email": "me@domain.com"}
	input := &dynamodb.DeleteItemInput{
		Key:                    awsDynamodbMakeKey(keyFilter),
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:              aws.String(testDynamodbTableName),
	}
	if _, err := db.DeleteItemWithContext(context.TODO(), input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDeleteItem, nil, MetricsCatAll, MetricsCatDML)
}

func TestDynamoDbProxy_DeleteTable(t *testing.T) {
	testName := "TestDynamoDbProxy_DeleteTable"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.DeleteTable(input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDeleteTable, nil, MetricsCatAll, MetricsCatDDL)
}

func TestDynamoDbProxy_DeleteTableWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_DeleteTableWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.DeleteTableWithContext(context.TODO(), input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDeleteTable, nil, MetricsCatAll, MetricsCatDDL)
}

func TestDynamoDbProxy_DescribeBackup(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeBackup"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeBackupInput{
		BackupArn: aws.String("arn:aws:dynamodb:dummy:1234567890:table/backup_" + testDynamodbTableName),
	}
	if _, err := db.DescribeBackup(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support DescribeBackup operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescBackup, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeBackupWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeBackupWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeBackupInput{
		BackupArn: aws.String("arn:aws:dynamodb:dummy:1234567890:table/backup_" + testDynamodbTableName),
	}
	if _, err := db.DescribeBackupWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support DescribeBackup operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescBackup, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeContinuousBackups(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeContinuousBackups"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeContinuousBackupsInput{
		TableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.DescribeContinuousBackups(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support DescribeContinuousBackups operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescContinuousBackups, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeContinuousBackupsWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeContinuousBackupsWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeContinuousBackupsInput{
		TableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.DescribeContinuousBackupsWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support DescribeContinuousBackups operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescContinuousBackups, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeContributorInsights(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeContributorInsights"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeContributorInsightsInput{
		TableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.DescribeContributorInsights(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support DescribeContributorInsights operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescContributorInsights, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeContributorInsightsWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeContributorInsightsWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeContributorInsightsInput{
		TableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.DescribeContributorInsightsWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support DescribeContributorInsights operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescContributorInsights, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeEndpoints(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeEndpoints"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeEndpointsInput{}
	if _, err := db.DescribeEndpoints(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support DescribeEndpoints operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescEndpoints, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeEndpointsWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeEndpointsWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeEndpointsInput{}
	if _, err := db.DescribeEndpointsWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support DescribeEndpoints operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescEndpoints, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeExport(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeExport"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeExportInput{
		ExportArn: aws.String("arn:aws:dynamodb:dummy:1234567890:table/" + testDynamodbTableName),
	}
	if _, err := db.DescribeExport(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support DescribeExport operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescExport, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeExportWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeExportWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeExportInput{
		ExportArn: aws.String("arn:aws:dynamodb:dummy:1234567890:table/" + testDynamodbTableName),
	}
	if _, err := db.DescribeExportWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support DescribeExport operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescExport, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeGlobalTable(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeGlobalTable"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeGlobalTableInput{
		GlobalTableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.DescribeGlobalTable(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support DescribeGlobalTable operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescGlobalTable, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeGlobalTableWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeGlobalTableWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeGlobalTableInput{
		GlobalTableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.DescribeGlobalTableWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support DescribeGlobalTable operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescGlobalTable, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeGlobalTableSettings(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeGlobalTableSettings"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeGlobalTableSettingsInput{
		GlobalTableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.DescribeGlobalTableSettings(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support DescribeGlobalTableSettings operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescGlobalTableSettings, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeGlobalTableSettingsWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeGlobalTableSettingsWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeGlobalTableSettingsInput{
		GlobalTableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.DescribeGlobalTableSettingsWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support DescribeGlobalTableSettings operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescGlobalTableSettings, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeKinesisStreamingDestination(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeKinesisStreamingDestination"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeKinesisStreamingDestinationInput{
		TableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.DescribeKinesisStreamingDestination(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support DescribeKinesisStreamingDestination operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescKinesisStreamingDestination, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeKinesisStreamingDestinationWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeKinesisStreamingDestinationWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeKinesisStreamingDestinationInput{
		TableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.DescribeKinesisStreamingDestinationWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support DescribeKinesisStreamingDestination operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescKinesisStreamingDestination, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeLimits(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeLimits"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeLimitsInput{}
	if _, err := db.DescribeLimits(input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescLimits, nil, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeLimitsWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeLimitsWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeLimitsInput{}
	if _, err := db.DescribeLimitsWithContext(context.TODO(), input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescLimits, nil, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeTable(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeTable"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.DescribeTable(input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescTable, nil, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeTableWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeTableWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.DescribeTableWithContext(context.TODO(), input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescTable, nil, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeTableReplicaAutoScaling(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeTableReplicaAutoScaling"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeTableReplicaAutoScalingInput{
		TableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.DescribeTableReplicaAutoScaling(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support DescribeTableReplicaAutoScaling operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescTableReplicaAutoScaling, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeTableReplicaAutoScalingWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeTableReplicaAutoScalingWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeTableReplicaAutoScalingInput{
		TableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.DescribeTableReplicaAutoScalingWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support DescribeTableReplicaAutoScaling operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescTableReplicaAutoScaling, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeTimeToLive(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeTimeToLive"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeTimeToLiveInput{
		TableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.DescribeTimeToLive(input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescTTL, nil, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DescribeTimeToLiveWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_DescribeTimeToLiveWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DescribeTimeToLiveInput{
		TableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.DescribeTimeToLiveWithContext(context.TODO(), input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDescTTL, nil, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DisableKinesisStreamingDestination(t *testing.T) {
	testName := "TestDynamoDbProxy_DisableKinesisStreamingDestination"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DisableKinesisStreamingDestinationInput{
		TableName: aws.String(testDynamodbTableName),
		StreamArn: aws.String("arn:aws:dynamodb:dummy:1234567890:table/stream_" + testDynamodbTableName),
	}
	if _, err := db.DisableKinesisStreamingDestination(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support DisableKinesisStreamingDestination operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDisableKinesisStreamingDestination, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_DisableKinesisStreamingDestinationWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_DisableKinesisStreamingDestinationWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.DisableKinesisStreamingDestinationInput{
		TableName: aws.String(testDynamodbTableName),
		StreamArn: aws.String("arn:aws:dynamodb:dummy:1234567890:table/stream_" + testDynamodbTableName),
	}
	if _, err := db.DisableKinesisStreamingDestinationWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support DisableKinesisStreamingDestination operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbDisableKinesisStreamingDestination, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_EnableKinesisStreamingDestination(t *testing.T) {
	testName := "TestDynamoDbProxy_EnableKinesisStreamingDestination"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.EnableKinesisStreamingDestinationInput{
		TableName: aws.String(testDynamodbTableName),
		StreamArn: aws.String("arn:aws:dynamodb:dummy:1234567890:table/stream_" + testDynamodbTableName),
	}
	if _, err := db.EnableKinesisStreamingDestination(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support EnableKinesisStreamingDestination operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbEnableKinesisStreamingDestination, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_EnableKinesisStreamingDestinationWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_EnableKinesisStreamingDestinationWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.EnableKinesisStreamingDestinationInput{
		TableName: aws.String(testDynamodbTableName),
		StreamArn: aws.String("arn:aws:dynamodb:dummy:1234567890:table/stream_" + testDynamodbTableName),
	}
	if _, err := db.EnableKinesisStreamingDestinationWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support EnableKinesisStreamingDestination operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbEnableKinesisStreamingDestination, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ExecuteStatement(t *testing.T) {
	testName := "TestDynamoDbProxy_ExecuteStatement"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	placeholderStm, params, err := _awsDynamodbParameterize(_awsDynamodbTestItem)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	input := &dynamodb.ExecuteStatementInput{
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		Parameters:             params,
		Statement:              aws.String(fmt.Sprintf("INSERT INTO \"%s\" VALUE {%s}", testDynamodbTableName, placeholderStm)),
	}
	if _, err = db.ExecuteStatement(input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbExecuteStatement, nil, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ExecuteStatementWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_ExecuteStatementWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	placeholderStm, params, err := _awsDynamodbParameterize(_awsDynamodbTestItem)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	input := &dynamodb.ExecuteStatementInput{
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		Parameters:             params,
		Statement:              aws.String(fmt.Sprintf("INSERT INTO \"%s\" VALUE {%s}", testDynamodbTableName, placeholderStm)),
	}
	if _, err = db.ExecuteStatementWithContext(context.TODO(), input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbExecuteStatement, nil, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ExecuteTransaction(t *testing.T) {
	testName := "TestDynamoDbProxy_ExecuteTransaction"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	placeholderStm, params, err := _awsDynamodbParameterize(_awsDynamodbTestItem)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	input := &dynamodb.ExecuteTransactionInput{
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TransactStatements: []*dynamodb.ParameterizedStatement{
			{
				Parameters: params,
				Statement:  aws.String(fmt.Sprintf("INSERT INTO \"%s\" VALUE {%s}", testDynamodbTableName, placeholderStm)),
			},
		},
	}
	if _, err = db.ExecuteTransaction(input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbExecuteTransaction, nil, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ExecuteTransactionWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_ExecuteTransactionWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	placeholderStm, params, err := _awsDynamodbParameterize(_awsDynamodbTestItem)
	if err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	input := &dynamodb.ExecuteTransactionInput{
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TransactStatements: []*dynamodb.ParameterizedStatement{
			{
				Parameters: params,
				Statement:  aws.String(fmt.Sprintf("INSERT INTO \"%s\" VALUE {%s}", testDynamodbTableName, placeholderStm)),
			},
		},
	}
	if _, err = db.ExecuteTransactionWithContext(context.TODO(), input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbExecuteTransaction, nil, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ExportTableToPointInTime(t *testing.T) {
	testName := "TestDynamoDbProxy_ExportTableToPointInTime"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ExportTableToPointInTimeInput{
		S3Bucket: aws.String("dummy"),
		TableArn: aws.String("arn:aws:dynamodb:dummy:1234567890:table/" + testDynamodbTableName),
	}
	if _, err := db.ExportTableToPointInTime(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support ExportTableToPointInTime operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbExportTableToPIT, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ExportTableToPointInTimeWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_ExportTableToPointInTimeWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ExportTableToPointInTimeInput{
		S3Bucket: aws.String("dummy"),
		TableArn: aws.String("arn:aws:dynamodb:dummy:1234567890:table/" + testDynamodbTableName),
	}
	if _, err := db.ExportTableToPointInTimeWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support ExportTableToPointInTime operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbExportTableToPIT, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_GetItem(t *testing.T) {
	testName := "TestDynamoDbProxy_GetItem"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.GetItemInput{
		Key:                    awsDynamodbMakeKey(bson.M{"username": "btnguyen2k", "email": "me@domain.com"}),
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:              aws.String(testDynamodbTableName),
	}
	if _, err := db.GetItem(input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbGetItem, nil, MetricsCatAll, MetricsCatDQL)
}

func TestDynamoDbProxy_GetItemWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_GetItemWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.GetItemInput{
		Key:                    awsDynamodbMakeKey(bson.M{"username": "btnguyen2k", "email": "me@domain.com"}),
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:              aws.String(testDynamodbTableName),
	}
	if _, err := db.GetItemWithContext(context.TODO(), input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbGetItem, nil, MetricsCatAll, MetricsCatDQL)
}

func TestDynamoDbProxy_ListBackups(t *testing.T) {
	testName := "TestDynamoDbProxy_ListBackups"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ListBackupsInput{
		TableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.ListBackups(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support ListBackups operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbListBackups, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ListBackupsWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_ListBackupsWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ListBackupsInput{
		TableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.ListBackupsWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support ListBackups operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbListBackups, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ListContributorInsights(t *testing.T) {
	testName := "TestDynamoDbProxy_ListContributorInsights"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ListContributorInsightsInput{
		TableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.ListContributorInsights(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support ListContributorInsights operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbListContributorInsights, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ListContributorInsightsWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_ListContributorInsightsWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ListContributorInsightsInput{
		TableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.ListContributorInsightsWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support ListContributorInsights operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbListContributorInsights, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ListContributorInsightsPages(t *testing.T) {
	testName := "TestDynamoDbProxy_ListContributorInsightsPages"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ListContributorInsightsInput{
		TableName: aws.String(testDynamodbTableName),
	}
	if err := db.ListContributorInsightsPages(input, func(output *dynamodb.ListContributorInsightsOutput, b bool) bool {
		return true
	}); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support ListContributorInsights operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbListContributorInsights, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ListContributorInsightsPagesWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_ListContributorInsightsPagesWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ListContributorInsightsInput{
		TableName: aws.String(testDynamodbTableName),
	}
	if err := db.ListContributorInsightsPagesWithContext(context.TODO(), input, func(output *dynamodb.ListContributorInsightsOutput, b bool) bool {
		return true
	}); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support ListContributorInsights operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbListContributorInsights, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ListExports(t *testing.T) {
	testName := "TestDynamoDbProxy_ListExports"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ListExportsInput{
		TableArn: aws.String("arn:aws:dynamodb:dummy:1234567890:table/" + testDynamodbTableName),
	}
	if _, err := db.ListExports(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support ListExports operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbListExports, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ListExportsWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_ListExportsWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ListExportsInput{
		TableArn: aws.String("arn:aws:dynamodb:dummy:1234567890:table/" + testDynamodbTableName),
	}
	if _, err := db.ListExportsWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support ListExports operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbListExports, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ListExportsPages(t *testing.T) {
	testName := "TestDynamoDbProxy_ListExportsPages"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ListExportsInput{
		TableArn: aws.String("arn:aws:dynamodb:dummy:1234567890:table/" + testDynamodbTableName),
	}
	if err := db.ListExportsPages(input, func(output *dynamodb.ListExportsOutput, b bool) bool {
		return true
	}); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support ListExports operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbListExports, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ListExportsPagesWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_ListExportsPagesWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ListExportsInput{
		TableArn: aws.String("arn:aws:dynamodb:dummy:1234567890:table/" + testDynamodbTableName),
	}
	if err := db.ListExportsPagesWithContext(context.TODO(), input, func(output *dynamodb.ListExportsOutput, b bool) bool {
		return true
	}); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support ListExports operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbListExports, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ListGlobalTables(t *testing.T) {
	testName := "TestDynamoDbProxy_ListGlobalTables"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ListGlobalTablesInput{}
	if _, err := db.ListGlobalTables(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support ListGlobalTables operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbListGlobalTables, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ListGlobalTablesWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_ListGlobalTablesWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ListGlobalTablesInput{}
	if _, err := db.ListGlobalTablesWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support ListGlobalTables operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbListGlobalTables, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ListTables(t *testing.T) {
	testName := "TestDynamoDbProxy_ListTables"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ListTablesInput{}
	if _, err := db.ListTables(input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbListTables, nil, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ListTablesWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_ListTablesWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ListTablesInput{}
	if _, err := db.ListTablesWithContext(context.TODO(), input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbListTables, nil, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ListTablesPages(t *testing.T) {
	testName := "TestDynamoDbProxy_ListTablesPages"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ListTablesInput{}
	if err := db.ListTablesPages(input, func(output *dynamodb.ListTablesOutput, b bool) bool {
		return true
	}); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbListTables, nil, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ListTablesPagesWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_ListTablesPagesWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ListTablesInput{}
	if err := db.ListTablesPagesWithContext(context.TODO(), input, func(output *dynamodb.ListTablesOutput, b bool) bool {
		return true
	}); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbListTables, nil, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ListTagsOfResource(t *testing.T) {
	testName := "TestDynamoDbProxy_ListTagsOfResource"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ListTagsOfResourceInput{
		ResourceArn: aws.String("arn:aws:dynamodb:dummy:1234567890:table/" + testDynamodbTableName),
	}
	if _, err := db.ListTagsOfResource(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support ListTagsOfResource operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbListTagsOfResource, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_ListTagsOfResourceWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_ListTagsOfResourceWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ListTagsOfResourceInput{
		ResourceArn: aws.String("arn:aws:dynamodb:dummy:1234567890:table/" + testDynamodbTableName),
	}
	if _, err := db.ListTagsOfResourceWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support ListTagsOfResource operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbListTagsOfResource, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_PutItem(t *testing.T) {
	testName := "TestDynamoDbProxy_PutItem"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	item, _ := dynamodbattribute.MarshalMap(_awsDynamodbTestItem)
	input := &dynamodb.PutItemInput{
		Item:                   item,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:              aws.String(testDynamodbTableName),
	}
	if _, err := db.PutItem(input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, nil, MetricsCatAll, MetricsCatDML)
}

func TestDynamoDbProxy_PutItemWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_PutItemWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	item, _ := dynamodbattribute.MarshalMap(_awsDynamodbTestItem)
	input := &dynamodb.PutItemInput{
		Item:                   item,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:              aws.String(testDynamodbTableName),
	}
	if _, err := db.PutItemWithContext(context.TODO(), input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbPutItem, nil, MetricsCatAll, MetricsCatDML)
}

func TestDynamoDbProxy_Query(t *testing.T) {
	testName := "TestDynamoDbProxy_Query"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	keyFilter := expression.Name("username").Equal(expression.Value("btnguyen2k"))
	builder := expression.NewBuilder().WithCondition(keyFilter)
	filterExp, _ := builder.Build()
	input := &dynamodb.QueryInput{
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:                 aws.String(testDynamodbTableName),
		ExpressionAttributeNames:  filterExp.Names(),
		ExpressionAttributeValues: filterExp.Values(),
		KeyConditionExpression:    filterExp.Condition(),
	}
	if _, err := db.Query(input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbQueryItems, nil, MetricsCatAll, MetricsCatDQL)
}

func TestDynamoDbProxy_QueryWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_QueryWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	keyFilter := expression.Name("username").Equal(expression.Value("btnguyen2k"))
	builder := expression.NewBuilder().WithCondition(keyFilter)
	filterExp, _ := builder.Build()
	input := &dynamodb.QueryInput{
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:                 aws.String(testDynamodbTableName),
		ExpressionAttributeNames:  filterExp.Names(),
		ExpressionAttributeValues: filterExp.Values(),
		KeyConditionExpression:    filterExp.Condition(),
	}
	if _, err := db.QueryWithContext(context.TODO(), input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbQueryItems, nil, MetricsCatAll, MetricsCatDQL)
}

func TestDynamoDbProxy_QueryPages(t *testing.T) {
	testName := "TestDynamoDbProxy_QueryPages"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	keyFilter := expression.Name("username").Equal(expression.Value("btnguyen2k"))
	builder := expression.NewBuilder().WithCondition(keyFilter)
	filterExp, _ := builder.Build()
	input := &dynamodb.QueryInput{
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:                 aws.String(testDynamodbTableName),
		ExpressionAttributeNames:  filterExp.Names(),
		ExpressionAttributeValues: filterExp.Values(),
		KeyConditionExpression:    filterExp.Condition(),
	}
	if err := db.QueryPages(input, func(output *dynamodb.QueryOutput, b bool) bool {
		return true
	}); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbQueryItems, nil, MetricsCatAll, MetricsCatDQL)
}

func TestDynamoDbProxy_QueryPagesWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_QueryPagesWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	keyFilter := expression.Name("username").Equal(expression.Value("btnguyen2k"))
	builder := expression.NewBuilder().WithCondition(keyFilter)
	filterExp, _ := builder.Build()
	input := &dynamodb.QueryInput{
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:                 aws.String(testDynamodbTableName),
		ExpressionAttributeNames:  filterExp.Names(),
		ExpressionAttributeValues: filterExp.Values(),
		KeyConditionExpression:    filterExp.Condition(),
	}
	if err := db.QueryPagesWithContext(context.TODO(), input, func(output *dynamodb.QueryOutput, b bool) bool {
		return true
	}); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbQueryItems, nil, MetricsCatAll, MetricsCatDQL)
}

func TestDynamoDbProxy_RestoreTableFromBackup(t *testing.T) {
	testName := "TestDynamoDbProxy_RestoreTableFromBackup"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.RestoreTableFromBackupInput{
		BackupArn:       aws.String("arn:aws:dynamodb:dummy:1234567890:table/backup_" + testDynamodbTableName),
		TargetTableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.RestoreTableFromBackup(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support RestoreTableFromBackup operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbRestoreTableFromBackup, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_RestoreTableFromBackupWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_RestoreTableFromBackupWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.RestoreTableFromBackupInput{
		BackupArn:       aws.String("arn:aws:dynamodb:dummy:1234567890:table/backup_" + testDynamodbTableName),
		TargetTableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.RestoreTableFromBackupWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support RestoreTableFromBackup operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbRestoreTableFromBackup, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_RestoreTableToPointInTime(t *testing.T) {
	testName := "TestDynamoDbProxy_RestoreTableToPointInTime"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.RestoreTableToPointInTimeInput{
		TargetTableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.RestoreTableToPointInTime(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support RestoreTableToPointInTime operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbRestoreTableToPIT, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_RestoreTableToPointInTimeWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_RestoreTableToPointInTimeWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.RestoreTableToPointInTimeInput{
		TargetTableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.RestoreTableToPointInTimeWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support RestoreTableToPointInTime operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbRestoreTableToPIT, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_Scan(t *testing.T) {
	testName := "TestDynamoDbProxy_Scan"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ScanInput{
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:              aws.String(testDynamodbTableName),
	}
	if _, err := db.Scan(input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbScanItems, nil, MetricsCatAll, MetricsCatDQL)
}

func TestDynamoDbProxy_ScanWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_ScanWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ScanInput{
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:              aws.String(testDynamodbTableName),
	}
	if _, err := db.ScanWithContext(context.TODO(), input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbScanItems, nil, MetricsCatAll, MetricsCatDQL)
}

func TestDynamoDbProxy_ScanPages(t *testing.T) {
	testName := "TestDynamoDbProxy_ScanPages"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ScanInput{
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:              aws.String(testDynamodbTableName),
	}
	if err := db.ScanPages(input, func(output *dynamodb.ScanOutput, b bool) bool {
		return true
	}); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbScanItems, nil, MetricsCatAll, MetricsCatDQL)
}

func TestDynamoDbProxy_ScanPagesWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_ScanPagesWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.ScanInput{
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:              aws.String(testDynamodbTableName),
	}
	if err := db.ScanPagesWithContext(context.TODO(), input, func(output *dynamodb.ScanOutput, b bool) bool {
		return true
	}); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbScanItems, nil, MetricsCatAll, MetricsCatDQL)
}

func TestDynamoDbProxy_TagResource(t *testing.T) {
	testName := "TestDynamoDbProxy_TagResource"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.TagResourceInput{
		ResourceArn: aws.String("arn:aws:dynamodb:dummy:1234567890:table/" + testDynamodbTableName),
		Tags: []*dynamodb.Tag{
			{Key: aws.String("key"), Value: aws.String("value")},
		},
	}
	if _, err := db.TagResource(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support TagResource operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTagResource, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_TagResourceWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_TagResourceWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.TagResourceInput{
		ResourceArn: aws.String("arn:aws:dynamodb:dummy:1234567890:table/" + testDynamodbTableName),
		Tags: []*dynamodb.Tag{
			{Key: aws.String("key"), Value: aws.String("value")},
		},
	}
	if _, err := db.TagResourceWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support TagResource operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTagResource, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_TransactGetItems(t *testing.T) {
	testName := "TestDynamoDbProxy_TransactGetItems"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	tx, _ := adc.BuildTxGet(testDynamodbTableName, bson.M{"username": "btnguyen2k", "email": "me@domain.com"})
	input := &dynamodb.TransactGetItemsInput{
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TransactItems:          []*dynamodb.TransactGetItem{tx},
	}
	if _, err := db.TransactGetItems(input); err != nil {
		if AwsIgnoreTransactErrorIfMatched(err, dynamodb.ErrCodeConditionalCheckFailedException) != nil {
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTransactGetItems, []string{dynamodb.ErrCodeTransactionCanceledException}, MetricsCatAll, MetricsCatDQL)
}

func TestDynamoDbProxy_TransactGetItemsWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_TransactGetItemsWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	tx, _ := adc.BuildTxGet(testDynamodbTableName, bson.M{"username": "btnguyen2k", "email": "me@domain.com"})
	input := &dynamodb.TransactGetItemsInput{
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TransactItems:          []*dynamodb.TransactGetItem{tx},
	}
	if _, err := db.TransactGetItemsWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreTransactErrorIfMatched(err, dynamodb.ErrCodeConditionalCheckFailedException) != nil {
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTransactGetItems, []string{dynamodb.ErrCodeTransactionCanceledException}, MetricsCatAll, MetricsCatDQL)
}

func TestDynamoDbProxy_TransactWriteItems(t *testing.T) {
	testName := "TestDynamoDbProxy_TransactWriteItems"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	tx, _ := adc.BuildTxPut(testDynamodbTableName, bson.M{"username": "btnguyen2k", "email": "me@domain.com"}, nil)
	input := &dynamodb.TransactWriteItemsInput{
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TransactItems:          []*dynamodb.TransactWriteItem{tx},
	}
	if _, err := db.TransactWriteItems(input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTransactWriteItems, nil, MetricsCatAll, MetricsCatDML)
}

func TestDynamoDbProxy_TransactWriteItemsWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_TransactWriteItemsWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	tx, _ := adc.BuildTxPut(testDynamodbTableName, bson.M{"username": "btnguyen2k", "email": "me@domain.com"}, nil)
	input := &dynamodb.TransactWriteItemsInput{
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TransactItems:          []*dynamodb.TransactWriteItem{tx},
	}
	if _, err := db.TransactWriteItemsWithContext(context.TODO(), input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbTransactWriteItems, nil, MetricsCatAll, MetricsCatDML)
}

func TestDynamoDbProxy_UntagResource(t *testing.T) {
	testName := "TestDynamoDbProxy_UntagResource"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.UntagResourceInput{
		ResourceArn: aws.String("arn:aws:dynamodb:dummy:1234567890:table/" + testDynamodbTableName),
		TagKeys:     []*string{aws.String("Key")},
	}
	if _, err := db.UntagResource(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support UntagResource operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUntagResource, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_UntagResourceWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_UntagResourceWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.UntagResourceInput{
		ResourceArn: aws.String("arn:aws:dynamodb:dummy:1234567890:table/" + testDynamodbTableName),
		TagKeys:     []*string{aws.String("Key")},
	}
	if _, err := db.UntagResourceWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support UntagResource operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUntagResource, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_UpdateContinuousBackups(t *testing.T) {
	testName := "TestDynamoDbProxy_UpdateContinuousBackups"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.UpdateContinuousBackupsInput{
		TableName:                        aws.String(testDynamodbTableName),
		PointInTimeRecoverySpecification: &dynamodb.PointInTimeRecoverySpecification{PointInTimeRecoveryEnabled: aws.Bool(false)},
	}
	if _, err := db.UpdateContinuousBackups(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support UpdateContinuousBackups operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateContinuousBackups, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_UpdateContinuousBackupsWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_UpdateContinuousBackupsWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.UpdateContinuousBackupsInput{
		TableName:                        aws.String(testDynamodbTableName),
		PointInTimeRecoverySpecification: &dynamodb.PointInTimeRecoverySpecification{PointInTimeRecoveryEnabled: aws.Bool(false)},
	}
	if _, err := db.UpdateContinuousBackupsWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support UpdateContinuousBackups operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateContinuousBackups, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_UpdateContributorInsights(t *testing.T) {
	testName := "TestDynamoDbProxy_UpdateContributorInsights"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.UpdateContributorInsightsInput{
		ContributorInsightsAction: aws.String(""),
		TableName:                 aws.String(testDynamodbTableName),
	}
	if _, err := db.UpdateContributorInsights(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support UpdateContributorInsights operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateContributorInsights, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_UpdateContributorInsightsWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_UpdateContributorInsightsWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.UpdateContributorInsightsInput{
		ContributorInsightsAction: aws.String(""),
		TableName:                 aws.String(testDynamodbTableName),
	}
	if _, err := db.UpdateContributorInsightsWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support UpdateContributorInsights operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateContributorInsights, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_UpdateGlobalTable(t *testing.T) {
	testName := "TestDynamoDbProxy_UpdateGlobalTable"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.UpdateGlobalTableInput{
		GlobalTableName: aws.String(testDynamodbTableName),
		ReplicaUpdates: []*dynamodb.ReplicaUpdate{
			{Delete: &dynamodb.DeleteReplicaAction{RegionName: aws.String("dummy")}},
		},
	}
	if _, err := db.UpdateGlobalTable(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support UpdateGlobalTable operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateGlobalTable, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_UpdateGlobalTableWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_UpdateGlobalTableWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.UpdateGlobalTableInput{
		GlobalTableName: aws.String(testDynamodbTableName),
		ReplicaUpdates: []*dynamodb.ReplicaUpdate{
			{Delete: &dynamodb.DeleteReplicaAction{RegionName: aws.String("dummy")}},
		},
	}
	if _, err := db.UpdateGlobalTableWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support UpdateGlobalTable operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateGlobalTable, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_UpdateGlobalTableSettings(t *testing.T) {
	testName := "TestDynamoDbProxy_UpdateGlobalTableSettings"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.UpdateGlobalTableSettingsInput{
		GlobalTableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.UpdateGlobalTableSettings(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support UpdateGlobalTableSettings operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateGlobalTableSettings, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_UpdateGlobalTableSettingsWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_UpdateGlobalTableSettingsWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.UpdateGlobalTableSettingsInput{
		GlobalTableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.UpdateGlobalTableSettingsWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support UpdateGlobalTableSettings operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateGlobalTableSettings, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_UpdateItem(t *testing.T) {
	testName := "TestDynamoDbProxy_UpdateItem"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.UpdateItemInput{
		Key:                    awsDynamodbMakeKey(bson.M{"username": "btnguyen2k", "email": "me@domain.com"}),
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:              aws.String(testDynamodbTableName),
	}
	if _, err := db.UpdateItem(input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateItem, nil, MetricsCatAll, MetricsCatDML)
}

func TestDynamoDbProxy_UpdateItemWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_UpdateItemWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.UpdateItemInput{
		Key:                    awsDynamodbMakeKey(bson.M{"username": "btnguyen2k", "email": "me@domain.com"}),
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		TableName:              aws.String(testDynamodbTableName),
	}
	if _, err := db.UpdateItemWithContext(context.TODO(), input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateItem, nil, MetricsCatAll, MetricsCatDML)
}

func TestDynamoDbProxy_UpdateTable(t *testing.T) {
	testName := "TestDynamoDbProxy_UpdateTable"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.UpdateTableInput{
		TableName:             aws.String(testDynamodbTableName),
		ProvisionedThroughput: awsDynamodbToProvisionedThroughput(1, 1),
	}
	if _, err := db.UpdateTable(input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateTable, nil, MetricsCatAll, MetricsCatDDL)
}

func TestDynamoDbProxy_UpdateTableWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_UpdateTableWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.UpdateTableInput{
		TableName:             aws.String(testDynamodbTableName),
		ProvisionedThroughput: awsDynamodbToProvisionedThroughput(1, 1),
	}
	if _, err := db.UpdateTableWithContext(context.TODO(), input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateTable, nil, MetricsCatAll, MetricsCatDDL)
}

func TestDynamoDbProxy_UpdateTableReplicaAutoScaling(t *testing.T) {
	testName := "TestDynamoDbProxy_UpdateTableReplicaAutoScaling"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.UpdateTableReplicaAutoScalingInput{
		TableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.UpdateTableReplicaAutoScaling(input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support UpdateTableReplicaAutoScaling operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateTableReplicaAutoScaling, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_UpdateTableReplicaAutoScalingWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_UpdateTableReplicaAutoScalingWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.UpdateTableReplicaAutoScalingInput{
		TableName: aws.String(testDynamodbTableName),
	}
	if _, err := db.UpdateTableReplicaAutoScalingWithContext(context.TODO(), input); err != nil {
		if AwsIgnoreErrorIfMatched(err, "UnknownOperationException") != nil {
			// 	TODO: the Docker version does not support UpdateTableReplicaAutoScaling operation
			t.Fatalf("%s failed: %s", testName, err)
		}
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateTableReplicaAutoScaling, []string{"UnknownOperationException"}, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_UpdateTimeToLive(t *testing.T) {
	testName := "TestDynamoDbProxy_UpdateTimeToLive"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(testDynamodbTableName),
		TimeToLiveSpecification: &dynamodb.TimeToLiveSpecification{
			Enabled:       aws.Bool(true),
			AttributeName: aws.String("dummy"),
		},
	}
	if _, err := db.UpdateTimeToLive(input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateTTL, nil, MetricsCatAll, MetricsCatOther)
}

func TestDynamoDbProxy_UpdateTimeToLiveWithContext(t *testing.T) {
	testName := "TestDynamoDbProxy_UpdateTimeToLiveWithContext"
	adc := _createAwsDynamodbConnect(t, testName)
	defer adc.Close()
	_adcPrepareTestTable(func(msg string) { t.Fatalf(msg) }, testName, adc)

	db := adc.GetDbProxy()
	input := &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(testDynamodbTableName),
		TimeToLiveSpecification: &dynamodb.TimeToLiveSpecification{
			Enabled:       aws.Bool(true),
			AttributeName: aws.String("dummy"),
		},
	}
	if _, err := db.UpdateTimeToLiveWithContext(context.TODO(), input); err != nil {
		t.Fatalf("%s failed: %s", testName, err)
	}
	_adcVerifyLastCommand(func(msg string) { t.Fatalf(msg) }, testName, adc, cmdDynamodbUpdateTTL, nil, MetricsCatAll, MetricsCatOther)
}
