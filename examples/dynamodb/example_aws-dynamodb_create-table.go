// go run example_aws-dynamodb_base.go example_aws-dynamodb_create-table.go
package main

import (
	"fmt"
	"time"

	awsdynamodb "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/btnguyen2k/prom/dynamodb"
)

func awsDynamodbCreateIndexAndWait(adc *dynamodb.AwsDynamodbConnect, table, index string) {
	err := adc.CreateGlobalSecondaryIndex(nil, table, index, 1, 1,
		[]dynamodb.AwsDynamodbNameAndType{{"email", "S"}},
		[]dynamodb.AwsDynamodbNameAndType{{"email", "HASH"}})
	fmt.Printf("  Create GSI [%s] on table [%s]: %s\n", index, table, err)
	sleepDuration := 1 * time.Second
	time.Sleep(sleepDuration)
	for status, err := adc.GetGlobalSecondaryIndexStatus(nil, table, index); status != "ACTIVE" && err == nil; {
		fmt.Printf("    GSI [%s] on table [%s] status: %v - %s\n", index, table, status, err)
		time.Sleep(sleepDuration)
		status, err = adc.GetGlobalSecondaryIndexStatus(nil, table, index)
	}
}

func awsDynamodbCreateTableAndWait(adc *dynamodb.AwsDynamodbConnect, table string, schema, key []dynamodb.AwsDynamodbNameAndType) {
	err := adc.CreateTable(nil, table, 2, 2, schema, key)
	fmt.Printf("  Create table [%s]: %s\n", table, err)
	if dynamodb.AwsIgnoreErrorIfMatched(err, awsdynamodb.ErrCodeResourceInUseException) == nil {
		if tables, err := adc.ListTables(nil); err != nil {
			fmt.Printf("    Error: %s\n", err)
		} else {
			fmt.Printf("    Tables: %v\n", tables)
		}
		if ok, err := adc.HasTable(nil, table); err != nil {
			fmt.Printf("    Error: %s\n", err)
		} else {
			fmt.Printf("    HasTable[%s]: %v\n", table, ok)
		}
	}
	sleepDuration := 1 * time.Second
	time.Sleep(sleepDuration)
	for status, err := adc.GetTableStatus(nil, table); status != "ACTIVE" && err == nil; {
		fmt.Printf("    Table [%s] status: %v - %s\n", table, status, err)
		time.Sleep(sleepDuration)
		status, err = adc.GetTableStatus(nil, table)
	}
}

func main() {
	adc := createAwsDynamodbConnect()
	defer adc.Close()

	fmt.Println("-== Create Table and GSI ==-")
	awsDynamodbCreateTableAndWait(adc, "test1",
		[]dynamodb.AwsDynamodbNameAndType{{"username", "S"}},
		[]dynamodb.AwsDynamodbNameAndType{{"username", "HASH"}})
	awsDynamodbCreateIndexAndWait(adc, "test1", awsDynamodbIndexName)
	awsDynamodbCreateTableAndWait(adc, "test2",
		[]dynamodb.AwsDynamodbNameAndType{{"username", "S"}, {"email", "S"}},
		[]dynamodb.AwsDynamodbNameAndType{{"username", "HASH"}, {"email", "RANGE"}})
	awsDynamodbCreateIndexAndWait(adc, "test2", awsDynamodbIndexName)
}
