// go run example_aws-dynamodb_base.go example_aws-dynamodb_delete-table.go
package main

import (
	"fmt"
	"time"

	"github.com/btnguyen2k/prom/dynamodb"
)

func awsDynamodbDeleteIndexAndWait(adc *dynamodb.AwsDynamodbConnect, table, index string) {
	err := adc.DeleteGlobalSecondaryIndex(nil, table, index)
	fmt.Printf("  Drop GSI [%s] on table [%s]: %s\n", index, table, err)
	sleepDuration := 1 * time.Second
	time.Sleep(sleepDuration)
	for status, err := adc.GetGlobalSecondaryIndexStatus(nil, table, index); status != "" && err == nil; {
		fmt.Printf("    GSI [%s] on table [%s] status: %v - %s\n", table, index, status, err)
		time.Sleep(sleepDuration)
		status, err = adc.GetGlobalSecondaryIndexStatus(nil, table, index)
	}
}

func awsDynamodbDeleteTableAndWait(adc *dynamodb.AwsDynamodbConnect, table string) {
	err := adc.DeleteTable(nil, table)
	fmt.Printf("  Drop table [%s]: %s\n", table, err)
	sleepDuration := 1 * time.Second
	time.Sleep(sleepDuration)
	for status, err := adc.GetTableStatus(nil, table); status != "" && err == nil; {
		fmt.Printf("    Table [%s] status: %v - %s\n", table, status, err)
		time.Sleep(sleepDuration)
		status, err = adc.GetTableStatus(nil, table)
	}
}

func main() {
	adc := createAwsDynamodbConnect()
	defer adc.Close()

	fmt.Println("-== Delete Table and GSI ==-")
	awsDynamodbDeleteIndexAndWait(adc, "test1", awsDynamodbIndexName)
	awsDynamodbDeleteTableAndWait(adc, "test1")
	awsDynamodbDeleteIndexAndWait(adc, "test2", awsDynamodbIndexName)
	awsDynamodbDeleteTableAndWait(adc, "test2")
}
