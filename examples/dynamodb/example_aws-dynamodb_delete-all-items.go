// go run example_aws-dynamodb_base.go example_aws-dynamodb_delete-all-items.go
package main

import (
	"fmt"

	awsdynamodb "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/btnguyen2k/prom/dynamodb"
)

func awsDynamodbDeleteAll(adc *dynamodb.AwsDynamodbConnect, table string, pkAttrs []string) {
	fmt.Printf("-== Delete all items from table [%s]==-\n", table)

	adc.ScanItemsWithCallback(nil, table, nil, dynamodb.AwsDynamodbNoIndex, nil, func(item dynamodb.AwsDynamodbItem, lastEvaluatedKey map[string]*awsdynamodb.AttributeValue) (b bool, e error) {
		keyFilter := make(map[string]interface{})
		for _, v := range pkAttrs {
			keyFilter[v] = item[v]
		}
		result, err := adc.DeleteItem(nil, table, keyFilter, nil)
		fmt.Printf("    Delete item from table [%s] with key %s: %v - %s\n", table, toJsonDynamodb(keyFilter), result, err)
		return true, err
	})
}

func main() {
	adc := createAwsDynamodbConnect()
	defer adc.Close()

	awsDynamodbDeleteAll(adc, "test1", []string{"username"})
	awsDynamodbDeleteAll(adc, "test2", []string{"username", "email"})
}
