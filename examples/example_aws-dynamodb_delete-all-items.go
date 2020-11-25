package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/btnguyen2k/prom"
)

func awsDynamodbDeleteAll(adc *prom.AwsDynamodbConnect, table string, pkAttrs []string) {
	fmt.Println("-== Delete all Items from Table ==-")

	adc.ScanItemsWithCallback(nil, table, nil, prom.AwsDynamodbNoIndex, nil, func(item prom.AwsDynamodbItem, lastEvaluatedKey map[string]*dynamodb.AttributeValue) (b bool, e error) {
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
	rand.Seed(time.Now().UnixNano())
	adc := createAwsDynamodbConnect("ap-southeast-1")
	defer adc.Close()

	awsDynamodbDeleteAll(adc, "test1", []string{"username"})
	awsDynamodbDeleteAll(adc, "test2", []string{"username", "email"})
}
