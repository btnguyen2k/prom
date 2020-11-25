package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb/expression"

	"github.com/btnguyen2k/prom"
)

func awsDynamodbScanItems(adc *prom.AwsDynamodbConnect, table string, filter *expression.ConditionBuilder, indexName string) {
	if indexName == "" {
		fmt.Printf("  Scanning items from table [%s] with filter: %v\n", table, *filter)
	} else {
		fmt.Printf("  Scanning items from table [%s] on index [%s] with filter: %v\n", table, indexName, *filter)
	}

	result, err := adc.ScanItems(nil, table, filter, indexName)
	if err != nil {
		fmt.Printf("    Error: %s\n", err)
	} else {
		fmt.Printf("    Items:\n")
		for _, item := range result {
			fmt.Println("      ", toJsonDynamodb(item))
		}
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	adc := createAwsDynamodbConnect("ap-southeast-1")
	defer adc.Close()

	var filter expression.ConditionBuilder

	fmt.Println("-== Scan Items from Table ==-")

	filter = expression.Name("username").Equal(expression.Value("user-0"))
	awsDynamodbScanItems(adc, "test1", &filter, "")
	awsDynamodbScanItems(adc, "test1", &filter, awsDynamodbIndexName)

	filter = expression.Name("email").Equal(expression.Value("email-0@test.com"))
	awsDynamodbScanItems(adc, "test2", &filter, "")
	awsDynamodbScanItems(adc, "test2", &filter, awsDynamodbIndexName)

	filter = expression.Name("b").Equal(expression.Value(true))
	awsDynamodbScanItems(adc, "test1", &filter, "")
	awsDynamodbScanItems(adc, "test1", &filter, awsDynamodbIndexName)

	filter = expression.Name("b").NotEqual(expression.Value(true))
	awsDynamodbScanItems(adc, "test2", &filter, "")
	awsDynamodbScanItems(adc, "test2", &filter, awsDynamodbIndexName)

	filter = expression.Name("m.m.b").Equal(expression.Value(true))
	awsDynamodbScanItems(adc, "test1", &filter, "")
	awsDynamodbScanItems(adc, "test1", &filter, awsDynamodbIndexName)

	filter = expression.Name("m.m.b").Equal(expression.Value(false))
	awsDynamodbScanItems(adc, "test2", &filter, "")
	awsDynamodbScanItems(adc, "test2", &filter, awsDynamodbIndexName)

	fmt.Println(awsDynamodbSep)
}
