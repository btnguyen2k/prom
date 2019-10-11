package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/btnguyen2k/prom"
	"math/rand"
	"time"
)

func awsDynamodbQueryItems(adc *prom.AwsDynamodbConnect, table string, keyFilter, nonKeyFilter *expression.ConditionBuilder, indexName string) {
	if indexName == "" {
		fmt.Printf("  Querying items from table [%s] with filter: %v/%v\n", table, keyFilter, nonKeyFilter)
	} else {
		fmt.Printf("  Querying items from table [%s] on index [%s] with filter: %v/%v\n", table, indexName, keyFilter, nonKeyFilter)
	}

	result, err := adc.QueryItems(nil, table, keyFilter, nonKeyFilter, indexName)
	if err != nil {
		fmt.Printf("    Error: %e\n", err)
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

	var keyFilter, nonKeyFilter expression.ConditionBuilder

	fmt.Println("-== Query Items from Table ==-")

	keyFilter = expression.Name("username").Equal(expression.Value("user-0"))
	awsDynamodbQueryItems(adc, "test1", &keyFilter, nil, "")
	awsDynamodbQueryItems(adc, "test1", &keyFilter, nil, awsDynamodbIndexName)
	awsDynamodbQueryItems(adc, "test2", &keyFilter, nil, "")
	awsDynamodbQueryItems(adc, "test2", &keyFilter, nil, awsDynamodbIndexName)

	keyFilter = expression.Name("email").Equal(expression.Value("email-0@test.com"))
	awsDynamodbQueryItems(adc, "test1", &keyFilter, nil, "")
	awsDynamodbQueryItems(adc, "test1", &keyFilter, nil, awsDynamodbIndexName)
	awsDynamodbQueryItems(adc, "test2", &keyFilter, nil, "")
	awsDynamodbQueryItems(adc, "test2", &keyFilter, nil, awsDynamodbIndexName)

	keyFilter = expression.Name("username").Equal(expression.Value("user-0")).And(expression.Name("email").Equal(expression.Value("email-0@test.com")))
	awsDynamodbQueryItems(adc, "test1", &keyFilter, nil, "")
	awsDynamodbQueryItems(adc, "test1", &keyFilter, nil, awsDynamodbIndexName)
	awsDynamodbQueryItems(adc, "test2", &keyFilter, nil, "")
	awsDynamodbQueryItems(adc, "test2", &keyFilter, nil, awsDynamodbIndexName)

	keyFilter = expression.Name("username").Equal(expression.Value("user-0"))
	nonKeyFilter = expression.Name("m.m.b").Equal(expression.Value(true))
	awsDynamodbQueryItems(adc, "test1", &keyFilter, &nonKeyFilter, "")
	awsDynamodbQueryItems(adc, "test2", &keyFilter, &nonKeyFilter, "")

	fmt.Println(awsDynamodbSep)
}
