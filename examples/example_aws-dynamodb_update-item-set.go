package main

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/btnguyen2k/prom"
	"math/rand"
	"time"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	adc := createAwsDynamodbConnect("ap-southeast-1")
	defer adc.Close()

	item := map[string]*dynamodb.AttributeValue{
		"username": {S: aws.String("user-update")},
		"email":    {S: aws.String("user-update@test.com")},
		"an": {
			NS: []*string{aws.String("1"), aws.String("2"), aws.String("3")},
		},
		"as": {
			SS: []*string{aws.String("1"), aws.String("2"), aws.String("3")},
		},
		"m": {M: map[string]*dynamodb.AttributeValue{}},
		"a": {L: []*dynamodb.AttributeValue{}},
	}
	keyFilter := map[string]interface{}{"username": "user-update"}
	key := make(map[string]*dynamodb.AttributeValue)
	for k, v := range keyFilter {
		key[k] = prom.AwsDynamodbToAttributeValue(v)
	}

	{
		fmt.Println("-== Add Values to Item Attributes ==-")
		fmt.Printf("  Inserting item: %s\n", toJsonDynamodb(item))
		_, err := adc.PutItemRaw(nil, "test1", item, nil)
		if err != nil {
			fmt.Printf("    Error: %e\n", err)
		}
		attrsAndValues := map[string]interface{}{"an": 8, "as": []string{"9", "10"}}
		condition := expression.Name("username").Equal(expression.Value("user-update"))
		dbresult, err := adc.AddValuesToSet(nil, "test1", keyFilter, &condition, attrsAndValues)
		fmt.Printf("  Adding values to attributes: %v - %e\n", dbresult, err)
		dbitem, err := adc.GetItem(nil, "test1", keyFilter)
		if err != nil {
			fmt.Printf("    Error getting item: %e\n", err)
		} else {
			fmt.Printf("    Item after update: %s\n", toJsonDynamodb(dbitem))
		}
		fmt.Println(awsDynamodbSep)
	}
	{
		fmt.Println("-== Delete Values from Item Attributes ==-")
		fmt.Printf("  Inserting item: %s\n", toJsonDynamodb(item))
		_, err := adc.PutItemRaw(nil, "test1", item, nil)
		if err != nil {
			fmt.Printf("    Error: %e\n", err)
		}
		attrsAndValues := map[string]interface{}{"an": []int{1, 2}, "as": []string{"3"}}
		condition := expression.Name("username").Equal(expression.Value("user-update"))
		dbresult, err := adc.DeleteValuesFromAttributes(nil, "test1", keyFilter, &condition, attrsAndValues)
		fmt.Printf("  Deleting values from attributes: %v - %e\n", dbresult, err)
		dbitem, err := adc.GetItem(nil, "test1", keyFilter)
		if err != nil {
			fmt.Printf("    Error getting item: %e\n", err)
		} else {
			fmt.Printf("    Item after update: %s\n", toJsonDynamodb(dbitem))
		}
		fmt.Println(awsDynamodbSep)
	}
}
