// go run example_aws-dynamodb_base.go example_aws-dynamodb_update-item.go
package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/btnguyen2k/prom/dynamodb"
)

func awsDynamodbRemoveAttrs(adc *dynamodb.AwsDynamodbConnect, table string, data, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrs []string) {
	fmt.Printf("  Inserting item: %s\n", toJsonDynamodb(data))
	_, err := adc.PutItem(nil, table, data, nil)
	if err != nil {
		fmt.Printf("    Error: %s\n", err)
	}

	ok, err := adc.RemoveAttributes(nil, table, keyFilter, condition, attrs)
	if condition == nil {
		fmt.Printf("  Removing attributes %v from item %v: %v - %s\n", attrs, keyFilter, ok, err)
	} else {
		fmt.Printf("  Removing attributes %v from item %v (with condition %v): %v - %s\n", attrs, keyFilter, *condition, ok, err)
	}

	item, err := adc.GetItem(nil, table, keyFilter)
	if err != nil {
		fmt.Printf("    Error getting item: %s\n", err)
	} else {
		fmt.Printf("    Item after update: %s\n", toJsonDynamodb(item))
	}
}

func awsDynamodbSetAttrs(adc *dynamodb.AwsDynamodbConnect, table string, data, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsAndValues map[string]interface{}) {
	fmt.Printf("  Inserting item: %s\n", toJsonDynamodb(data))
	_, err := adc.PutItem(nil, table, data, nil)
	if err != nil {
		fmt.Printf("    Error: %s\n", err)
	}

	ok, err := adc.SetAttributes(nil, table, keyFilter, condition, attrsAndValues)
	if condition == nil {
		fmt.Printf("  Updating attributes %v from item %v: %v - %s\n", attrsAndValues, keyFilter, ok, err)
	} else {
		fmt.Printf("  Updating attributes %v from item %v (with condition %v): %v - %s\n", attrsAndValues, keyFilter, *condition, ok, err)
	}

	item, err := adc.GetItem(nil, table, keyFilter)
	if err != nil {
		fmt.Printf("    Error getting item: %s\n", err)
	} else {
		fmt.Printf("    Item after update: %s\n", toJsonDynamodb(item))
	}
}

func awsDynamodbAddValues(adc *dynamodb.AwsDynamodbConnect, table string, data, keyFilter map[string]interface{}, condition *expression.ConditionBuilder, attrsAndValues map[string]interface{}) {
	fmt.Printf("  Inserting item: %s\n", toJsonDynamodb(data))
	_, err := adc.PutItem(nil, table, data, nil)
	if err != nil {
		fmt.Printf("    Error: %s\n", err)
	}

	ok, err := adc.AddValuesToAttributes(nil, table, keyFilter, condition, attrsAndValues)
	if condition == nil {
		fmt.Printf("  Adding values to attributes %v from item %v: %v - %s\n", attrsAndValues, keyFilter, ok, err)
	} else {
		fmt.Printf("  Adding values to attributes %v from item %v (with condition %v): %v - %s\n", attrsAndValues, keyFilter, *condition, ok, err)
	}

	item, err := adc.GetItem(nil, table, keyFilter)
	if err != nil {
		fmt.Printf("    Error getting item: %s\n", err)
	} else {
		fmt.Printf("    Item after update: %s\n", toJsonDynamodb(item))
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	adc := createAwsDynamodbConnect()
	defer adc.Close()

	a := []interface{}{rand.Int()%2 == 0, 1, "a"}
	m := map[string]interface{}{"b": rand.Int()%2 == 0, "n": 2, "s": "m"}
	data := map[string]interface{}{
		"username": "user-update",
		"email":    "user-update@test.com",
		"b":        true,
		"s":        "a string",
		"n1":       1,
		"n2":       2.3,
		"m":        m,
		"a":        a,
		"an":       []int{1, 2, 3},
		"as":       []string{"1", "2", "3"},
	}
	keyFilter := map[string]interface{}{"username": "user-update"}
	var condition expression.ConditionBuilder

	fmt.Println("-== Remove Attributes from Item ==-")
	attrsToRemove := []string{"n2", "m.n", "a[0]"}
	awsDynamodbRemoveAttrs(adc, "test1", data, keyFilter, nil, attrsToRemove)
	condition = expression.Name("b").Equal(expression.Value(true))
	awsDynamodbRemoveAttrs(adc, "test1", data, keyFilter, &condition, attrsToRemove)
	condition = expression.Name("b").Equal(expression.Value(false))
	awsDynamodbRemoveAttrs(adc, "test1", data, keyFilter, &condition, attrsToRemove)
	fmt.Println(awsDynamodbSep)

	fmt.Println("-== Set Item Attributes ==-")
	attrsAndValuesToSet := map[string]interface{}{"n": 1, "n1": 0, "n2": nil, "m.n": 0, "m.n1": 1, "m.new": true, "a[0]": 1, "a[10]": 10}
	awsDynamodbSetAttrs(adc, "test1", data, keyFilter, nil, attrsAndValuesToSet)
	condition = expression.Name("b").Equal(expression.Value(true))
	awsDynamodbSetAttrs(adc, "test1", data, keyFilter, &condition, attrsAndValuesToSet)
	condition = expression.Name("b").Equal(expression.Value(false))
	awsDynamodbSetAttrs(adc, "test1", data, keyFilter, &condition, attrsAndValuesToSet)
	fmt.Println(awsDynamodbSep)

	fmt.Println("-== Add Values to Item Attributes ==-")
	// no easy way to work with "set" yet, see: https://github.com/aws/aws-sdk-go/issues/1990
	attrsAndValuesToAdd := map[string]interface{}{"n": 1, "n1": 2.3, "m.n": 1.2, "m.new": 3, "a[1]": 1.1, "a[10]": 10}
	awsDynamodbAddValues(adc, "test1", data, keyFilter, nil, attrsAndValuesToAdd)
	condition = expression.Name("b").Equal(expression.Value(true))
	awsDynamodbAddValues(adc, "test1", data, keyFilter, &condition, attrsAndValuesToAdd)
	condition = expression.Name("b").Equal(expression.Value(false))
	awsDynamodbAddValues(adc, "test1", data, keyFilter, &condition, attrsAndValuesToAdd)
	fmt.Println(awsDynamodbSep)
}
