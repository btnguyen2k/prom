package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/btnguyen2k/prom"
)

func awsDynamodbGetItem(adc *prom.AwsDynamodbConnect, table string, filter map[string]interface{}) {
	fmt.Printf("  Loading an item from table [%s] with filter: %v\n", table, filter)
	result, err := adc.GetItem(nil, table, filter)
	if err != nil {
		fmt.Printf("    Error: %s\n", err)
	} else if result == nil {
		fmt.Printf("    Item not found with filter: %v\n", filter)
	} else {
		fmt.Printf("    Item: %s\n", toJsonDynamodb(result))
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	adc := createAwsDynamodbConnect("ap-southeast-1")
	defer adc.Close()

	fmt.Println("-== Get Single Item from Table ==-")
	awsDynamodbGetItem(adc, "test1", map[string]interface{}{"username": "user-0"})
	awsDynamodbGetItem(adc, "test1", map[string]interface{}{"username": "user-not-exist"})
	awsDynamodbGetItem(adc, "test1", map[string]interface{}{"email": "email-0@test.com"})
	for i := 0; i < awsDynamodbRandomRange; i++ {
		awsDynamodbGetItem(adc, "test1", map[string]interface{}{"username": "user-2", "email": "email-" + strconv.Itoa(i) + "@test.com"})
	}
	fmt.Println(awsDynamodbSep)

	fmt.Println("-== Get Single Item from Table ==-")
	awsDynamodbGetItem(adc, "test2", map[string]interface{}{"username": "user-0"})
	awsDynamodbGetItem(adc, "test2", map[string]interface{}{"username": "user-not-exist"})
	awsDynamodbGetItem(adc, "test2", map[string]interface{}{"email": "email-0@test.com"})
	for i := 0; i < awsDynamodbRandomRange; i++ {
		awsDynamodbGetItem(adc, "test2", map[string]interface{}{"username": "user-2", "email": "email-" + strconv.Itoa(i) + "@test.com"})
	}
	fmt.Println(awsDynamodbSep)
}
