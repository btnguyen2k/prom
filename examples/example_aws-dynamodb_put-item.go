package main

import (
	"fmt"
	"github.com/btnguyen2k/prom"
	"math/rand"
	"strconv"
	"time"
)

func awsDynamodbPutItem(adc *prom.AwsDynamodbConnect, table string, pkAttrs []string) {
	fmt.Println("-== Put Items to Table ==-")

	for i := 0; i < awsDynamodbNumItems; i++ {
		a := []interface{}{i%2 == 0, 1, "a"}
		m := map[string]interface{}{"b": i%2 == 0, "n": 2, "s": "m"}
		item := map[string]interface{}{
			"username": "user-" + strconv.Itoa(i),
			"email":    "email-" + strconv.Itoa(rand.Intn(awsDynamodbRandomRange)) + "@test.com",
			"t":        time.Unix(int64(rand.Int31()), rand.Int63()%1000000000),
			"b":        i%2 == 0,
			"i":        rand.Int31(),
			"f":        rand.ExpFloat64(),
			"m": map[string]interface{}{
				"s": strconv.Itoa(i),
				"n": i % 3,
				"t": time.Unix(int64(rand.Int31()), rand.Int63()%1000000000),
				"a": a,
				"m": m,
			},
			"a": []interface{}{strconv.Itoa(i), i * 2, float32(i) * 3.4, time.Unix(int64(rand.Int31()), rand.Int63()%1000000000), a, m},
		}
		switch i % 4 {
		case 0:
			delete(item, "b")
		case 1:
			delete(item, "i")
		case 2:
			delete(item, "f")
		}
		fmt.Printf("  Inserting item to table [%s]: %s\n", table, toJsonDynamodb(item))
		_, err := adc.PutItem(nil, table, item, nil)
		if err != nil {
			fmt.Printf("    Error: %e\n", err)
		}
	}

	// insert a duplicated PK item
	item := map[string]interface{}{
		"username": "user-" + strconv.Itoa(0),
		"email":    "email-" + strconv.Itoa(0) + "@test.com",
		"t":        time.Unix(int64(rand.Int31()), rand.Int63()%1000000000),
	}
	for i := 0; i < awsDynamodbRandomRange; i++ {
		item["email"] = "email-" + strconv.Itoa(i) + "@test.com"
		item["t"] = time.Unix(int64(rand.Int31()), rand.Int63()%1000000000)
		fmt.Printf("  Inserting item: %s\n", toJsonDynamodb(item))
		result, err := adc.PutItemIfNotExist(nil, table, item, pkAttrs)
		if err != nil {
			fmt.Printf("    Error: %e\n", err)
		} else {
			fmt.Printf("    Insert document: %v\n", result)
		}
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	adc := createAwsDynamodbConnect("ap-southeast-1")
	defer adc.Close()

	awsDynamodbPutItem(adc, "test1", []string{"username"})
	awsDynamodbPutItem(adc, "test2", []string{"username", "email"})
}
