package main

import (
	"fmt"
	"github.com/btnguyen2k/prom"
	"math/rand"
	"time"
)

func awsDynamodbCreateIndexAndWait(adc *prom.AwsDynamodbConnect, table, index string) {
	err := adc.CreateGlobalSecondaryIndex(nil, table, index, 1, 1,
		[]prom.AwsDynamodbNameAndType{{"email", "S"}},
		[]prom.AwsDynamodbNameAndType{{"email", "HASH"}})
	fmt.Printf("  Create GSI [%s] on table [%s]: %e\n", index, table, err)
	time.Sleep(1 * time.Second)
	for status, err := adc.GetGlobalSecondaryIndexStatus(nil, table, index); status != "ACTIVE" && err == nil; {
		fmt.Printf("    GSI [%s] on table [%s] status: %v - %e\n", index, table, status, err)
		time.Sleep(1 * time.Second)
		status, err = adc.GetGlobalSecondaryIndexStatus(nil, table, index)
	}
}

func awsDynamodbCreateTableAndWait(adc *prom.AwsDynamodbConnect, table string, schema, key []prom.AwsDynamodbNameAndType) {
	err := adc.CreateTable(nil, table, 2, 2, schema, key)
	fmt.Printf("  Create table [%s]: %e\n", table, err)
	if err == nil {
		tables, err := adc.ListTables(nil)
		if err != nil {
			fmt.Printf("    Error: %e\n", err)
		} else {
			fmt.Printf("    Tables: %v\n", tables)
		}
		ok, err := adc.HasTable(nil, table)
		if err != nil {
			fmt.Printf("    Error: %e\n", err)
		} else {
			fmt.Printf("    HasTable[%s]: %v\n", table, ok)
		}
	}
	time.Sleep(1 * time.Second)
	for status, err := adc.GetTableStatus(nil, table); status != "ACTIVE" && err == nil; {
		fmt.Printf("    Table [%s] status: %v - %e\n", table, status, err)
		time.Sleep(1 * time.Second)
		status, err = adc.GetTableStatus(nil, table)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	adc := createAwsDynamodbConnect("ap-southeast-1")
	defer adc.Close()

	fmt.Println("-== Create Table and GSI ==-")
	awsDynamodbCreateTableAndWait(adc, "test1",
		[]prom.AwsDynamodbNameAndType{{"username", "S"}},
		[]prom.AwsDynamodbNameAndType{{"username", "HASH"}})
	awsDynamodbCreateIndexAndWait(adc, "test1", awsDynamodbIndexName)
	awsDynamodbCreateTableAndWait(adc, "test2",
		[]prom.AwsDynamodbNameAndType{{"username", "S"}, {"email", "S"}},
		[]prom.AwsDynamodbNameAndType{{"username", "HASH"}, {"email", "RANGE"}})
	awsDynamodbCreateIndexAndWait(adc, "test2", awsDynamodbIndexName)
}
