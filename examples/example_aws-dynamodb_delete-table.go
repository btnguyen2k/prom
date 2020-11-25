package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/btnguyen2k/prom"
)

func awsDynamodbDeleteIndexAndWait(adc *prom.AwsDynamodbConnect, table, index string) {
	err := adc.DeleteGlobalSecondaryIndex(nil, table, index)
	fmt.Printf("  Drop GSI [%s] on table [%s]: %s\n", table, index, err)
	time.Sleep(1 * time.Second)
	for status, err := adc.GetGlobalSecondaryIndexStatus(nil, table, index); status != "" && err == nil; {
		fmt.Printf("    GSI [%s] on table [%s] status: %v - %s\n", table, index, status, err)
		time.Sleep(1 * time.Second)
		status, err = adc.GetGlobalSecondaryIndexStatus(nil, table, index)
	}
}

func awsDynamodbDeleteTableAndWait(adc *prom.AwsDynamodbConnect, table string) {
	err := adc.DeleteTable(nil, table)
	fmt.Printf("  Drop table [%s]: %s\n", table, err)
	time.Sleep(1 * time.Second)
	for status, err := adc.GetTableStatus(nil, table); status != "" && err == nil; {
		fmt.Printf("    Table [%s] status: %v - %s\n", table, status, err)
		time.Sleep(1 * time.Second)
		status, err = adc.GetTableStatus(nil, table)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	adc := createAwsDynamodbConnect("ap-southeast-1")
	defer adc.Close()

	fmt.Println("-== Delete Table and GSI ==-")
	awsDynamodbDeleteIndexAndWait(adc, "test1", awsDynamodbIndexName)
	awsDynamodbDeleteTableAndWait(adc, "test1")
	awsDynamodbDeleteIndexAndWait(adc, "test2", awsDynamodbIndexName)
	awsDynamodbDeleteTableAndWait(adc, "test2")
}
