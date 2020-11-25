package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"

	"github.com/btnguyen2k/prom"
)

const (
	_orderTable1 = "test_order_1"
	_orderTable2 = "test_order_2"
	_orderTable3 = "test_order_3"
)

func _orderCreateTables(adc *prom.AwsDynamodbConnect) {
	var rcu int64 = 2
	var wcu int64 = 4

	var table string
	table = _orderTable1
	if ok, err := adc.HasTable(nil, table); err != nil {
		panic(err)
	} else if !ok {
		var attrDefs, pkDefs []prom.AwsDynamodbNameAndType
		attrDefs = []prom.AwsDynamodbNameAndType{
			{Name: "id", Type: prom.AwsAttrTypeString},
		}
		pkDefs = []prom.AwsDynamodbNameAndType{
			{Name: "id", Type: prom.AwsKeyTypePartition},
		}
		if err := adc.CreateTable(nil, table, rcu, wcu, attrDefs, pkDefs); err != nil {
			panic(err)
		}
		for status, err := adc.GetTableStatus(nil, table); status != "ACTIVE" && err == nil; {
			fmt.Printf("Table [%s] status: %v - %s\n", table, status, err)
			time.Sleep(1 * time.Second)
			status, err = adc.GetTableStatus(nil, table)
		}
	} else {
		// delete all items
		pkAttrs := []string{"id"}
		adc.ScanItemsWithCallback(nil, table, nil, prom.AwsDynamodbNoIndex, nil, func(item prom.AwsDynamodbItem, lastEvaluatedKey map[string]*dynamodb.AttributeValue) (b bool, e error) {
			keyFilter := make(map[string]interface{})
			for _, v := range pkAttrs {
				keyFilter[v] = item[v]
			}
			_, err := adc.DeleteItem(nil, table, keyFilter, nil)
			fmt.Printf("Delete item from table [%s] with key %s: %s\n", table, keyFilter, err)
			return true, nil
		})
	}

	table = _orderTable2
	if ok, err := adc.HasTable(nil, table); err != nil {
		panic(err)
	} else if !ok {
		var attrDefs, pkDefs []prom.AwsDynamodbNameAndType
		attrDefs = []prom.AwsDynamodbNameAndType{
			{Name: "id", Type: prom.AwsAttrTypeString},
			{Name: "username", Type: prom.AwsAttrTypeString},
		}
		pkDefs = []prom.AwsDynamodbNameAndType{
			{Name: "id", Type: prom.AwsKeyTypePartition},
			{Name: "username", Type: prom.AwsKeyTypeSort},
		}
		if err := adc.CreateTable(nil, table, rcu, wcu, attrDefs, pkDefs); err != nil {
			panic(err)
		}
		for status, err := adc.GetTableStatus(nil, table); status != "ACTIVE" && err == nil; {
			fmt.Printf("Table [%s] status: %v - %s\n", table, status, err)
			time.Sleep(1 * time.Second)
			status, err = adc.GetTableStatus(nil, table)
		}
	} else {
		// delete all items
		pkAttrs := []string{"id", "username"}
		adc.ScanItemsWithCallback(nil, table, nil, prom.AwsDynamodbNoIndex, nil, func(item prom.AwsDynamodbItem, lastEvaluatedKey map[string]*dynamodb.AttributeValue) (b bool, e error) {
			keyFilter := make(map[string]interface{})
			for _, v := range pkAttrs {
				keyFilter[v] = item[v]
			}
			_, err := adc.DeleteItem(nil, table, keyFilter, nil)
			fmt.Printf("Delete item from table [%s] with key %s: %s\n", table, keyFilter, err)
			return true, nil
		})
	}

	table = _orderTable3
	if ok, err := adc.HasTable(nil, table); err != nil {
		panic(err)
	} else if !ok {
		var attrDefs, pkDefs []prom.AwsDynamodbNameAndType
		attrDefs = []prom.AwsDynamodbNameAndType{
			{Name: "id", Type: prom.AwsAttrTypeString},
		}
		pkDefs = []prom.AwsDynamodbNameAndType{
			{Name: "id", Type: prom.AwsKeyTypePartition},
		}
		if err := adc.CreateTable(nil, table, rcu, wcu, attrDefs, pkDefs); err != nil {
			panic(err)
		}
		for status, err := adc.GetTableStatus(nil, table); status != "ACTIVE" && err == nil; {
			fmt.Printf("Table [%s] status: %v - %s\n", table, status, err)
			time.Sleep(1 * time.Second)
			status, err = adc.GetTableStatus(nil, table)
		}
	} else {
		// delete all items
		pkAttrs := []string{"id"}
		adc.ScanItemsWithCallback(nil, table, nil, prom.AwsDynamodbNoIndex, nil, func(item prom.AwsDynamodbItem, lastEvaluatedKey map[string]*dynamodb.AttributeValue) (b bool, e error) {
			keyFilter := make(map[string]interface{})
			for _, v := range pkAttrs {
				keyFilter[v] = item[v]
			}
			_, err := adc.DeleteItem(nil, table, keyFilter, nil)
			fmt.Printf("Delete item from table [%s] with key %s: %s\n", table, keyFilter, err)
			return true, nil
		})
	}
	idxName := "idx_username"
	if status, _ := adc.GetGlobalSecondaryIndexStatus(nil, table, idxName); status == "" {
		var attrDefs, pkDefs []prom.AwsDynamodbNameAndType
		attrDefs = []prom.AwsDynamodbNameAndType{
			{Name: "id", Type: prom.AwsAttrTypeString},
			{Name: "username", Type: prom.AwsAttrTypeString},
		}
		pkDefs = []prom.AwsDynamodbNameAndType{
			{Name: "id", Type: prom.AwsKeyTypePartition},
			{Name: "username", Type: prom.AwsKeyTypeSort},
		}
		if err := adc.CreateGlobalSecondaryIndex(nil, table, idxName, 1, 1, attrDefs, pkDefs); err != nil {
			fmt.Println(err)
		} else {
			for status, err := adc.GetGlobalSecondaryIndexStatus(nil, table, idxName); status != "ACTIVE" && err == nil; {
				fmt.Printf("Index [%s/%s] status: %v - %s\n", table, idxName, status, err)
				time.Sleep(1 * time.Second)
				status, err = adc.GetGlobalSecondaryIndexStatus(nil, table, idxName)
			}
		}
	}
}

func _orderInitData(adc *prom.AwsDynamodbConnect, tableName string) {
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("%03d", rand.Int31n(1000))
		if tableName == _orderTable2 {
			id = "000"
		}
		username := fmt.Sprintf("%03d", rand.Int31n(1000))
		item := map[string]interface{}{"id": id, "username": username}
		fmt.Printf("Inserting item %v to table %s...\n", item, tableName)
		if _, err := adc.PutItem(nil, tableName, item, nil); err != nil {
			fmt.Println("    Error:", err)
		}
	}
}

func _orderLoadItems1(adc *prom.AwsDynamodbConnect) {
	tableName := _orderTable1
	indexName := ""
	fmt.Printf("Fetching items from table [%s] with index [%s]...\n", tableName, indexName)
	filter := expression.Name("username").GreaterThanEqual(expression.Value("123"))
	fmt.Println("    Filter:", filter)
	counter := 0
	err := adc.ScanItemsWithCallback(nil, tableName, &filter, indexName, nil, func(item prom.AwsDynamodbItem, lastEvaluatedKey map[string]*dynamodb.AttributeValue) (b bool, e error) {
		fmt.Printf("    %v\n", item)
		counter++
		return true, nil
	})
	fmt.Println("Finish:", counter, err)
}

func _orderLoadItems2(adc *prom.AwsDynamodbConnect) {
	tableName := _orderTable2
	indexName := ""
	fmt.Printf("Fetching items from table [%s] with index [%s]...\n", tableName, indexName)
	filter := expression.Name("id").Equal(expression.Value("000")).And(expression.Name("username").GreaterThanEqual(expression.Value("000")))
	fmt.Println("    Filter:", filter)
	counter := 0
	err := adc.QueryItemsWithCallback(nil, tableName, &filter, nil, indexName, nil, func(item prom.AwsDynamodbItem, lastEvaluatedKey map[string]*dynamodb.AttributeValue) (b bool, e error) {
		fmt.Printf("    %v\n", item)
		counter++
		return true, nil
	})
	fmt.Println("Finish:", counter, err)
}

func _orderLoadItems3(adc *prom.AwsDynamodbConnect) {
	tableName := _orderTable3
	indexName := "idx_username"
	fmt.Printf("Fetching items from table [%s] with index [%s]...\n", tableName, indexName)
	filter := expression.Name("id").Equal(expression.Value("038"))
	fmt.Println("    Filter:", filter)
	counter := 0
	err := adc.QueryItemsWithCallback(nil, tableName, &filter, nil, indexName, nil, func(item prom.AwsDynamodbItem, lastEvaluatedKey map[string]*dynamodb.AttributeValue) (b bool, e error) {
		fmt.Printf("    %v\n", item)
		counter++
		return true, nil
	})
	fmt.Println("Finish:", counter, err)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	adc := createAwsDynamodbConnect("ap-southeast-1")
	defer adc.Close()

	// _orderCreateTables(adc)
	// _orderInitData(adc, _orderTable1)
	// _orderInitData(adc, _orderTable2)
	// _orderInitData(adc, _orderTable3)

	// _orderLoadItems1(adc)
	// _orderLoadItems2(adc)
	_orderLoadItems3(adc)
}
