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

func _txInitData(adc *prom.AwsDynamodbConnect, tableName string, accounts []string, initBalance int) {
	if ok, err := adc.HasTable(nil, tableName); err != nil {
		panic(err)
	} else if !ok {
		var attrDefs, pkDefs []prom.AwsDynamodbNameAndType
		attrDefs = []prom.AwsDynamodbNameAndType{
			{Name: "id", Type: prom.AwsAttrTypeString},
		}
		pkDefs = []prom.AwsDynamodbNameAndType{
			{Name: "id", Type: prom.AwsKeyTypePartition},
		}
		if err := adc.CreateTable(nil, tableName, 1, 1, attrDefs, pkDefs); err != nil {
			panic(err)
		}
		for status, err := adc.GetTableStatus(nil, tableName); status != "ACTIVE" && err == nil; {
			fmt.Printf("    Table [%s] status: %v - %e\n", tableName, status, err)
			time.Sleep(1 * time.Second)
			status, err = adc.GetTableStatus(nil, tableName)
		}
	} else {
		// delete all items
		pkAttrs := []string{"id"}
		adc.ScanItemsWithCallback(nil, tableName, nil, prom.AwsDynamodbNoIndex, nil, func(item prom.AwsDynamodbItem, lastEvaluatedKey map[string]*dynamodb.AttributeValue) (b bool, e error) {
			keyFilter := make(map[string]interface{})
			for _, v := range pkAttrs {
				keyFilter[v] = item[v]
			}
			_, err := adc.DeleteItem(nil, tableName, keyFilter, nil)
			fmt.Printf("    Delete item from table [%s] with key %s: %e\n", tableName, keyFilter, err)
			return true, nil
		})
	}

	for _, account := range accounts {
		if _, err := adc.PutItem(nil, tableName, map[string]interface{}{"id": account, "balance": initBalance}, nil); err != nil {
			panic(err)
		}
	}
}

func _txTransferMoney(adc *prom.AwsDynamodbConnect, tableName, from, to string, amount int) {
	txItems := make([]*dynamodb.TransactWriteItem, 0)
	conditionFrom := expression.Name("balance").GreaterThanEqual(expression.Value(amount))
	if txItem, err := adc.BuildTxAddValuesToAttributes(tableName, map[string]interface{}{"id": from}, &conditionFrom, map[string]interface{}{"balance": -amount}); err == nil && txItem != nil {
		txItems = append(txItems, txItem)
	} else {
		panic(err)
	}
	conditionTo := expression.Name("id").AttributeExists()
	if txItem, err := adc.BuildTxAddValuesToAttributes(tableName, map[string]interface{}{"id": to}, &conditionTo, map[string]interface{}{"balance": amount}); err == nil && txItem != nil {
		txItems = append(txItems, txItem)
	} else {
		panic(err)
	}
	fmt.Printf("Transferring [%d] from [%s] to [%s]...\n", amount, from, to)
	result, err := adc.ExecTxWriteItems(nil, &dynamodb.TransactWriteItemsInput{
		ReturnConsumedCapacity: aws.String("INDEXES"),
		TransactItems:          txItems,
	})
	fmt.Printf("%e\n%#v\n", err, err)
	fmt.Println(result)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	adc := createAwsDynamodbConnect("ap-southeast-1")
	defer adc.Close()

	tableName := "test_account"
	_txInitData(adc, tableName, []string{"btnguyen2k", "thanhnb"}, 1000)

	txItems := make([]*dynamodb.TransactWriteItem, 0)
	if txItem, err := adc.BuildTxPutIfNotExist(tableName, map[string]interface{}{"id": "btnguyen2k", "balance": 1234}, []string{"id"}); err == nil && txItem != nil {
		txItems = append(txItems, txItem)
	} else {
		panic(err)
	}
	if txItem, err := adc.BuildTxPutIfNotExist(tableName, map[string]interface{}{"id": "thanhnb", "balance": 4321}, []string{"id"}); err == nil && txItem != nil {
		txItems = append(txItems, txItem)
	} else {
		panic(err)
	}
	if txItem, err := adc.BuildTxPutIfNotExist(tableName, map[string]interface{}{"id": "nbthanh", "balance": 1000}, []string{"id"}); err == nil && txItem != nil {
		txItems = append(txItems, txItem)
	} else {
		panic(err)
	}
	result, err := adc.ExecTxWriteItems(nil, &dynamodb.TransactWriteItemsInput{
		ReturnConsumedCapacity: aws.String("INDEXES"),
		TransactItems:          txItems,
	})
	fmt.Printf("%e\n%#v\n", err, err)
	fmt.Println(result)

	_txTransferMoney(adc, tableName, "btnguyen2k", "thanhnb", 100)
	_txTransferMoney(adc, tableName, "thanhnb", "btnguyen2k", 2000)
	_txTransferMoney(adc, tableName, "btnguyen2k", "nbthanh", 200)
}
