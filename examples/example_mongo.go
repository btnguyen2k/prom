package main

import (
	"fmt"
	"github.com/btnguyen2k/prom"
)

func main() {
	url := "mongodb://test:test@localhost:27017/test"
	db := "test"
	timeoutMs := 10000
	mongoConnect, err := prom.NewMongoConnect(url, db, timeoutMs)
	if mongoConnect == nil {
		panic(err)
	}
	if err != nil {
		fmt.Println("Error making connection to Mongo:", err)
	}
	fmt.Println("Ping    :", mongoConnect.Ping(), mongoConnect.IsConnected())
	dbObj := mongoConnect.GetDatabase()
	fmt.Println("Database:", dbObj.Name())

	hasDb, err := mongoConnect.HasDatabase("test")
	fmt.Println("Has database 'test'  :", hasDb, err)
	hasDb, err = mongoConnect.HasDatabase("test1")
	fmt.Println("Has database 'test1' :", hasDb, err)

	hasCollection, err := mongoConnect.HasCollection("demo")
	fmt.Println("Has collection 'demo':", hasCollection, err)
	if hasCollection {
		err := mongoConnect.GetCollection("demo").Drop(nil)
		if err != nil {
			fmt.Println("Error while dropping collection 'demo':", err)
		}
	}
	{
		result, err := mongoConnect.CreateCollection("demo")
		fmt.Println("Create collection 'demo'            :", result.Err(), err)
		indexes := []interface{}{
			map[string]interface{}{
				"key": map[string]interface{}{
					"username": 1, // ascending index
				},
				"name":   "uidx_username",
				"unique": true,
			},
			map[string]interface{}{
				"key": map[string]interface{}{
					"email": -1, // descending index
				},
				"name": "idx_email",
			},
		}
		result, err = mongoConnect.CreateIndexes("demo", indexes)
		fmt.Println("Create indexes for collection 'demo':", result.Err(), err)
	}
	hasCollection, err = mongoConnect.HasCollection("demo")
	fmt.Println("Has collection 'demo':", hasCollection, err)
}
