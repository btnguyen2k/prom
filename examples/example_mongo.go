package main

import (
	"fmt"
	"github.com/btnguyen2k/prom"
	"go.mongodb.org/mongo-driver/bson"
)

// construct an 'prom.MongoConnect' instance
func createMongoConnect() *prom.MongoConnect {
	url := "mongodb://test:test@localhost:27017/test"
	db := "test"
	timeoutMs := 10000
	mongoConnect, _ := prom.NewMongoConnect(url, db, timeoutMs)
	if mongoConnect == nil {
		panic("error creating [prom.MongoConnect] instance")
	}
	return mongoConnect
}

func main() {
	mongoConnect := createMongoConnect()

	{
		// get the database object and send ping command
		dbObj := mongoConnect.GetDatabase()
		fmt.Println("Current database:", dbObj.Name())
		fmt.Println("Is connected    :", mongoConnect.IsConnected())
		err := mongoConnect.Ping()
		if err != nil {
			fmt.Println("Ping error      :", err)
		} else {
			fmt.Println("Ping ok")
		}

		fmt.Println("==================================================")
	}

	{
		// check if a database/collection exists
		hasDb, err := mongoConnect.HasDatabase("test")
		fmt.Println("Has database [test]  :", hasDb, err)
		hasDb, err = mongoConnect.HasDatabase("test1")
		fmt.Println("Has database [test1] :", hasDb, err)

		// check if a collection in current database exists
		hasCollection, err := mongoConnect.HasCollection("demo")
		fmt.Println("Has collection [demo]:", hasCollection, err)

		fmt.Println("==================================================")
	}

	{
		// drop a collection
		err := mongoConnect.GetCollection("demo").Drop(nil)
		fmt.Println("Drop collection [demo]'s            :", err)

		// create a collection
		result, err := mongoConnect.CreateCollection("demo")
		fmt.Println("Create collection [demo]'s          :", result.Err(), err)
		if err == nil && result.Err() == nil {
			// create indexes for a collection
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
			fmt.Println("Create indexes for collection [demo]:", result.Err(), err)
		}

		fmt.Println("==================================================")
	}

	{
		// insert some documents
		demo := mongoConnect.GetCollection("demo")

		doc := map[string]interface{}{
			"username": "btnguyen2k",
			"email":    "btnguyen2k(at)gmail.com",
			"name": map[string]interface{}{
				"first": "Thanh",
				"last":  "Nguyen",
			},
			"tags": []string{"Java", "Golang", "HTML", "CSS", "JS"},
		}
		fmt.Println("Inserting document:", doc)
		result, err := demo.InsertOne(nil, doc)
		if err != nil {
			fmt.Println("\tError:", err)
		} else {
			fmt.Println("\tNew document:", result.InsertedID)
		}

		fmt.Println("==================================================")
	}

	{
		// load a document
		filter := bson.M{"username": "btnguyen2k"}
		demo := mongoConnect.GetCollection("demo")
		fmt.Println("Loading a document with filter:", filter)
		result := demo.FindOne(nil, filter)
		{
			row, err := mongoConnect.DecodeSingleResult(result)
			if err != nil {
				fmt.Println("\tError:", err)
			} else if row == nil {
				fmt.Println("\tDocument not found with filter:", filter)
			} else {
				fmt.Println("\tDocument:", row)
			}
		}
		{
			// result of type mongo.SingleResult is only available once, the following will result "document not found"!
			row, err := mongoConnect.DecodeSingleResult(result)
			if err != nil {
				fmt.Println("\tError:", err)
			} else if row == nil {
				fmt.Println("\tDocument not found with filter:", filter)
			} else {
				fmt.Println("\tDocument:", row)
			}
		}

		fmt.Println("==================================================")
	}

	{
		// load a document
		filter := bson.M{"username": "btnguyen2k"}
		demo := mongoConnect.GetCollection("demo")
		fmt.Println("Loading a document with filter:", filter)
		result := demo.FindOne(nil, filter)
		{
			row, err := mongoConnect.DecodeSingleResultRaw(result)
			if err != nil {
				fmt.Println("\tError:", err)
			} else if row == "" {
				fmt.Println("\tDocument not found with filter:", filter)
			} else {
				fmt.Println("\tDocument:", string(row))
			}
		}
		{
			// result of type mongo.SingleResult is only available once, the following will result "document not found"!
			row, err := mongoConnect.DecodeSingleResultRaw(result)
			if err != nil {
				fmt.Println("\tError:", err)
			} else if row == "" {
				fmt.Println("\tDocument not found with filter:", filter)
			} else {
				fmt.Println("\tDocument:", string(row))
			}
		}

		fmt.Println("==================================================")
	}
}
