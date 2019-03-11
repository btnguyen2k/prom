package main

import (
	"encoding/json"
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
			"tags": []string{"Java", "Golang"},
		}
		fmt.Println("Inserting document:", doc)
		result, err := demo.InsertOne(nil, doc)
		if err != nil {
			fmt.Println("\tError:", err)
		} else {
			fmt.Println("\tNew document:", result.InsertedID)
		}

		doc = map[string]interface{}{
			"username": "nbthanh",
			"email":    "btnguyen2k(at)gmail.com",
			"name":     "Thanh Nguyen",
			"tags":     []string{"HTML", "CSS", "JS"},
		}
		fmt.Println("Inserting document:", doc)
		result, err = demo.InsertOne(nil, doc)
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
		fmt.Println("Loading a document (decoded as document) with filter:", filter)
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
		// load a document: multiple document matched, but only one returned
		filter := bson.M{"email": "btnguyen2k(at)gmail.com"}
		demo := mongoConnect.GetCollection("demo")
		fmt.Println("Loading a document (decoded as raw json data) with filter:", filter)
		result := demo.FindOne(nil, filter)
		{
			row, err := mongoConnect.DecodeSingleResultRaw(result)
			if err != nil {
				fmt.Println("\tError:", err)
			} else if row == nil {
				fmt.Println("\tDocument not found with filter:", filter)
			} else {
				var doc interface{}
				err := json.Unmarshal(row, &doc)
				if err != nil {
					fmt.Println("\tError:", err)
					fmt.Println("\tData :", string(row))
				} else {
					fmt.Println("\tDocument:", doc)
				}
			}
		}
		{
			// result of type mongo.SingleResult is only available once, the following will result "document not found"!
			row, err := mongoConnect.DecodeSingleResultRaw(result)
			if err != nil {
				fmt.Println("\tError:", err)
			} else if row == nil {
				fmt.Println("\tDocument not found with filter:", filter)
			} else {
				var doc interface{}
				err := json.Unmarshal(row, &doc)
				if err != nil {
					fmt.Println("\tError:", err)
					fmt.Println("\tData :", string(row))
				} else {
					fmt.Println("\tDocument:", doc)
				}
			}
		}

		fmt.Println("==================================================")
	}

	{
		// load list of documents
		filter := bson.M{"email": "btnguyen2k(at)gmail.com"}
		demo := mongoConnect.GetCollection("demo")
		fmt.Println("Loading documents (with callback & decoded as document) with filter:", filter)
		result, err := demo.Find(nil, filter)
		if err != nil {
			fmt.Println("\tError:", err)
		} else {
			defer result.Close(nil)
			mongoConnect.DecodeResultCallback(nil, result, func(docNum int, doc bson.M, err error) bool {
				if err != nil {
					fmt.Println("\tError loading document #", docNum)
				} else {
					fmt.Println("\tDoc [", docNum, "]:", doc)
				}
				return true // continue processing remaining rows
			})
		}

		fmt.Println("==================================================")
	}

	{
		// load list of documents
		filter := bson.M{"email": "btnguyen2k(at)gmail.com"}
		demo := mongoConnect.GetCollection("demo")
		fmt.Println("Loading documents (with callback & decoded as raw json data) with filter:", filter)
		result, err := demo.Find(nil, filter)
		if err != nil {
			fmt.Println("\tError:", err)
		} else {
			defer result.Close(nil)
			mongoConnect.DecodeResultCallbackRaw(nil, result, func(docNum int, row []byte, err error) bool {
				if err != nil {
					fmt.Println("\tError loading document #", docNum)
				} else {
					var doc interface{}
					err := json.Unmarshal(row, &doc)
					if err != nil {
						fmt.Println("\tError:", err)
						fmt.Println("\tData :", string(row))
					} else {
						fmt.Println("\tDoc [", docNum, "]:", doc)
					}
				}
				return true // continue processing remaining rows
			})
		}

		fmt.Println("==================================================")
	}
}
