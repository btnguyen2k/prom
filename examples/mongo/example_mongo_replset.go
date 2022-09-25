// go run example_mongo_replset.go
//
// Run a replica-set MongoDB via Docker:
//   docker run -d --name mongodb-replset -e MONGODB_REPLICA_SET_MODE=primary -e MONGODB_REPLICA_SET_KEY=myreplset -e MONGODB_ADVERTISED_HOSTNAME=localhost -e MONGODB_PRIMARY_PORT_NUMBER=27017 -e MONGODB_ROOT_PASSWORD=test -e MONGODB_DATABASE=test -p 27017:27017 bitnami/mongodb:4.2
// Setup env:
//   export MONGO_DB="test"
//   export MONGO_URL="mongodb://root:test@localhost:27017/?authSource=admin&replicaSet=replicaset"
package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/btnguyen2k/prom/mongo"
)

var _timezoneMongo = "Asia/Ho_Chi_Minh"

// construct an 'prom.MongoConnect' instance
func _createMongoConnect() *mongo.MongoConnect {
	url := strings.ReplaceAll(os.Getenv("MONGO_URL"), `"`, "")
	db := "test"
	if os.Getenv("MONGO_DB") != "" {
		db = strings.ReplaceAll(os.Getenv("MONGO_DB"), `"`, "")
	}
	timeoutMs := 30000
	mongoConnect, _ := mongo.NewMongoConnect(url, db, timeoutMs)
	if mongoConnect == nil {
		panic("error creating [prom.MongoConnect] instance")
	}
	return mongoConnect
}

func _toJson(o interface{}) string {
	js, _ := json.Marshal(o)
	return string(js)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	SEP := "======================================================================"
	mongoConnect := _createMongoConnect()
	defer mongoConnect.Close(nil)
	loc, _ := time.LoadLocation(_timezoneMongo)
	fmt.Println("Timezone:", loc)

	{
		fmt.Println("-== Database & Ping info ==-")

		// get the database object and send ping command
		dbObj := mongoConnect.GetDatabase()
		fmt.Println("\tCurrent database:", dbObj.Name())
		fmt.Println("\tIs connected    :", mongoConnect.IsConnected())
		err := mongoConnect.Ping(nil)
		if err != nil {
			fmt.Println("\tPing error      :", err)
		} else {
			fmt.Println("\tPing ok")
		}

		fmt.Println(SEP)
	}

	{
		fmt.Println("-== Database & Collection Existence ==-")

		// check if a database exists
		hasDb, err := mongoConnect.HasDatabase("test")
		fmt.Println("\tHas database [test]  :", hasDb, err)
		hasDb, err = mongoConnect.HasDatabase("test1")
		fmt.Println("\tHas database [test1] :", hasDb, err)

		// check if a collection in current database exists
		hasCollection, err := mongoConnect.HasCollection("demo")
		fmt.Println("\tHas collection [demo]:", hasCollection, err)
		hasCollection, err = mongoConnect.HasCollection("demo1")
		fmt.Println("\tHas collection [demo1]:", hasCollection, err)

		fmt.Println(SEP)
	}

	{
		fmt.Println("-== Drop/Create Collection & Index ==-")

		// drop a collection
		err := mongoConnect.GetCollection("demo").Drop(nil)
		fmt.Println("\tDrop collection [demo]'s            :", err)

		// create a collection
		err = mongoConnect.CreateCollection("demo")
		fmt.Println("\tCreate collection [demo]'s          :", err)
		if err == nil {
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
			result, err := mongoConnect.CreateCollectionIndexes("demo", indexes)
			fmt.Println("\tCreate indexes for collection [demo]:", result, err)
		}

		fmt.Println(SEP)
	}

	{
		fmt.Println("-== Insert Documents to Collection ==-")
		demo := mongoConnect.GetCollection("demo")

		t := time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
		doc := map[string]interface{}{
			"username":   "btnguyen2k",
			"email":      "btnguyen2k(at)gmail.com",
			"data_bool":  true,
			"data_int":   103,
			"data_float": 19.81,
			"date_time":  t,
			"data_map": map[string]interface{}{
				"a": "a string",
				"b": 1,
				"c": false,
				"t": t,
			},
			"data_arr": []interface{}{"1", 2, 3.4, t},
		}
		fmt.Println("\tInserting document:", _toJson(doc))
		result, err := demo.InsertOne(nil, doc)
		if err != nil {
			fmt.Println("\t\tError:", err)
		} else {
			fmt.Println("\t\tNew document:", result.InsertedID)
		}

		t = time.Unix(int64(rand.Int31()), rand.Int63()%1000000000).In(loc)
		doc = map[string]interface{}{
			"username": "nbthanh",
			"email":    "btnguyen2k(at)gmail.com",
			"name":     "Thanh Nguyen",
			"tags":     []string{"HTML", "CSS", "JS"},
			"time":     t,
		}
		fmt.Println("\tInserting document:", _toJson(doc))
		result, err = demo.InsertOne(nil, doc)
		if err != nil {
			fmt.Println("\t\tError:", err)
		} else {
			fmt.Println("\t\tNew document:", result.InsertedID)
		}

		fmt.Println(SEP)
	}

	{
		fmt.Println("-== Load a Single Document from Collection ==-")
		demo := mongoConnect.GetCollection("demo")

		filter := bson.M{"username": "btnguyen2k"}
		fmt.Println("\tLoading a document (decoded as document) with filter:", filter)
		result := demo.FindOne(nil, filter)
		{
			row, err := mongoConnect.DecodeSingleResult(result)
			if err != nil {
				fmt.Println("\t\tError:", err)
			} else if row == nil {
				fmt.Println("\t\tDocument not found with filter:", filter)
			} else {
				fmt.Println("\t\tDocument:", _toJson(row))
			}
		}
		{
			// result of type mongo.SingleResult is only available once, the following will result "document not found"!
			row, err := mongoConnect.DecodeSingleResult(result)
			if err != nil {
				fmt.Println("\t\tError:", err)
			} else if row == nil {
				fmt.Println("\t\tDocument not found with filter:", filter)
			} else {
				fmt.Println("\t\tDocument:", _toJson(row))
			}
		}

		fmt.Println(SEP)
	}

	{
		fmt.Println("-== Load a Single Document (multiple matches) from Collection ==-")
		demo := mongoConnect.GetCollection("demo")

		// load a document: multiple document matched, but only one returned
		filter := bson.M{"email": "btnguyen2k(at)gmail.com"}
		fmt.Println("\tLoading a document (decoded as raw json data) with filter:", filter)
		result := demo.FindOne(nil, filter)
		{
			row, err := mongoConnect.DecodeSingleResultRaw(result)
			if err != nil {
				fmt.Println("\t\tError:", err)
			} else if row == nil {
				fmt.Println("\t\tDocument not found with filter:", filter)
			} else {
				var doc interface{}
				err := json.Unmarshal(row, &doc)
				if err != nil {
					fmt.Println("\t\tError:", err)
					fmt.Println("\t\tData :", string(row))
				} else {
					fmt.Println("\t\tDocument:", _toJson(doc))
				}
			}
		}
		{
			// result of type mongo.SingleResult is only available once, the following will result "document not found"!
			row, err := mongoConnect.DecodeSingleResultRaw(result)
			if err != nil {
				fmt.Println("\t\tError:", err)
			} else if row == nil {
				fmt.Println("\t\tDocument not found with filter:", filter)
			} else {
				var doc interface{}
				err := json.Unmarshal(row, &doc)
				if err != nil {
					fmt.Println("\\ttError:", err)
					fmt.Println("\t\tData :", string(row))
				} else {
					fmt.Println("\t\tDocument:", _toJson(doc))
				}
			}
		}

		fmt.Println(SEP)
	}

	{
		fmt.Println("-== Load Multiple Documents from Collection ==-")
		demo := mongoConnect.GetCollection("demo")

		// load list of documents
		filter := bson.M{"email": "btnguyen2k(at)gmail.com"}
		fmt.Println("\tLoading documents (with callback & decoded as document) with filter:", filter)
		result, err := demo.Find(nil, filter)
		if err != nil {
			fmt.Println("\t\tError:", err)
		} else {
			defer result.Close(nil)
			mongoConnect.DecodeResultCallback(nil, result, func(docNum int, doc bson.M, err error) bool {
				if err != nil {
					fmt.Println("\t\tError loading document #", docNum)
				} else {
					fmt.Println("\t\tDoc [", docNum, "]:", _toJson(doc))
				}
				return true // continue processing remaining rows
			})
		}

		fmt.Println(SEP)
	}

	{
		fmt.Println("-== Load Multiple Documents from Collection ==-")
		demo := mongoConnect.GetCollection("demo")

		// load list of documents
		filter := bson.M{"email": "btnguyen2k(at)gmail.com"}
		fmt.Println("\tLoading documents (with callback & decoded as raw json data) with filter:", filter)
		result, err := demo.Find(nil, filter)
		if err != nil {
			fmt.Println("\t\tError:", err)
		} else {
			defer result.Close(nil)
			mongoConnect.DecodeResultCallbackRaw(nil, result, func(docNum int, row []byte, err error) bool {
				if err != nil {
					fmt.Println("\t\tError loading document #", docNum)
				} else {
					var doc interface{}
					err := json.Unmarshal(row, &doc)
					if err != nil {
						fmt.Println("\t\tError:", err)
						fmt.Println("\t\tData :", string(row))
					} else {
						fmt.Println("\tDoc [", docNum, "]:", _toJson(doc))
					}
				}
				return true // continue processing remaining rows
			})
		}

		fmt.Println(SEP)
	}

	{
		fmt.Println("-== Load Non-exist Document from Collection ==-")
		demo := mongoConnect.GetCollection("demo")

		// load non-exist document
		filter := bson.M{"username": "not-exist"}
		row := demo.FindOne(nil, filter)
		jsData, err := mongoConnect.DecodeSingleResultRaw(row)
		fmt.Println(jsData != nil, err)

		fmt.Println(SEP)
	}
}
