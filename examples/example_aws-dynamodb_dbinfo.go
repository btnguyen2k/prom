package main

import (
	"fmt"
	"math/rand"
	"time"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	adc := createAwsDynamodbConnect("ap-southeast-1")
	defer adc.Close()

	fmt.Println("-== Database Info ==-")

	db := adc.GetDb()
	fmt.Println("  Region    :", db.SigningRegion)
	fmt.Println("  Endpoint  :", db.Endpoint)
	fmt.Println("  APIVersion:", db.APIVersion)
	fmt.Println("  Service   :", db.ServiceID, db.ServiceName, db.SigningName)
}
