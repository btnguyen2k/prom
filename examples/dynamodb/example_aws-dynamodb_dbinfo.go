// go run example_aws-dynamodb_base.go example_aws-dynamodb_dbinfo.go
package main

import (
	"fmt"
)

func main() {
	adc := createAwsDynamodbConnect()
	defer adc.Close()

	fmt.Println("-== Database Info ==-")

	db := adc.GetDb()
	fmt.Println("  Region    :", db.SigningRegion)
	fmt.Println("  Endpoint  :", db.Endpoint)
	fmt.Println("  APIVersion:", db.APIVersion)
	fmt.Println("  Service   :", db.ServiceID, db.ServiceName, db.SigningName)
}
