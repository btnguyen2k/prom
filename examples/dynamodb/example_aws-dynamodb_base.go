package main

import (
	"encoding/json"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/btnguyen2k/prom/dynamodb"
)

// construct an 'prom.AwsDynamodbConnect' instance
func createAwsDynamodbConnect() *dynamodb.AwsDynamodbConnect {
	awsRegion := strings.ReplaceAll(os.Getenv("AWS_REGION"), `"`, "")
	if awsRegion == "" {
		awsRegion = "default"
	}
	cfg := &aws.Config{
		Region:      aws.String(awsRegion),
		Credentials: credentials.NewEnvCredentials(),
	}
	awsDynamodbEndpoint := strings.ReplaceAll(os.Getenv("AWS_DYNAMODB_ENDPOINT"), `"`, "")
	if awsDynamodbEndpoint != "" {
		cfg.Endpoint = aws.String(awsDynamodbEndpoint)
		cfg.DisableSSL = aws.Bool(strings.HasPrefix(awsDynamodbEndpoint, "http://"))
	}
	adc, _ := dynamodb.NewAwsDynamodbConnect(cfg, nil, nil, 10000)
	if adc == nil {
		panic("error creating [prom.AwsDynamodbConnect] instance")
	}
	return adc
}

func toJsonDynamodb(o interface{}) string {
	js, _ := json.Marshal(o)
	return string(js)
}

var awsDynamodbSep = "======================================================================"
var awsDynamodbPkAttrs []string
var awsDynamodbIndexName = "idx_email"
var awsDynamodbNumItems = 30
var awsDynamodbRandomRange = 4
