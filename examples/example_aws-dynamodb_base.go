package main

import (
	"encoding/json"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/btnguyen2k/prom"
)

// construct an 'prom.AwsDynamodbConnect' instance
func createAwsDynamodbConnect(region string) *prom.AwsDynamodbConnect {
	cfg := &aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewEnvCredentials(),
	}
	awsDynamodbEndpoint := strings.ReplaceAll(os.Getenv("AWS_DYNAMODB_ENDPOINT"), `"`, "")
	if awsDynamodbEndpoint != "" {
		cfg.Endpoint = aws.String(awsDynamodbEndpoint)
		cfg.DisableSSL = aws.Bool(strings.HasPrefix(awsDynamodbEndpoint, "http://"))
	}
	adc, _ := prom.NewAwsDynamodbConnect(cfg, nil, nil, 10000)
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
