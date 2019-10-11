**'Prom' for AWS DynamoDB (https://github.com/aws/aws-sdk-go/tree/master/service/dynamodb)**

Usage:

```golang
// credentials from env.AWS_ACCESS_KEY_ID & env.AWS_SECRET_ACCESS_KEY & env.AWS_SESSION_TOKEN
cfg := &aws.Config{
    Region:      aws.String(region),
    Credentials: credentials.NewEnvCredentials(),
}
awsDynamodbConnect, err := prom.NewAwsDynamodbConnect(cfg, nil, nil, 10000)
if err != nil {
    panic(err)
}

// from now on, the AwsDynamodbConnect instance can be shared & used by all goroutines within the application
```

See usage examples in [examples directory](examples/). Documentation at [![GoDoc](https://godoc.org/github.com/btnguyen2k/prom?status.svg)](https://godoc.org/github.com/btnguyen2k/prom#AwsDynamodbConnect)

See also [Go driver for AWS SDK](https://github.com/aws/aws-sdk-go/).
