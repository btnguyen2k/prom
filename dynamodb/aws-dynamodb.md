**'Prom' for AWS DynamoDB (https://github.com/aws/aws-sdk-go/tree/main/service/dynamodb)**

[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/prom)](https://pkg.go.dev/github.com/btnguyen2k/prom#AwsDynamodbConnect)

Usage:

```go
// credentials from env.AWS_ACCESS_KEY_ID & env.AWS_SECRET_ACCESS_KEY & env.AWS_SESSION_TOKEN
cfg := &aws.Config{
    Region:      aws.String(region),
    Credentials: credentials.NewEnvCredentials(),
}
timeoutMs := 10000
awsDynamodbConnect, err := prom.NewAwsDynamodbConnect(cfg, nil, nil, timeoutMs)
if err != nil {
    panic(err)
}

// from now on, the AwsDynamodbConnect instance can be shared & used by all goroutines within the application
```

See more:
- [examples](examples/)
- [Go driver for AWS SDK](https://github.com/aws/aws-sdk-go/)
