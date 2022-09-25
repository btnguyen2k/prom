**'Prom' for the official Go driver for MongoDB (https://github.com/mongodb/mongo-go-driver)**

[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/prom)](https://pkg.go.dev/github.com/btnguyen2k/prom/mongo)

> Supported/Tested libraries/drivers+version: `go.mongodb.org/mongo-driver v1.9.1`.

Usage:

```go
import (
    "github.com/btnguyen2k/prom/mongo"
)

url := "mongodb://user:passwd@localhost:37017/?authSource=auth_db"
// urlReplSet := "mongodb://user:passwd@host1:27017,host2:27017,host3:27017/?authSource=auth_db&replicaSet=rsName"
db := "mydb"
timeoutMs := 10000
mongoConnect, err := mongo.NewMongoConnect(url, db, timeoutMs)

if err != nil {
    panic(err)
}

// from now on, the MongoConnect instance can be shared & used by all goroutines within the application
```

See more:
- [examples](../examples/mongo/)
- [Go driver for MongoDB](https://godoc.org/go.mongodb.org/mongo-driver/mongo)
