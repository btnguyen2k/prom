## 'Prom' for the official Go driver for MongoDB (https://github.com/mongodb/mongo-go-driver)

Usage:

```golang
url := "mongodb://username:password@localhost:27017/auth_db"
db := "mydb"
mongoConnect, err := NewMongoConnect(url, db, 10000)

if err != nil {
    // if mongoConnect is not nil, error can be ignore
}

// from now on, one MongoConnect instance can be shared & used by all goroutines within the application
```

[![GoDoc](https://godoc.org/github.com/btnguyen2k/prom?status.svg)](https://godoc.org/github.com/btnguyen2k/prom#MongoConnect)
