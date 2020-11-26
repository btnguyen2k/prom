**'Prom' for go-redis library (https://github.com/go-redis/redis)**

[![PkgGoDev](https://pkg.go.dev/badge/github.com/btnguyen2k/prom)](https://pkg.go.dev/github.com/btnguyen2k/prom#GoRedisConnect)

Usage:

```go
hostsAndPorts  := "host1:6379,host2;host3:6380"
password       := ""
maxRetries     := 3
goRedisConnect := prom.NewGoRedisConnect(hostsAndPorts, password, maxRetries)

// Enable read-only commands for slave nodes. Used by cluster clients only!
goRedisConnect.SetSlaveReadOnly(true)

// Set name of master node. Used by failover/sentinel clients only!
goRedisConnect.SetSentinelMasterName("failover-master")

// from now on, one goRedisConnect instance can be shared & used by all goroutines within the application

db := 0

// get a *redis.Client instance, connecting to Redis database specified by 'db'
client := goRedisConnect.GetClient(db)

// get a failover *redis.Client instance, connecting to Redis database specified by 'db'
failoverClient := goRedisConnect.GetFailoverClient(db)

// get a *redis.ClusterClient instance 
clusterClient := goRedisConnect.GetClusterClient()
```

See more:
- [examples](examples/)
- [go-redis](https://godoc.org/github.com/go-redis/redis)
