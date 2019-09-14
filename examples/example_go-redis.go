package main

import (
    "fmt"
    "github.com/btnguyen2k/prom"
)

/*
Setup a Redis Sentinel using docker:

1$ docker network create app-tier --driver bridge
2$ docker run -d --name redis-server -e ALLOW_EMPTY_PASSWORD=yes --network app-tier -p 6379:6379 bitnami/redis:latest
3$ docker run -it --rm --name redis-sentinel -e REDIS_MASTER_HOST=redis-server --network app-tier -p 26379:26379 bitnami/redis-sentinel:latest
*/

func main() {
    SEP := "======================================================================"

    hostsAndPorts := "localhost:7006;localhost:5000" // first server is normal Redis server, second one is sentinel server
    password := ""
    maxRetries := 3
    goRedisConnect := prom.NewGoRedisConnect(hostsAndPorts, password, maxRetries)

    db := 0
    client := goRedisConnect.GetClient(db)
    {
        fmt.Println(SEP)
        fmt.Println("Redis client:", client)
        result, err := client.FlushAll().Result()
        fmt.Println("FlushAll:", result, err)
        result, err = client.Ping().Result()
        fmt.Println("Ping    :", result, err)

        fmt.Println(SEP)
        resultGet := client.Get("key")
        fmt.Println("Get[key]:", resultGet)
        resultSet := client.Set("key", "value", 0)
        fmt.Println("Set[key]:", resultSet)
        resultGet = client.Get("key")
        fmt.Println("Get[key]:", resultGet)
    }

    //
    // goRedisConnect.SetSentinelMasterName("master")
    // failoverClient := goRedisConnect.GetFailoverClient(db)
    // {
    // 	fmt.Println("Sentinel Redis client:", failoverClient)
    // 	result, err := failoverClient.FlushAll().Result()
    // 	fmt.Println("FlushAll:", result, err)
    // 	result, err = failoverClient.Ping().Result()
    // 	fmt.Println("Ping    :", result, err)
    // 	fmt.Println("==============================")
    // }
}
