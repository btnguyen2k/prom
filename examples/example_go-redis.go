package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

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

	rand.Seed(time.Now().UnixNano())
	{
		goRedisConnect := prom.NewGoRedisConnect("localhost:7006", "", 3)
		client := goRedisConnect.GetClient(0)
		fmt.Println("Redis client:", client)
		result, err := client.FlushAll().Result()
		fmt.Println("FlushAll:", result, err)
		result, err = client.Ping().Result()
		fmt.Println("Ping    :", result, err)

		resultGet := client.Get("key")
		fmt.Printf("Get[key]: %#v / %s\n", resultGet.Val(), resultGet.Err())
		resultSet := client.Set("key", strconv.Itoa(rand.Int()), 0)
		fmt.Printf("Set[key]: %#v / %s\n", resultSet.Val(), resultSet.Err())
		resultGet = client.Get("key")
		fmt.Printf("Get[key]: %#v / %s\n", resultGet.Val(), resultSet.Err())
		fmt.Println(SEP)
	}

	{
		goRedisConnect := prom.NewGoRedisConnect("localhost:5000", "", 3)
		goRedisConnect.SetSentinelMasterName("sentinel7000")
		client := goRedisConnect.GetFailoverClient(0)
		fmt.Println("Sentinel Redis client:", client)
		result, err := client.FlushAll().Result()
		fmt.Println("FlushAll:", result, err)
		result, err = client.Ping().Result()
		fmt.Println("Ping    :", result, err)

		resultGet := client.Get("key")
		fmt.Printf("Get[key]: %#v / %s\n", resultGet.Val(), resultGet.Err())
		resultSet := client.Set("key", strconv.Itoa(rand.Int()), 0)
		fmt.Printf("Set[key]: %#v / %s\n", resultSet.Val(), resultSet.Err())
		resultGet = client.Get("key")
		fmt.Printf("Get[key]: %#v / %s\n", resultGet.Val(), resultSet.Err())
		fmt.Println(SEP)
	}

	{
		goRedisConnect := prom.NewGoRedisConnect("localhost:7000,localhost:7001,localhost:7002,localhost:7003,localhost:7004,localhost:7005", "", 3)
		goRedisConnect.SetSentinelMasterName("sentinel7000")
		client := goRedisConnect.GetClusterClient()
		fmt.Println("Cluster Redis client:", client)
		result, err := client.FlushAll().Result()
		fmt.Println("FlushAll:", result, err)
		result, err = client.Ping().Result()
		fmt.Println("Ping    :", result, err)

		resultGet := client.Get("key")
		fmt.Printf("Get[key]: %#v / %s\n", resultGet.Val(), resultGet.Err())
		resultSet := client.Set("key", strconv.Itoa(rand.Int()), 0)
		fmt.Printf("Set[key]: %#v / %s\n", resultSet.Val(), resultSet.Err())
		resultGet = client.Get("key")
		fmt.Printf("Get[key]: %#v / %s\n", resultGet.Val(), resultSet.Err())
		fmt.Println(SEP)
	}
}
