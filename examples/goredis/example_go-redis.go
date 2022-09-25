// go run example_go-redis.go
//
// Run Redis instances via Docker:
//   docker run -d --name redis-all-in-one -e STANDALONE=true -e SENTINEL=true -e IP=0.0.0.0 -p 5000-5002:5000-5002 -p 7000-7007:7000-7007 -p 6379:7006 grokzen/redis-cluster:6.2.0
// Setup env:
//   export REDIS_HOST_AND_PORT="127.0.0.1:6379"
//   export REDIS_FAILOVER_HOSTS_AND_PORTS="127.0.0.1:5000"
//   export REDIS_FAILOVER_MASTER_NAME="sentinel7000"
//   export REDIS_CLUSTER_HOSTS_AND_PORTS="127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005"
package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/btnguyen2k/prom/goredis"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	SEP := "======================================================================"
	redisHostAndPort := strings.ReplaceAll(os.Getenv("REDIS_HOST_AND_PORT"), `"`, "")
	redisFailoverHostsAndPorts := strings.ReplaceAll(os.Getenv("REDIS_FAILOVER_HOSTS_AND_PORTS"), `"`, "")
	redisFailoverMasterName := strings.ReplaceAll(os.Getenv("REDIS_FAILOVER_MASTER_NAME"), `"`, "")
	redisClusterHostsAndPorts := strings.ReplaceAll(os.Getenv("REDIS_CLUSTER_HOSTS_AND_PORTS"), `"`, "")

	if redisHostAndPort != "" {
		goRedisConnect, err := goredis.NewGoRedisConnect(redisHostAndPort, "", 3)
		if err != nil {
			fmt.Println("Error:", err)
		} else {
			client := goRedisConnect.GetClient(0)
			fmt.Println("Redis client:", client)
			result, err := client.FlushAll(context.TODO()).Result()
			fmt.Println("FlushAll:", result, err)
			result, err = client.Ping(context.TODO()).Result()
			fmt.Println("Ping    :", result, err)

			resultGet := client.Get(context.TODO(), "key")
			fmt.Printf("Get[key]: %#v / %s\n", resultGet.Val(), resultGet.Err())
			resultSet := client.Set(context.TODO(), "key", strconv.Itoa(rand.Int()), 0)
			fmt.Printf("Set[key]: %#v / %s\n", resultSet.Val(), resultSet.Err())
			resultGet = client.Get(context.TODO(), "key")
			fmt.Printf("Get[key]: %#v / %s\n", resultGet.Val(), resultSet.Err())
			fmt.Println(SEP)
		}
	}

	if redisFailoverHostsAndPorts != "" {
		goRedisConnect, err := goredis.NewGoRedisConnect(redisFailoverHostsAndPorts, "", 3)
		if err != nil {
			fmt.Println("Error:", err)
		} else {
			goRedisConnect.SetSentinelMasterName(redisFailoverMasterName)
			client := goRedisConnect.GetFailoverClient(0)
			fmt.Println("Sentinel Redis client:", client)
			result, err := client.FlushAll(context.TODO()).Result()
			fmt.Println("FlushAll:", result, err)
			result, err = client.Ping(context.TODO()).Result()
			fmt.Println("Ping    :", result, err)

			resultGet := client.Get(context.TODO(), "key")
			fmt.Printf("Get[key]: %#v / %s\n", resultGet.Val(), resultGet.Err())
			resultSet := client.Set(context.TODO(), "key", strconv.Itoa(rand.Int()), 0)
			fmt.Printf("Set[key]: %#v / %s\n", resultSet.Val(), resultSet.Err())
			resultGet = client.Get(context.TODO(), "key")
			fmt.Printf("Get[key]: %#v / %s\n", resultGet.Val(), resultSet.Err())
			fmt.Println(SEP)
		}
	}

	if redisClusterHostsAndPorts != "" {
		goRedisConnect, err := goredis.NewGoRedisConnect(redisClusterHostsAndPorts, "", 3)
		if err != nil {
			fmt.Println("Error:", err)
		} else {
			client := goRedisConnect.GetClusterClient()
			fmt.Println("Cluster Redis client:", client)
			result, err := client.FlushAll(context.TODO()).Result()
			fmt.Println("FlushAll:", result, err)
			result, err = client.Ping(context.TODO()).Result()
			fmt.Println("Ping    :", result, err)

			resultGet := client.Get(context.TODO(), "key")
			fmt.Printf("Get[key]: %#v / %s\n", resultGet.Val(), resultGet.Err())
			resultSet := client.Set(context.TODO(), "key", strconv.Itoa(rand.Int()), 0)
			fmt.Printf("Set[key]: %#v / %s\n", resultSet.Val(), resultSet.Err())
			resultGet = client.Get(context.TODO(), "key")
			fmt.Printf("Get[key]: %#v / %s\n", resultGet.Val(), resultSet.Err())
			fmt.Println(SEP)
		}
	}
}
