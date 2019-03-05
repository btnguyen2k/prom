package prom

import (
	"github.com/go-redis/redis"
	"regexp"
	"sync"
)

/*
GoRedisConnect holds a go-redis client (https://github.com/go-redis/redis) that can be shared within the application.
*/
type GoRedisConnect struct {
	hostsAndPorts []string // list of host:port addresses
	password      string   // password to authenticate against Redis server
	maxRetries    int      // max number of retries for failed operations

	masterName      string                // the sentinel master name, used by failover clients
	slaveReadOnly   bool                  // enables read-only commands on slave nodes
	clients         map[int]*redis.Client // clients mapped with Redis databases
	failoverClients map[int]*redis.Client // failover-clients mapped with Redis databases
	clusterClient   *redis.ClusterClient  // cluster-enabled client
}

/*
NewGoRedisConnect constructs a new GoRedisConnect instance with supplied options.

Parameters:

  - hostsAndPorts : list of Redis servers (example: "host1:6379,host2;host3:6380")
  - password      : password to authenticate against Redis server
  - maxRetries    : max number of retries for failed operations
*/
func NewGoRedisConnect(hostsAndPorts, password string, maxRetries int) *GoRedisConnect {
	if maxRetries < 0 {
		maxRetries = 0
	}
	r := &GoRedisConnect{
		hostsAndPorts:   regexp.MustCompile("[,;\\s]+").Split(hostsAndPorts, -1),
		password:        password,
		maxRetries:      maxRetries,
		slaveReadOnly:   true,
		clients:         map[int]*redis.Client{},
		failoverClients: map[int]*redis.Client{},
	}
	return r
}

/*
SetSlaveReadOnly enables/disables read-only commands on slave nodes.
The change will apply to newly created clients, existing one will NOT be effected!
This function returns the current GoRedisConnect instance so that function calls can be chained.
*/
func (r *GoRedisConnect) SetSlaveReadOnly(readOnly bool) *GoRedisConnect {
	r.slaveReadOnly = readOnly
	return r
}

/*
SetSentinelMasterName sets the sentinel master name, used by failover clients.
The change will apply to newly created clients, existing one will NOT be effected!
This function returns the current GoRedisConnect instance so that function calls can be chained.
*/
func (r *GoRedisConnect) SetSentinelMasterName(masterName string) *GoRedisConnect {
	r.masterName = masterName
	return r
}

var mutexGoredis sync.Mutex

/*----------------------------------------------------------------------*/

func (r *GoRedisConnect) newClient(db int) *redis.Client {
	if db < 0 {
		db = 0
	}
	client := redis.NewClient(&redis.Options{
		Addr:       r.hostsAndPorts[0],
		Password:   r.password,
		MaxRetries: r.maxRetries,
		DB:         db,
	})
	return client
}

/*
GetClient returns the redis.Client associated with the specified db number.

  - This function use the first entry of 'hostsAndPorts' as Redis server address.
  - This function returns the existing client, if any (i.e. no new redis.Client instance will be created).
*/
func (r *GoRedisConnect) GetClient(db int) *redis.Client {
	if db < 0 {
		db = 0
	}
	client, ok := r.clients[db]
	if !ok {
		mutexGoredis.Lock()
		defer mutexGoredis.Unlock()
		client, ok = r.clients[db]
		if !ok {
			client = r.newClient(db)
			r.clients[db] = client
		}
	}
	return client
}

func (r *GoRedisConnect) newFailoverClient(db int) *redis.Client {
	if db < 0 {
		db = 0
	}
	client := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    r.masterName,
		SentinelAddrs: r.hostsAndPorts,
		Password:      r.password,
		MaxRetries:    r.maxRetries,
		DB:            db,
	})
	return client
}

/*
GetFailoverClient returns the failover redis.Client associated with the specified db number.

  - This function use 'hostsAndPorts' config as list of Redis Sentinel server addresses.
  - This function returns the existing client, if any (i.e. no new redis.Client instance will be created).
*/
func (r *GoRedisConnect) GetFailoverClient(db int) *redis.Client {
	if db < 0 {
		db = 0
	}
	client, ok := r.failoverClients[db]
	if !ok {
		mutexGoredis.Lock()
		defer mutexGoredis.Unlock()
		client, ok = r.failoverClients[db]
		if !ok {
			client = r.newFailoverClient(db)
			r.failoverClients[db] = client
		}
	}
	return client
}

func (r *GoRedisConnect) newClusterClient() *redis.ClusterClient {
	return redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:          r.hostsAndPorts,
		Password:       r.password,
		MaxRetries:     r.maxRetries,
		ReadOnly:       r.slaveReadOnly,
		RouteByLatency: true,
	})
}

/*
GetClusterClient returns the redis.ClusterClient associated with the GoRedisConnect.

  - This function use 'hostsAndPorts' config as list of Redis server addresses.
  - This function returns the existing client, if any (i.e. no new redis.ClusterClient instance will be created).
*/
func (r *GoRedisConnect) GetClusterClient() *redis.ClusterClient {
	if r.clusterClient == nil {
		mutexGoredis.Lock()
		defer mutexGoredis.Unlock()
		if r.clusterClient == nil {
			r.clusterClient = r.newClusterClient()
		}
	}
	return r.clusterClient
}
