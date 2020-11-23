package prom

import (
	"regexp"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

// GoRedisConnect holds a go-redis client (https://github.com/go-redis/redis) that can be shared within the application.
type GoRedisConnect struct {
	hostsAndPorts []string // list of host:port addresses
	password      string   // password to authenticate against Redis server
	maxRetries    int      // max number of retries for failed operations

	poolOpts *RedisPoolOpts // Redis connection pool options
	mutex    sync.Mutex

	masterName      string                // the sentinel master name, used by failover clients
	slaveReadOnly   bool                  // enables read-only commands on slave nodes
	clients         map[int]*redis.Client // clients mapped with Redis databases
	failoverClients map[int]*redis.Client // failover-clients mapped with Redis databases
	clusterClient   *redis.ClusterClient  // cluster-enabled client
}

// RedisPoolOpts holds options to configure Redis connection pool.
//
// @available since v0.2.8
type RedisPoolOpts struct {
	// Dial timeout for establishing new connections.
	// Set zero or negative value to use go-redis' default value.
	DialTimeout time.Duration
	// Timeout for socket reads.
	// Set zero or negative value to use go-redis' default value.
	ReadTimeout time.Duration
	// Timeout for socket writes.
	// Set zero or negative value to use go-redis' default value.
	WriteTimeout time.Duration

	// Maximum number of connections.
	// Set zero or negative value to use go-redis' default value.
	PoolSize int
	// Minimum number of idle connections. Default value is 1.
	MinIdleConns int
}

var (
	defaultRedisPoolOpts = &RedisPoolOpts{
		// fast-failed options
		DialTimeout:  10 * time.Millisecond,
		ReadTimeout:  10 * time.Millisecond,
		WriteTimeout: 10 * time.Millisecond,
	}
)

// NewGoRedisConnect constructs a new GoRedisConnect instance with supplied options and default Redis pool options.
//
// Parameters:
//   - hostsAndPorts : list of Redis servers (example: "host1:6379,host2;host3:6380")
//   - password      : password to authenticate against Redis server
//   - maxRetries    : max number of retries for failed operations
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
		poolOpts:        defaultRedisPoolOpts,
	}
	return r
}

// GetSlaveReadOnly returns the current value of 'slaveReadOnly' setting.
//
// @available since v0.2.8
func (r *GoRedisConnect) GetSlaveReadOnly() bool {
	return r.slaveReadOnly
}

// SetSlaveReadOnly enables/disables read-only commands on slave nodes.
//
// The change will apply to newly created clients, existing one will NOT be effected!
// This function returns the current GoRedisConnect instance so that function calls can be chained.
func (r *GoRedisConnect) SetSlaveReadOnly(readOnly bool) *GoRedisConnect {
	r.slaveReadOnly = readOnly
	return r
}

// GetSentinelMasterName returns the current sentinel master name.
//
// @available since v0.2.8
func (r *GoRedisConnect) GetSentinelMasterName() string {
	return r.masterName
}

// SetSentinelMasterName sets the sentinel master name, used by failover clients.
//
// The change will apply to newly created clients, existing one will NOT be effected!
// This function returns the current GoRedisConnect instance so that function calls can be chained.
func (r *GoRedisConnect) SetSentinelMasterName(masterName string) *GoRedisConnect {
	r.masterName = masterName
	return r
}

// GetRedisPoolOpts returns Redis connection pool configurations.
//
// @available since v0.2.8
func (r *GoRedisConnect) GetRedisPoolOpts() *RedisPoolOpts {
	return r.poolOpts
}

// SetRedisPoolOpts sets Redis connection pool configurations.
//
// The change will apply to newly created clients, existing one will NOT be effected!
// This function returns the current GoRedisConnect instance so that function calls can be chained.
// @available since v0.2.8
func (r *GoRedisConnect) SetRedisPoolOpts(opts *RedisPoolOpts) *GoRedisConnect {
	r.poolOpts = opts
	return r
}

/*----------------------------------------------------------------------*/

func (r *GoRedisConnect) closeClients() {
	if r.clients != nil {
		for _, c := range r.clients {
			c.Close()
		}
	}
	r.clients = map[int]*redis.Client{}
}

func (r *GoRedisConnect) closeFailoverClients() {
	if r.failoverClients != nil {
		for _, c := range r.failoverClients {
			c.Close()
		}
	}
	r.failoverClients = map[int]*redis.Client{}
}

func (r *GoRedisConnect) closeClusterClient() {
	if r.clusterClient != nil {
		r.clusterClient.Close()
	}
	r.clusterClient = nil
}

/*
Close closes all underlying Redis connections associated with this GoRedisConnect.

Available: since v0.2.0
*/
func (r *GoRedisConnect) Close() error {
	go r.closeClients()
	go r.closeFailoverClients()
	go r.closeClusterClient()
	return nil
}

func (r *GoRedisConnect) newClient(db int) *redis.Client {
	if db < 0 {
		db = 0
	}
	redisOpts := &redis.Options{
		Addr:       r.hostsAndPorts[0],
		Password:   r.password,
		MaxRetries: r.maxRetries,
		DB:         db,
	}
	if r.poolOpts != nil {
		if r.poolOpts.DialTimeout > 0 {
			redisOpts.DialTimeout = r.poolOpts.DialTimeout
		}
		if r.poolOpts.ReadTimeout > 0 {
			redisOpts.ReadTimeout = r.poolOpts.ReadTimeout
		}
		if r.poolOpts.WriteTimeout > 0 {
			redisOpts.WriteTimeout = r.poolOpts.WriteTimeout
		}
		if r.poolOpts.PoolSize > 0 {
			redisOpts.PoolSize = r.poolOpts.PoolSize
		}
		if r.poolOpts.MinIdleConns > 0 {
			redisOpts.MinIdleConns = r.poolOpts.MinIdleConns
		} else {
			redisOpts.MinIdleConns = 1
		}
	}
	return redis.NewClient(redisOpts)
}

// GetClient returns the redis.Client associated with the specified db number.
//
// Note: do NOT close the returned client (e.g. call redis.Client.Close()).
//
// This function uses the first entry of 'hostsAndPorts' as Redis server address.
// This function returns the existing client, if any (i.e. no new redis.Client instance will be created).
func (r *GoRedisConnect) GetClient(db int) *redis.Client {
	if db < 0 {
		db = 0
	}
	client, ok := r.clients[db]
	if !ok {
		r.mutex.Lock()
		defer r.mutex.Unlock()
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
	redisOpts := &redis.FailoverOptions{
		MasterName:    r.masterName,
		SentinelAddrs: r.hostsAndPorts,
		Password:      r.password,
		MaxRetries:    r.maxRetries,
		DB:            db,
	}
	if r.poolOpts != nil {
		if r.poolOpts.DialTimeout > 0 {
			redisOpts.DialTimeout = r.poolOpts.DialTimeout
		}
		if r.poolOpts.ReadTimeout > 0 {
			redisOpts.ReadTimeout = r.poolOpts.ReadTimeout
		}
		if r.poolOpts.WriteTimeout > 0 {
			redisOpts.WriteTimeout = r.poolOpts.WriteTimeout
		}
		if r.poolOpts.PoolSize > 0 {
			redisOpts.PoolSize = r.poolOpts.PoolSize
		}
		if r.poolOpts.MinIdleConns > 0 {
			redisOpts.MinIdleConns = r.poolOpts.MinIdleConns
		} else {
			redisOpts.MinIdleConns = 1
		}
	}
	return redis.NewFailoverClient(redisOpts)
}

// GetFailoverClient returns the failover redis.Client associated with the specified db number.
//
// Note: do NOT close the returned client (e.g. call redis.Client.Close()).
//
// This function uses 'hostsAndPorts' config as list of Redis Sentinel server addresses.
// This function returns the existing client, if any (i.e. no new redis.Client instance will be created).
func (r *GoRedisConnect) GetFailoverClient(db int) *redis.Client {
	if db < 0 {
		db = 0
	}
	client, ok := r.failoverClients[db]
	if !ok {
		r.mutex.Lock()
		defer r.mutex.Unlock()
		client, ok = r.failoverClients[db]
		if !ok {
			client = r.newFailoverClient(db)
			r.failoverClients[db] = client
		}
	}
	return client
}

func (r *GoRedisConnect) newClusterClient() *redis.ClusterClient {
	redisOpts := &redis.ClusterOptions{
		Addrs:          r.hostsAndPorts,
		Password:       r.password,
		MaxRetries:     r.maxRetries,
		ReadOnly:       r.slaveReadOnly,
		RouteByLatency: true,
	}
	if r.poolOpts != nil {
		if r.poolOpts.DialTimeout > 0 {
			redisOpts.DialTimeout = r.poolOpts.DialTimeout
		}
		if r.poolOpts.ReadTimeout > 0 {
			redisOpts.ReadTimeout = r.poolOpts.ReadTimeout
		}
		if r.poolOpts.WriteTimeout > 0 {
			redisOpts.WriteTimeout = r.poolOpts.WriteTimeout
		}
		if r.poolOpts.PoolSize > 0 {
			redisOpts.PoolSize = r.poolOpts.PoolSize
		}
		if r.poolOpts.MinIdleConns > 0 {
			redisOpts.MinIdleConns = r.poolOpts.MinIdleConns
		} else {
			redisOpts.MinIdleConns = 1
		}
	}
	return redis.NewClusterClient(redisOpts)
}

// GetClusterClient returns the redis.ClusterClient associated with the GoRedisConnect.
//
// Note: do NOT close the returned client (e.g. call redis.ClusterClient.Close()).
//
// This function uses 'hostsAndPorts' config as list of Redis server addresses.
// This function returns the existing client, if any (i.e. no new redis.ClusterClient instance will be created).
func (r *GoRedisConnect) GetClusterClient() *redis.ClusterClient {
	if r.clusterClient == nil {
		r.mutex.Lock()
		defer r.mutex.Unlock()
		if r.clusterClient == nil {
			r.clusterClient = r.newClusterClient()
		}
	}
	return r.clusterClient
}
