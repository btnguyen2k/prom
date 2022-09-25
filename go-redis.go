package prom

import (
	"regexp"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// GoRedisConnect holds a go-redis client (https://github.com/go-redis/redis) that can be shared within the application.
type GoRedisConnect struct {
	hostsAndPorts []string // list of host:port addresses
	password      string   // password to authenticate against Redis server
	maxRetries    int      // max number of retries for failed operations

	poolOpts      *RedisPoolOpts // Redis connection pool options
	mutex         sync.Mutex
	metricsLogger IMetricsLogger // (since v0.3.0) if non-nil, GoRedisConnect automatically logs executing commands.

	/* other options */
	sentinelMasterName string                // the sentinel master name, used by failover clients
	slaveReadOnly      bool                  // enables read-only commands on slave nodes
	clients            map[int]*redis.Client // clients mapped with Redis databases
	failoverClients    map[int]*redis.Client // failover-clients mapped with Redis databases
	clusterClient      *redis.ClusterClient  // cluster-enabled client
}

// RedisPoolOpts holds options to configure Redis connection pool.
//
// Available: since v0.2.8
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
// Parameters: see NewGoRedisConnectWithPoolOptions
func NewGoRedisConnect(hostsAndPorts, password string, maxRetries int) (*GoRedisConnect, error) {
	return NewGoRedisConnectWithPoolOptions(hostsAndPorts, password, maxRetries, defaultRedisPoolOpts)
}

// NewGoRedisConnectWithPoolOptions constructs a new GoRedisConnect instance with supplied options and default Redis pool options.
//
// Parameters:
//   - hostsAndPorts: list of Redis servers (example: "host1:6379,host2;host3:6380")
//   - password     : password to authenticate against Redis server
//   - maxRetries   : max number of retries for failed operations
//   - poolOpts     : Redis connection pool settings
//
// Available since v0.2.8
func NewGoRedisConnectWithPoolOptions(hostsAndPorts, password string, maxRetries int, poolOpts *RedisPoolOpts) (*GoRedisConnect, error) {
	if maxRetries < 0 {
		maxRetries = 0
	}
	if poolOpts == nil {
		poolOpts = defaultRedisPoolOpts
	}
	r := &GoRedisConnect{
		hostsAndPorts:   regexp.MustCompile("[,;\\s]+").Split(hostsAndPorts, -1),
		password:        password,
		maxRetries:      maxRetries,
		slaveReadOnly:   true,
		clients:         make(map[int]*redis.Client),
		failoverClients: make(map[int]*redis.Client),
		poolOpts:        poolOpts,
		metricsLogger:   NewMemoryStoreMetricsLogger(1028),
	}
	return r, r.Init()
}

// Init should be called to initialize the GoRedisConnect instance before use.
//
// Available since v0.3.0
func (r *GoRedisConnect) Init() error {
	if r.clients != nil && r.failoverClients != nil {
		return nil
	}
	if r.maxRetries < 0 {
		r.maxRetries = 0
	}
	if r.poolOpts == nil {
		r.poolOpts = defaultRedisPoolOpts
	}
	if r.metricsLogger == nil {
		r.metricsLogger = NewMemoryStoreMetricsLogger(1028)
	}
	if r.clients == nil {
		r.clients = make(map[int]*redis.Client)
	}
	if r.failoverClients == nil {
		r.failoverClients = make(map[int]*redis.Client)
	}
	return nil
}

// RegisterMetricsLogger associates an IMetricsLogger instance with this GoRedisConnect.
// If non-nil, GoRedisConnect automatically logs executing commands.
//
// Available since v0.3.0
func (r *GoRedisConnect) RegisterMetricsLogger(metricsLogger IMetricsLogger) *GoRedisConnect {
	r.metricsLogger = metricsLogger
	return r
}

// MetricsLogger returns the associated IMetricsLogger instance.
//
// Available since v0.3.0
func (r *GoRedisConnect) MetricsLogger() IMetricsLogger {
	return r.metricsLogger
}

// NewCmdExecInfo is convenient function to create a new CmdExecInfo instance.
//
// The returned CmdExecInfo has its 'id' and 'begin-time' fields initialized.
//
// Available since v0.3.0
func (r *GoRedisConnect) NewCmdExecInfo() *CmdExecInfo {
	return &CmdExecInfo{
		Id:        NewId(),
		BeginTime: time.Now(),
		Cost:      -1,
	}
}

// LogMetrics is convenient function to put the CmdExecInfo to the metrics log.
//
// This function is silently no-op of the input if nil or there is no associated metrics logger.
//
// Available since v0.3.0
func (r *GoRedisConnect) LogMetrics(category string, cmd *CmdExecInfo) error {
	if cmd != nil && r.metricsLogger != nil {
		return r.metricsLogger.Put(category, cmd)
	}
	return nil
}

// Metrics is convenient function to capture the snapshot of command execution metrics.
//
// This function is silently no-op of there is no associated metrics logger.
//
// Available since v0.3.0
func (r *GoRedisConnect) Metrics(category string, opts ...MetricsOpts) (*Metrics, error) {
	if r.metricsLogger != nil {
		return r.metricsLogger.Metrics(category, opts...)
	}
	return nil, nil
}

// GetSlaveReadOnly returns the current value of 'slaveReadOnly' setting.
//
// Available: since v0.2.8
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
// Available: since v0.2.8
func (r *GoRedisConnect) GetSentinelMasterName() string {
	return r.sentinelMasterName
}

// SetSentinelMasterName sets the sentinel master name, used by failover clients.
//
// The change will apply to newly created clients, existing one will NOT be effected!
// This function returns the current GoRedisConnect instance so that function calls can be chained.
func (r *GoRedisConnect) SetSentinelMasterName(masterName string) *GoRedisConnect {
	r.sentinelMasterName = masterName
	return r
}

// GetRedisPoolOpts returns Redis connection pool configurations.
//
// Available: since v0.2.8
func (r *GoRedisConnect) GetRedisPoolOpts() *RedisPoolOpts {
	return r.poolOpts
}

// SetRedisPoolOpts sets Redis connection pool configurations.
//
// The change will apply to newly created clients, existing one will NOT be effected!
// This function returns the current GoRedisConnect instance so that function calls can be chained.
//
// Available: since v0.2.8
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
	r.clients = make(map[int]*redis.Client)
}

func (r *GoRedisConnect) closeFailoverClients() {
	if r.failoverClients != nil {
		for _, c := range r.failoverClients {
			c.Close()
		}
	}
	r.failoverClients = make(map[int]*redis.Client)
}

func (r *GoRedisConnect) closeClusterClient() {
	if r.clusterClient != nil {
		r.clusterClient.Close()
	}
	r.clusterClient = nil
}

// Close closes all underlying Redis connections associated with this GoRedisConnect.
//
// Available: since v0.2.0
func (r *GoRedisConnect) Close() error {
	go r.closeClients()
	go r.closeFailoverClients()
	go r.closeClusterClient()
	return nil
}

// availablie since v0.3.0
func (r *GoRedisConnect) newClientWithHostAndPort(hostAndPort string, db int) *redis.Client {
	if db < 0 {
		db = 0
	}
	redisOpts := &redis.Options{
		Addr:       hostAndPort,
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

func (r *GoRedisConnect) newClient(db int) *redis.Client {
	return r.newClientWithHostAndPort(r.hostsAndPorts[0], db)
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

// GetClientProxy is similar to GetClient, but returns a proxy that can be used as a replacement.
//
// Available since v0.3.0
func (r *GoRedisConnect) GetClientProxy(db int) *RedisClientProxy {
	proxy := &RedisClientProxy{
		CmdableWrapper: CmdableWrapper{
			Cmdable: r.GetClient(db),
			rc:      r,
		},
	}
	return proxy
}

func (r *GoRedisConnect) newFailoverClient(db int) *redis.Client {
	if db < 0 {
		db = 0
	}
	redisOpts := &redis.FailoverOptions{
		MasterName:    r.sentinelMasterName,
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

// GetFailoverClientProxy is similar to GetFailoverClient, but returns a proxy that can be used as a replacement.
//
// Available since v0.3.0
func (r *GoRedisConnect) GetFailoverClientProxy(db int) *RedisFailoverClientProxy {
	proxy := &RedisFailoverClientProxy{
		RedisClientProxy: RedisClientProxy{
			CmdableWrapper: CmdableWrapper{
				Cmdable: r.GetFailoverClient(db),
				rc:      r,
			},
		},
	}
	return proxy
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

// GetClusterClientProxy is similar to GetClusterClient, but returns a proxy that can be used as a replacement.
//
// Available since v0.3.0
func (r *GoRedisConnect) GetClusterClientProxy() *RedisClusterClientProxy {
	proxy := &RedisClusterClientProxy{
		CmdableWrapper: CmdableWrapper{
			Cmdable: r.GetClusterClient(),
			rc:      r,
		},
	}
	return proxy
}
