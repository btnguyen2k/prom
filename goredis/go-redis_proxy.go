package goredis

import (
	"context"
	"time"

	"github.com/btnguyen2k/prom"
	"github.com/redis/go-redis/v9"
)

type m map[string]interface{}

// RedisClientProxy is a proxy that can be used as replacement for redis.Client.
//
// This proxy overrides some functions from redis.Client and automatically logs the execution metrics.
//
// Available since v0.3.0
type RedisClientProxy struct {
	CmdableWrapper
}

// Wait overrides redis.Client/Wait to log execution metrics.
func (cp *RedisClientProxy) Wait(ctx context.Context, numSlaves int, timeout time.Duration) *redis.IntCmd {
	cmd := cp.rc.NewCmdExecInfo()
	defer func() {
		cp.rc.LogMetrics(prom.MetricsCatAll, cmd)
		cp.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "wait", m{"numSlaves": numSlaves, "timeout": timeout}
	result := cp.Cmdable.(*redis.Client).Wait(ctx, numSlaves, timeout)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// PSubscribe overrides redis.Client/PSubscribe to log execution metrics.
func (cp *RedisClientProxy) PSubscribe(ctx context.Context, channels ...string) *redis.PubSub {
	cmd := cp.rc.NewCmdExecInfo()
	defer func() {
		cp.rc.LogMetrics(prom.MetricsCatAll, cmd)
		cp.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "psubscribe", m{"channels": channels}
	result := cp.Cmdable.(*redis.Client).PSubscribe(ctx, channels...)
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, nil)
	return result
}

// Subscribe overrides redis.Client/Subscribe to log execution metrics.
func (cp *RedisClientProxy) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	cmd := cp.rc.NewCmdExecInfo()
	defer func() {
		cp.rc.LogMetrics(prom.MetricsCatAll, cmd)
		cp.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "subscribe", m{"channels": channels}
	result := cp.Cmdable.(*redis.Client).Subscribe(ctx, channels...)
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, nil)
	return result
}

/*----------------------------------------------------------------------*/

// RedisFailoverClientProxy is a proxy that can be used as replacement for fail-over redis.Client.
//
// This proxy overrides some functions from fail-over redis.Client and automatically logs the execution metrics.
//
// Available since v0.3.0
type RedisFailoverClientProxy struct {
	RedisClientProxy
}

/*----------------------------------------------------------------------*/

// RedisClusterClientProxy is a proxy that can be used as replacement for redis.ClusterClient.
//
// This proxy overrides some functions from redis.ClusterClient and automatically logs the execution metrics.
//
// Available since v0.3.0
type RedisClusterClientProxy struct {
	CmdableWrapper
}

// Wait overrides redis.ClusterClient/Wait to log execution metrics.
func (cp *RedisClusterClientProxy) Wait(ctx context.Context, numSlaves int, timeout time.Duration) *redis.IntCmd {
	cmd := cp.rc.NewCmdExecInfo()
	defer func() {
		cp.rc.LogMetrics(prom.MetricsCatAll, cmd)
		cp.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "wait", m{"numSlaves": numSlaves, "timeout": timeout}
	result := cp.Cmdable.(*redis.ClusterClient).Wait(ctx, numSlaves, timeout)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// PSubscribe overrides redis.ClusterClient/PSubscribe to log execution metrics.
func (cp *RedisClusterClientProxy) PSubscribe(ctx context.Context, channels ...string) *redis.PubSub {
	cmd := cp.rc.NewCmdExecInfo()
	defer func() {
		cp.rc.LogMetrics(prom.MetricsCatAll, cmd)
		cp.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "psubscribe", m{"channels": channels}
	result := cp.Cmdable.(*redis.ClusterClient).PSubscribe(ctx, channels...)
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, nil)
	return result
}

// Subscribe overrides redis.ClusterClient/Subscribe to log execution metrics.
func (cp *RedisClusterClientProxy) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	cmd := cp.rc.NewCmdExecInfo()
	defer func() {
		cp.rc.LogMetrics(prom.MetricsCatAll, cmd)
		cp.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "subscribe", m{"channels": channels}
	result := cp.Cmdable.(*redis.ClusterClient).Subscribe(ctx, channels...)
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, nil)
	return result
}

/*----------------------------------------------------------------------*/

// CmdableWrapper is a wrapper for redis.Cmdable; overrides redis.Cmdable's function to log execution metrics.
type CmdableWrapper struct {
	redis.Cmdable
	rc *GoRedisConnect
}

/*----- Bitmap-related commands -----*/

// BitCount overrides redis.Cmdable/BitCount to log execution metrics.
//
// @Redis: available since v2.6.0
func (c *CmdableWrapper) BitCount(ctx context.Context, key string, bitCount *redis.BitCount) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "bitCount", m{"key": key, "args": bitCount}
	result := c.Cmdable.BitCount(ctx, key, bitCount)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// BitField overrides redis.Cmdable/BitField to log execution metrics.
//
// @Redis: available since v3.2.0
func (c *CmdableWrapper) BitField(ctx context.Context, key string, args ...interface{}) *redis.IntSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "bitField", m{"key": key, "args": args}
	result := c.Cmdable.BitField(ctx, key, args...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// BitOpAnd overrides redis.Cmdable/BitOpAnd to log execution metrics.
//
// @Redis: available since v2.6.0
func (c *CmdableWrapper) BitOpAnd(ctx context.Context, destKey string, keys ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "bitOp", m{"op": "and", "destKey": destKey, "keys": keys}
	result := c.Cmdable.BitOpAnd(ctx, destKey, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// BitOpOr overrides redis.Cmdable/BitOpOr to log execution metrics.
//
// @Redis: available since v2.6.0
func (c *CmdableWrapper) BitOpOr(ctx context.Context, destKey string, keys ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "bitOp", m{"op": "or", "destKey": destKey, "keys": keys}
	result := c.Cmdable.BitOpOr(ctx, destKey, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// BitOpXor overrides redis.Cmdable/BitOpXor to log execution metrics.
//
// @Redis: available since v2.6.0
func (c *CmdableWrapper) BitOpXor(ctx context.Context, destKey string, keys ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "bitOp", m{"op": "xor", "destKey": destKey, "keys": keys}
	result := c.Cmdable.BitOpXor(ctx, destKey, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// BitOpNot overrides redis.Cmdable/BitOpNot to log execution metrics.
//
// @Redis: available since v2.6.0
func (c *CmdableWrapper) BitOpNot(ctx context.Context, destKey, key string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "bitOp", m{"op": "not", "destKey": destKey, "key": key}
	result := c.Cmdable.BitOpNot(ctx, destKey, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// BitPos overrides redis.Cmdable/BitPos to log execution metrics.
//
// @Redis: available since v2.8.7
func (c *CmdableWrapper) BitPos(ctx context.Context, key string, bit int64, pos ...int64) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "bitPos", m{"key": key, "bit": bit, "pos": pos}
	result := c.Cmdable.BitPos(ctx, key, bit, pos...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// GetBit overrides redis.Cmdable/GetBit to log execution metrics.
//
// @Redis: available since v2.2.0
func (c *CmdableWrapper) GetBit(ctx context.Context, key string, offset int64) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "getBit", m{"key": key, "offset": offset}
	result := c.Cmdable.GetBit(ctx, key, offset)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SetBit overrides redis.Cmdable/SetBit to log execution metrics.
//
// @Redis: available since v2.2.0
func (c *CmdableWrapper) SetBit(ctx context.Context, key string, offset int64, value int) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "setBit", m{"key": key, "offset": offset, "value": value}
	result := c.Cmdable.SetBit(ctx, key, offset, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

/*----- Cluster-related commands -----*/

// ReadOnly overrides redis.Cmdable/ReadOnly to log execution metrics.
//
// @Redis: available since v3.0.0
func (c *CmdableWrapper) ReadOnly(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "readOnly", nil
	result := c.Cmdable.ReadOnly(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ReadWrite overrides redis.Cmdable/ReadWrite to log execution metrics.
//
// @Redis: available since v3.0.0
func (c *CmdableWrapper) ReadWrite(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "readWrite", nil
	result := c.Cmdable.ReadWrite(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

/* Generic commands */

// Copy overrides redis.Cmdable/Copy to log execution metrics.
//
// @Redis: available since v6.2.0
func (c *CmdableWrapper) Copy(ctx context.Context, srcKey, destKey string, destDb int, replace bool) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "copy", m{"srcKey": srcKey, "destKey": destKey, "destDb": destDb, "replace": replace}
	result := c.Cmdable.Copy(ctx, srcKey, destKey, destDb, replace)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Del overrides redis.Cmdable/Del to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "del", m{"keys": keys}
	result := c.Cmdable.Del(ctx, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Dump overrides redis.Cmdable/Dump to log execution metrics.
//
// @Redis: available since v2.6.0
func (c *CmdableWrapper) Dump(ctx context.Context, key string) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "dump", m{"key": key}
	result := c.Cmdable.Dump(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Exists overrides redis.Cmdable/Exists to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) Exists(ctx context.Context, keys ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "exists", m{"keys": keys}
	result := c.Cmdable.Exists(ctx, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Expire overrides redis.Cmdable/Expire to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) Expire(ctx context.Context, key string, value time.Duration) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "expire", m{"key": key, "value": value}
	result := c.Cmdable.Expire(ctx, key, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ExpireGT overrides redis.Cmdable/ExpireGT to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) ExpireGT(ctx context.Context, key string, value time.Duration) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "expire_gt", m{"key": key, "value": value}
	result := c.Cmdable.ExpireGT(ctx, key, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ExpireLT overrides redis.Cmdable/ExpireLT to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) ExpireLT(ctx context.Context, key string, value time.Duration) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "expire_lt", m{"key": key, "value": value}
	result := c.Cmdable.ExpireLT(ctx, key, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ExpireNX overrides redis.Cmdable/ExpireNX to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) ExpireNX(ctx context.Context, key string, value time.Duration) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "expire_nx", m{"key": key, "value": value}
	result := c.Cmdable.ExpireNX(ctx, key, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ExpireXX overrides redis.Cmdable/ExpireXX to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) ExpireXX(ctx context.Context, key string, value time.Duration) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "expire_xx", m{"key": key, "value": value}
	result := c.Cmdable.ExpireXX(ctx, key, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ExpireAt overrides redis.Cmdable.ExpireAt to log execution metrics.
func (c *CmdableWrapper) ExpireAt(ctx context.Context, key string, value time.Time) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "expireAt", m{"key": key, "value": value}
	result := c.Cmdable.ExpireAt(ctx, key, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Keys overrides redis.Cmdable.Keys to log execution metrics.
func (c *CmdableWrapper) Keys(ctx context.Context, pattern string) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "keys", m{"pattern": pattern}
	result := c.Cmdable.Keys(ctx, pattern)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Migrate overrides redis.Cmdable.Migrate to log execution metrics.
func (c *CmdableWrapper) Migrate(ctx context.Context, host, port, key string, db int, timeout time.Duration) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "migrate", m{"host": host, "port": port, "key": key, "db": db, "timeout": timeout}
	result := c.Cmdable.Migrate(ctx, host, port, key, db, timeout)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Move overrides redis.Cmdable.Move to log execution metrics.
func (c *CmdableWrapper) Move(ctx context.Context, key string, db int) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "move", m{"key": key, "db": db}
	result := c.Cmdable.Move(ctx, key, db)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ObjectEncoding overrides redis.Cmdable.ObjectEncoding to log execution metrics.
func (c *CmdableWrapper) ObjectEncoding(ctx context.Context, key string) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "objectEncoding", m{"key": key}
	result := c.Cmdable.ObjectEncoding(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// No function ObjectFreq for now!

// ObjectIdleTime overrides redis.Cmdable.ObjectIdleTime to log execution metrics.
func (c *CmdableWrapper) ObjectIdleTime(ctx context.Context, key string) *redis.DurationCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "objectIdleTime", m{"key": key}
	result := c.Cmdable.ObjectIdleTime(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ObjectRefCount overrides redis.Cmdable.ObjectRefCount to log execution metrics.
func (c *CmdableWrapper) ObjectRefCount(ctx context.Context, key string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "objectRefCount", m{"key": key}
	result := c.Cmdable.ObjectRefCount(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Persist overrides redis.Cmdable.Persist to log execution metrics.
func (c *CmdableWrapper) Persist(ctx context.Context, key string) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "persist", m{"key": key}
	result := c.Cmdable.Persist(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// PExpire overrides redis.Cmdable.PExpire to log execution metrics.
func (c *CmdableWrapper) PExpire(ctx context.Context, key string, value time.Duration) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "pexpire", m{"key": key, "value": value}
	result := c.Cmdable.PExpire(ctx, key, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// PExpireAt overrides redis.Cmdable.PExpireAt to log execution metrics.
func (c *CmdableWrapper) PExpireAt(ctx context.Context, key string, value time.Time) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "pexpireAt", m{"key": key, "value": value}
	result := c.Cmdable.PExpireAt(ctx, key, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Ping overrides redis.Cmdable.Ping to log execution metrics.
func (c *CmdableWrapper) Ping(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "ping", nil
	result := c.Cmdable.Ping(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// PTTL overrides redis.Cmdable.PTTL to log execution metrics.
func (c *CmdableWrapper) PTTL(ctx context.Context, key string) *redis.DurationCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "pttl", m{"key": key}
	result := c.Cmdable.PTTL(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// RandomKey overrides redis.Cmdable.RandomKey to log execution metrics.
func (c *CmdableWrapper) RandomKey(ctx context.Context) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "randomKey", nil
	result := c.Cmdable.RandomKey(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Rename overrides redis.Cmdable.Rename to log execution metrics.
func (c *CmdableWrapper) Rename(ctx context.Context, key, newKey string) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "rename", m{"key": key, "newKey": newKey}
	result := c.Cmdable.Rename(ctx, key, newKey)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// RenameNX overrides redis.Cmdable.RenameNX to log execution metrics.
func (c *CmdableWrapper) RenameNX(ctx context.Context, key, newKey string) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "renamenx", m{"key": key, "newKey": newKey}
	result := c.Cmdable.RenameNX(ctx, key, newKey)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Restore overrides redis.Cmdable.Restore to log execution metrics.
func (c *CmdableWrapper) Restore(ctx context.Context, key string, ttl time.Duration, value string) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "restore", m{"key": key, "ttl": ttl, "value": value}
	result := c.Cmdable.Restore(ctx, key, ttl, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Scan overrides redis.Cmdable.Scan to log execution metrics.
func (c *CmdableWrapper) Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "scan", m{"cursor": cursor, "match": match, "count": count}
	result := c.Cmdable.Scan(ctx, cursor, match, count)
	val, _, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Sort overrides redis.Cmdable.Sort to log execution metrics.
func (c *CmdableWrapper) Sort(ctx context.Context, key string, sort *redis.Sort) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "sort", m{"key": key, "args": sort}
	result := c.Cmdable.Sort(ctx, key, sort)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Touch overrides redis.Cmdable.Touch to log execution metrics.
func (c *CmdableWrapper) Touch(ctx context.Context, keys ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "touch", m{"keys": keys}
	result := c.Cmdable.Touch(ctx, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// TTL overrides redis.Cmdable.TTL to log execution metrics.
func (c *CmdableWrapper) TTL(ctx context.Context, key string) *redis.DurationCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "ttl", m{"key": key}
	result := c.Cmdable.TTL(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Type overrides redis.Cmdable.Type to log execution metrics.
func (c *CmdableWrapper) Type(ctx context.Context, key string) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "type", m{"key": key}
	result := c.Cmdable.Type(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Unlink overrides redis.Cmdable.Unlink to log execution metrics.
func (c *CmdableWrapper) Unlink(ctx context.Context, keys ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "unlink", m{"keys": keys}
	result := c.Cmdable.Unlink(ctx, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Function Wait is overridden by each proxy!

/*----- Geospatial-related commands -----*/

func (c *CmdableWrapper) GeoAdd(ctx context.Context, key string, geoLocations ...*redis.GeoLocation) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "geoAdd", m{"key": key, "locations": geoLocations}
	result := c.Cmdable.GeoAdd(ctx, key, geoLocations...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

func (c *CmdableWrapper) GeoDist(ctx context.Context, key, member1, member2, unit string) *redis.FloatCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "geoDist", m{"key": key, "member1": member1, "member2": member2, "unit": unit}
	result := c.Cmdable.GeoDist(ctx, key, member1, member2, unit)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

func (c *CmdableWrapper) GeoHash(ctx context.Context, key string, members ...string) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "geoHash", m{"key": key, "members": members}
	result := c.Cmdable.GeoHash(ctx, key, members...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

func (c *CmdableWrapper) GeoPos(ctx context.Context, key string, members ...string) *redis.GeoPosCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "geoPos", m{"key": key, "members": members}
	result := c.Cmdable.GeoPos(ctx, key, members...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Functions GeoRadius, GeoRadius_RO, GeoRadiusByMember and GeoRadiusByMember_RO are deprecated!

func (c *CmdableWrapper) GeoSearch(ctx context.Context, key string, query *redis.GeoSearchQuery) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "geoSearch", m{"key": key, "query": query}
	result := c.Cmdable.GeoSearch(ctx, key, query)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

func (c *CmdableWrapper) GeoSearchLocation(ctx context.Context, key string, query *redis.GeoSearchLocationQuery) *redis.GeoSearchLocationCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "geoSearchLocation", m{"key": key, "query": query}
	result := c.Cmdable.GeoSearchLocation(ctx, key, query)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

func (c *CmdableWrapper) GeoSearchStore(ctx context.Context, key, store string, query *redis.GeoSearchStoreQuery) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "geoSearchStore", m{"key": key, "store": store, "query": query}
	result := c.Cmdable.GeoSearchStore(ctx, key, store, query)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

/*----- Hash-related commands -----*/

// HDel overrides redis.Cmdable.HDel to log execution metrics.
func (c *CmdableWrapper) HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hdel", m{"key": key, "fields": fields}
	result := c.Cmdable.HDel(ctx, key, fields...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// HExists overrides redis.Cmdable.HExists to log execution metrics.
func (c *CmdableWrapper) HExists(ctx context.Context, key, field string) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hexists", m{"key": key, "field": field}
	result := c.Cmdable.HExists(ctx, key, field)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// HGet overrides redis.Cmdable.HGet to log execution metrics.
func (c *CmdableWrapper) HGet(ctx context.Context, key, field string) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hget", m{"key": key, "field": field}
	result := c.Cmdable.HGet(ctx, key, field)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// HGetAll overrides redis.Cmdable.HGetAll to log execution metrics.
func (c *CmdableWrapper) HGetAll(ctx context.Context, key string) *redis.StringStringMapCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hgetAll", m{"key": key}
	result := c.Cmdable.HGetAll(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// HIncrBy overrides redis.Cmdable.HIncrBy to log execution metrics.
func (c *CmdableWrapper) HIncrBy(ctx context.Context, key, field string, value int64) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hincrBy", m{"key": key, "field": field, "value": value}
	result := c.Cmdable.HIncrBy(ctx, key, field, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// HIncrByFloat overrides redis.Cmdable.HIncrByFloat to log execution metrics.
func (c *CmdableWrapper) HIncrByFloat(ctx context.Context, key, field string, value float64) *redis.FloatCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hincrByFloat", m{"key": key, "field": field, "value": value}
	result := c.Cmdable.HIncrByFloat(ctx, key, field, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// HKeys overrides redis.Cmdable.HKeys to log execution metrics.
func (c *CmdableWrapper) HKeys(ctx context.Context, key string) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hkeys", m{"key": key}
	result := c.Cmdable.HKeys(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// HLen overrides redis.Cmdable.HLen to log execution metrics.
func (c *CmdableWrapper) HLen(ctx context.Context, key string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hlen", m{"key": key}
	result := c.Cmdable.HLen(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// HMGet overrides redis.Cmdable.HMGet to log execution metrics.
func (c *CmdableWrapper) HMGet(ctx context.Context, key string, fields ...string) *redis.SliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hmget", m{"key": key, "fields": fields}
	result := c.Cmdable.HMGet(ctx, key, fields...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Function HMSet is deprecated!

// HRandField overrides redis.Cmdable.HRandField to log execution metrics.
func (c *CmdableWrapper) HRandField(ctx context.Context, key string, count int, withValues bool) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hrandField", m{"key": key, "count": count, "withValues": withValues}
	result := c.Cmdable.HRandField(ctx, key, count, withValues)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// HScan overrides redis.Cmdable.HScan to log execution metrics.
func (c *CmdableWrapper) HScan(ctx context.Context, key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hscan", m{"key": key, "cursor": cursor, "match": match, "count": count}
	result := c.Cmdable.HScan(ctx, key, cursor, match, count)
	val, _, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// HSet overrides redis.Cmdable.HSet to log execution metrics.
func (c *CmdableWrapper) HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hset", m{"key": key, "values": values}
	result := c.Cmdable.HSet(ctx, key, values...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// HSetNX overrides redis.Cmdable.HSetNX to log execution metrics.
func (c *CmdableWrapper) HSetNX(ctx context.Context, key, field string, value interface{}) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hsetnx", m{"key": key, "field": field, "value": value}
	result := c.Cmdable.HSetNX(ctx, key, field, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// No function HStrLen for now!

// HVals overrides redis.Cmdable.HVals to log execution metrics.
func (c *CmdableWrapper) HVals(ctx context.Context, key string) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hvals", m{"key": key}
	result := c.Cmdable.HVals(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

/*----- HyperLogLog-related commands -----*/

// PFAdd overrides redis.Cmdable.PFAdd to log execution metrics.
func (c *CmdableWrapper) PFAdd(ctx context.Context, key string, elements ...interface{}) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "pfadd", m{"key": key, "elements": elements}
	result := c.Cmdable.PFAdd(ctx, key, elements...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// PFCount overrides redis.Cmdable.PFCount to log execution metrics.
func (c *CmdableWrapper) PFCount(ctx context.Context, keys ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "pfcount", m{"keys": keys}
	result := c.Cmdable.PFCount(ctx, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// PFMerge overrides redis.Cmdable.PFMerge to log execution metrics.
func (c *CmdableWrapper) PFMerge(ctx context.Context, destKey string, keys ...string) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "pfmerge", m{"destKey": destKey, "keys": keys}
	result := c.Cmdable.PFMerge(ctx, destKey, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

/*----- List-related commands -----*/

// BLMove overrides redis.Cmdable.BLMove to log execution metrics.
func (c *CmdableWrapper) BLMove(ctx context.Context, srcKey, destKey, srcPos, destPos string, timeout time.Duration) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "blmove", m{"srcKey": srcKey, "destKey": destKey, "srcPos": srcPos, "destPos": destPos, "timeout": timeout}
	result := c.Cmdable.BLMove(ctx, srcKey, destKey, srcPos, destPos, timeout)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// BLPop overrides redis.Cmdable.BLPop to log execution metrics.
func (c *CmdableWrapper) BLPop(ctx context.Context, timeout time.Duration, keys ...string) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "blpop", m{"keys": keys, "timeout": timeout}
	result := c.Cmdable.BLPop(ctx, timeout, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// BRPop overrides redis.Cmdable.BRPop to log execution metrics.
func (c *CmdableWrapper) BRPop(ctx context.Context, timeout time.Duration, keys ...string) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "brpop", m{"keys": keys, "timeout": timeout}
	result := c.Cmdable.BRPop(ctx, timeout, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Function BRPopLPush is deprecated!

// LIndex overrides redis.Cmdable.LIndex to log execution metrics.
func (c *CmdableWrapper) LIndex(ctx context.Context, key string, index int64) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "lindex", m{"key": key, "index": index}
	result := c.Cmdable.LIndex(ctx, key, index)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// LInsert overrides redis.Cmdable.LInsert to log execution metrics.
func (c *CmdableWrapper) LInsert(ctx context.Context, key, op string, pivot, value interface{}) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "linsert", m{"key": key, "op": op, "pivot": pivot, "value": value}
	result := c.Cmdable.LInsert(ctx, key, op, pivot, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// LLen overrides redis.Cmdable.LLen to log execution metrics.
func (c *CmdableWrapper) LLen(ctx context.Context, key string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "llen", m{"key": key}
	result := c.Cmdable.LLen(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// LMove overrides redis.Cmdable.LMove to log execution metrics.
func (c *CmdableWrapper) LMove(ctx context.Context, srcKey, destKey, srcPos, destPos string) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "lmove", m{"srcKey": srcKey, "destKey": destKey, "srcPos": srcPos, "destPos": destPos}
	result := c.Cmdable.LMove(ctx, srcKey, destKey, srcPos, destPos)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// LPop overrides redis.Cmdable.LPop to log execution metrics.
func (c *CmdableWrapper) LPop(ctx context.Context, key string) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "lpop", m{"key": key}
	result := c.Cmdable.LPop(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// LPos overrides redis.Cmdable.LPos to log execution metrics.
func (c *CmdableWrapper) LPos(ctx context.Context, key, value string, args redis.LPosArgs) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "lpos", m{"key": key, "value": value, "args": args}
	result := c.Cmdable.LPos(ctx, key, value, args)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// LPush overrides redis.Cmdable.LPush to log execution metrics.
func (c *CmdableWrapper) LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "lpush", m{"key": key, "values": values}
	result := c.Cmdable.LPush(ctx, key, values...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// LPushX overrides redis.Cmdable.LPushX to log execution metrics.
func (c *CmdableWrapper) LPushX(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "lpushx", m{"key": key, "values": values}
	result := c.Cmdable.LPushX(ctx, key, values...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// LRange overrides redis.Cmdable.LRange to log execution metrics.
func (c *CmdableWrapper) LRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "lrange", m{"key": key, "start": start, "stop": stop}
	result := c.Cmdable.LRange(ctx, key, start, stop)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// LRem overrides redis.Cmdable.LRem to log execution metrics.
func (c *CmdableWrapper) LRem(ctx context.Context, key string, count int64, value interface{}) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "lrem", m{"key": key, "count": count, "value": value}
	result := c.Cmdable.LRem(ctx, key, count, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// LSet overrides redis.Cmdable.LSet to log execution metrics.
func (c *CmdableWrapper) LSet(ctx context.Context, key string, index int64, value interface{}) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "lset", m{"key": key, "index": index, "value": value}
	result := c.Cmdable.LSet(ctx, key, index, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// LTrim overrides redis.Cmdable.LTrim to log execution metrics.
func (c *CmdableWrapper) LTrim(ctx context.Context, key string, start, stop int64) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "ltrim", m{"key": key, "start": start, "stop": stop}
	result := c.Cmdable.LTrim(ctx, key, start, stop)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// RPop overrides redis.Cmdable.RPop to log execution metrics.
func (c *CmdableWrapper) RPop(ctx context.Context, key string) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "rpop", m{"key": key}
	result := c.Cmdable.RPop(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Function RPopLPush is deprecated!

// RPush overrides redis.Cmdable.RPush to log execution metrics.
func (c *CmdableWrapper) RPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "rpush", m{"key": key, "values": values}
	result := c.Cmdable.RPush(ctx, key, values...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// RPushX overrides redis.Cmdable.RPushX to log execution metrics.
func (c *CmdableWrapper) RPushX(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "rpushx", m{"key": key, "values": values}
	result := c.Cmdable.RPushX(ctx, key, values...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

/*----- Pub/Sub-related commands -----*/

// Function PSubscribe is overridden by each proxy!

// Publish overrides redis.Cmdable.Publish to log execution metrics.
func (c *CmdableWrapper) Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "publish", m{"channel": channel, "message": message}
	result := c.Cmdable.Publish(ctx, channel, message)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// PubSubChannels overrides redis.Cmdable.PubSubChannels to log execution metrics.
func (c *CmdableWrapper) PubSubChannels(ctx context.Context, pattern string) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "pubSubChannels", m{"pattern": pattern}
	result := c.Cmdable.PubSubChannels(ctx, pattern)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// PubSubNumPat overrides redis.Cmdable.PubSubNumPat to log execution metrics.
func (c *CmdableWrapper) PubSubNumPat(ctx context.Context) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "pubSubNumPat", nil
	result := c.Cmdable.PubSubNumPat(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// PubSubNumSub overrides redis.Cmdable.PubSubNumSub to log execution metrics.
func (c *CmdableWrapper) PubSubNumSub(ctx context.Context, channels ...string) *redis.StringIntMapCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "pubSubNumSub", m{"channels": channels}
	result := c.Cmdable.PubSubNumSub(ctx, channels...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Function Subscribe is overridden by each proxy!

/*----- Scripting-related commands -----*/

// Eval overrides redis.Cmdable.Eval to log execution metrics.
func (c *CmdableWrapper) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "eval", m{"script": script, "keys": keys, "args": args}
	result := c.Cmdable.Eval(ctx, script, keys, args...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// EvalSha overrides redis.Cmdable.EvalSha to log execution metrics.
func (c *CmdableWrapper) EvalSha(ctx context.Context, scriptSha string, keys []string, args ...interface{}) *redis.Cmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "evalSha", m{"sha": scriptSha, "keys": keys, "args": args}
	result := c.Cmdable.EvalSha(ctx, scriptSha, keys, args...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// No function ScriptLoad for now!

// ScriptExists overrides redis.Cmdable.ScriptExists to log execution metrics.
func (c *CmdableWrapper) ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "scriptExists", m{"hashes": hashes}
	result := c.Cmdable.ScriptExists(ctx, hashes...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ScriptFlush overrides redis.Cmdable.ScriptFlush to log execution metrics.
func (c *CmdableWrapper) ScriptFlush(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "scriptFlush", nil
	result := c.Cmdable.ScriptFlush(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ScriptKill overrides redis.Cmdable.ScriptKill to log execution metrics.
func (c *CmdableWrapper) ScriptKill(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "scriptKill", nil
	result := c.Cmdable.ScriptKill(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ScriptLoad overrides redis.Cmdable.ScriptLoad to log execution metrics.
func (c *CmdableWrapper) ScriptLoad(ctx context.Context, script string) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "scriptLoad", m{"script": script}
	result := c.Cmdable.ScriptLoad(ctx, script)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

/*----- Server-related commands -----*/

// DBSize overrides redis.Cmdable.DBSize to log execution metrics.
func (c *CmdableWrapper) DBSize(ctx context.Context) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "dbsize", nil
	result := c.Cmdable.DBSize(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// FlushAll overrides redis.Cmdable.FlushAll to log execution metrics.
func (c *CmdableWrapper) FlushAll(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "flushAll", nil
	result := c.Cmdable.FlushAll(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// FlushAllAsync overrides redis.Cmdable.FlushAllAsync to log execution metrics.
func (c *CmdableWrapper) FlushAllAsync(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "flushAllAsync", nil
	result := c.Cmdable.FlushAllAsync(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// FlushDB overrides redis.Cmdable.FlushDB to log execution metrics.
func (c *CmdableWrapper) FlushDB(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "flushDb", nil
	result := c.Cmdable.FlushDB(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// FlushDBAsync overrides redis.Cmdable.FlushDBAsync to log execution metrics.
func (c *CmdableWrapper) FlushDBAsync(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "flushDbAsync", nil
	result := c.Cmdable.FlushDBAsync(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

/*----- Set-related commands -----*/

// No function SAdd for now!

// SAdd overrides redis.Cmdable.SAdd to log execution metrics.
func (c *CmdableWrapper) SAdd(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "sadd", m{"key": key, "values": values}
	result := c.Cmdable.SAdd(ctx, key, values...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SCard overrides redis.Cmdable.SCard to log execution metrics.
func (c *CmdableWrapper) SCard(ctx context.Context, key string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "scard", m{"key": key}
	result := c.Cmdable.SCard(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SDiff overrides redis.Cmdable.SDiff to log execution metrics.
func (c *CmdableWrapper) SDiff(ctx context.Context, keys ...string) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "sdiff", m{"keys": keys}
	result := c.Cmdable.SDiff(ctx, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SDiffStore overrides redis.Cmdable.SDiffStore to log execution metrics.
func (c *CmdableWrapper) SDiffStore(ctx context.Context, destKey string, keys ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "sdiffStore", m{"dest": destKey, "keys": keys}
	result := c.Cmdable.SDiffStore(ctx, destKey, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SInter overrides redis.Cmdable.SInter to log execution metrics.
func (c *CmdableWrapper) SInter(ctx context.Context, keys ...string) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "sinter", m{"keys": keys}
	result := c.Cmdable.SInter(ctx, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SInterStore overrides redis.Cmdable.SInterStore to log execution metrics.
func (c *CmdableWrapper) SInterStore(ctx context.Context, destKey string, keys ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "sinterStore", m{"dest": destKey, "keys": keys}
	result := c.Cmdable.SInterStore(ctx, destKey, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SIsMember overrides redis.Cmdable.SIsMember to log execution metrics.
func (c *CmdableWrapper) SIsMember(ctx context.Context, key string, value interface{}) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "sisMember", m{"key": key, "value": value}
	result := c.Cmdable.SIsMember(ctx, key, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SMembers overrides redis.Cmdable.SMembers to log execution metrics.
func (c *CmdableWrapper) SMembers(ctx context.Context, key string) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "smembers", m{"key": key}
	result := c.Cmdable.SMembers(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SMIsMember overrides redis.Cmdable.SMIsMember to log execution metrics.
func (c *CmdableWrapper) SMIsMember(ctx context.Context, key string, values ...interface{}) *redis.BoolSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "smisMember", m{"key": key, "values": values}
	result := c.Cmdable.SMIsMember(ctx, key, values...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SMove overrides redis.Cmdable.SMove to log execution metrics.
func (c *CmdableWrapper) SMove(ctx context.Context, srcKey, destKey string, value interface{}) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "smove", m{"src": srcKey, "dest": destKey, "value": value}
	result := c.Cmdable.SMove(ctx, srcKey, destKey, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SPop overrides redis.Cmdable.SPop to log execution metrics.
func (c *CmdableWrapper) SPop(ctx context.Context, key string) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "spop", m{"key": key}
	result := c.Cmdable.SPop(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SPopN overrides redis.Cmdable.SPopN to log execution metrics.
func (c *CmdableWrapper) SPopN(ctx context.Context, key string, count int64) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "spop", m{"key": key, "count": count}
	result := c.Cmdable.SPopN(ctx, key, count)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SRandMember overrides redis.Cmdable.SRandMember to log execution metrics.
func (c *CmdableWrapper) SRandMember(ctx context.Context, key string) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "srandMember", m{"key": key}
	result := c.Cmdable.SRandMember(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SRandMemberN overrides redis.Cmdable.SRandMemberN to log execution metrics.
func (c *CmdableWrapper) SRandMemberN(ctx context.Context, key string, count int64) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "srandMember", m{"key": key, "count": count}
	result := c.Cmdable.SRandMemberN(ctx, key, count)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SRem overrides redis.Cmdable.SRem to log execution metrics.
func (c *CmdableWrapper) SRem(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "srem", m{"key": key, "values": values}
	result := c.Cmdable.SRem(ctx, key, values...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SScan overrides redis.Cmdable.SScan to log execution metrics.
func (c *CmdableWrapper) SScan(ctx context.Context, key string, cursor uint64, pattern string, count int64) *redis.ScanCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "sscan", m{"key": key, "cursor": cursor, "pattern": pattern, "count": count}
	result := c.Cmdable.SScan(ctx, key, cursor, pattern, count)
	val, _, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SUnion overrides redis.Cmdable.SUnion to log execution metrics.
func (c *CmdableWrapper) SUnion(ctx context.Context, keys ...string) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "sunion", m{"keys": keys}
	result := c.Cmdable.SUnion(ctx, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SUnionStore overrides redis.Cmdable.SUnionStore to log execution metrics.
func (c *CmdableWrapper) SUnionStore(ctx context.Context, destKey string, keys ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "sunionStore", m{"dest": destKey, "keys": keys}
	result := c.Cmdable.SUnionStore(ctx, destKey, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

/*----- Sorted set-related commands -----*/

// BZPopMax overrides redis.Cmdable.BZPopMax to log execution metrics.
func (c *CmdableWrapper) BZPopMax(ctx context.Context, timeout time.Duration, keys ...string) *redis.ZWithKeyCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "bzpopMax", m{"keys": keys, "timeout": timeout}
	result := c.Cmdable.BZPopMax(ctx, timeout, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// BZPopMin overrides redis.Cmdable.BZPopMin to log execution metrics.
func (c *CmdableWrapper) BZPopMin(ctx context.Context, timeout time.Duration, keys ...string) *redis.ZWithKeyCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "bzpopMin", m{"keys": keys, "timeout": timeout}
	result := c.Cmdable.BZPopMin(ctx, timeout, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZAdd overrides redis.Cmdable.ZAdd to log execution metrics.
func (c *CmdableWrapper) ZAdd(ctx context.Context, key string, values ...*redis.Z) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zadd", m{"key": key, "values": values}
	result := c.Cmdable.ZAdd(ctx, key, values...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZCard overrides redis.Cmdable.ZCard to log execution metrics.
func (c *CmdableWrapper) ZCard(ctx context.Context, key string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zcard", m{"key": key}
	result := c.Cmdable.ZCard(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZCount overrides redis.Cmdable.ZCount to log execution metrics.
func (c *CmdableWrapper) ZCount(ctx context.Context, key, min, max string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zcount", m{"key": key, "min": min, "max": max}
	result := c.Cmdable.ZCount(ctx, key, min, max)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZDiff overrides redis.Cmdable.ZDiff to log execution metrics.
func (c *CmdableWrapper) ZDiff(ctx context.Context, keys ...string) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zdiff", m{"keys": keys}
	result := c.Cmdable.ZDiff(ctx, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZDiffWithScores overrides redis.Cmdable.ZDiffWithScores to log execution metrics.
func (c *CmdableWrapper) ZDiffWithScores(ctx context.Context, keys ...string) *redis.ZSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zdiff", m{"keys": keys, "withScores": true}
	result := c.Cmdable.ZDiffWithScores(ctx, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZDiffStore overrides redis.Cmdable.ZDiffStore to log execution metrics.
func (c *CmdableWrapper) ZDiffStore(ctx context.Context, destKey string, keys ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zdiffStore", m{"dest": destKey, "keys": keys}
	result := c.Cmdable.ZDiffStore(ctx, destKey, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZIncrBy overrides redis.Cmdable.ZIncrBy to log execution metrics.
func (c *CmdableWrapper) ZIncrBy(ctx context.Context, key string, increment float64, value string) *redis.FloatCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zincrBy", m{"key": key, "value": value, "increment": increment}
	result := c.Cmdable.ZIncrBy(ctx, key, increment, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZInter overrides redis.Cmdable.ZInter to log execution metrics.
func (c *CmdableWrapper) ZInter(ctx context.Context, store *redis.ZStore) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zinter", m{"keys": store.Keys, "weights": store.Weights, "aggregate": store.Aggregate}
	result := c.Cmdable.ZInter(ctx, store)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZInterWithScores overrides redis.Cmdable.ZInterWithScores to log execution metrics.
func (c *CmdableWrapper) ZInterWithScores(ctx context.Context, store *redis.ZStore) *redis.ZSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zinter", m{"keys": store.Keys, "weights": store.Weights, "aggregate": store.Aggregate, "withScores": true}
	result := c.Cmdable.ZInterWithScores(ctx, store)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZInterStore overrides redis.Cmdable.ZInterStore to log execution metrics.
func (c *CmdableWrapper) ZInterStore(ctx context.Context, destKey string, store *redis.ZStore) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zinterStore", m{"dest": destKey, "store": store}
	result := c.Cmdable.ZInterStore(ctx, destKey, store)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZLexCount overrides redis.Cmdable.ZLexCount to log execution metrics.
func (c *CmdableWrapper) ZLexCount(ctx context.Context, key, min, max string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zlexCount", m{"key": key, "min": min, "max": max}
	result := c.Cmdable.ZLexCount(ctx, key, min, max)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZMScore overrides redis.Cmdable.ZMScore to log execution metrics.
func (c *CmdableWrapper) ZMScore(ctx context.Context, key string, values ...string) *redis.FloatSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zmscore", m{"key": key, "values": values}
	result := c.Cmdable.ZMScore(ctx, key, values...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZPopMax overrides redis.Cmdable.ZPopMax to log execution metrics.
func (c *CmdableWrapper) ZPopMax(ctx context.Context, key string, count ...int64) *redis.ZSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zpopMax", m{"key": key, "count": count}
	result := c.Cmdable.ZPopMax(ctx, key, count...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZPopMin overrides redis.Cmdable.ZPopMin to log execution metrics.
func (c *CmdableWrapper) ZPopMin(ctx context.Context, key string, count ...int64) *redis.ZSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zpopMin", m{"key": key, "count": count}
	result := c.Cmdable.ZPopMin(ctx, key, count...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRandMember overrides redis.Cmdable.ZRandMember to log execution metrics.
func (c *CmdableWrapper) ZRandMember(ctx context.Context, key string, count int, withScores bool) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrandMember", m{"key": key, "count": count}
	result := c.Cmdable.ZRandMember(ctx, key, count, withScores)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRangeArgs overrides redis.Cmdable.ZRangeArgs to log execution metrics.
func (c *CmdableWrapper) ZRangeArgs(ctx context.Context, args redis.ZRangeArgs) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrange", m{
		"key": args.Key, "rev": args.Rev, "byScore": args.ByScore, "byLex": args.ByLex,
		"start": args.Start, "stop": args.Stop, "offset": args.Offset, "count": args.Count,
	}
	result := c.Cmdable.ZRangeArgs(ctx, args)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRangeArgsWithScores overrides redis.Cmdable.ZRangeArgsWithScores to log execution metrics.
func (c *CmdableWrapper) ZRangeArgsWithScores(ctx context.Context, args redis.ZRangeArgs) *redis.ZSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrange", m{
		"key": args.Key, "rev": args.Rev, "byScore": args.ByScore, "byLex": args.ByLex, "withScores": true,
		"start": args.Start, "stop": args.Stop, "offset": args.Offset, "count": args.Count,
	}
	result := c.Cmdable.ZRangeArgsWithScores(ctx, args)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRange overrides redis.Cmdable.ZRange to log execution metrics.
func (c *CmdableWrapper) ZRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	return c.ZRangeArgs(ctx, redis.ZRangeArgs{Key: key, Start: start, Stop: stop})
}

// ZRangeWithScores overrides redis.Cmdable.ZRangeWithScores to log execution metrics.
func (c *CmdableWrapper) ZRangeWithScores(ctx context.Context, key string, start, stop int64) *redis.ZSliceCmd {
	return c.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{Key: key, Start: start, Stop: stop})
}

// ZRangeByLex overrides redis.Cmdable.ZRangeByLex to log execution metrics.
//
// @Deprecated since Redis 6.2.0, use ZRangeArgs instead.
func (c *CmdableWrapper) ZRangeByLex(ctx context.Context, key string, opts *redis.ZRangeBy) *redis.StringSliceCmd {
	return c.ZRangeArgs(ctx, redis.ZRangeArgs{Key: key, Start: opts.Min, Stop: opts.Max, Offset: opts.Offset, Count: opts.Count, ByLex: true})
}

// ZRangeByScore overrides redis.Cmdable.ZRangeByScore to log execution metrics.
//
// @Deprecated since Redis 6.2.0, use ZRangeArgs instead.
func (c *CmdableWrapper) ZRangeByScore(ctx context.Context, key string, opts *redis.ZRangeBy) *redis.StringSliceCmd {
	return c.ZRangeArgs(ctx, redis.ZRangeArgs{Key: key, Start: opts.Min, Stop: opts.Max, Offset: opts.Offset, Count: opts.Count, ByScore: true})
}

// ZRangeByScoreWithScores overrides redis.Cmdable.ZRangeByScoreWithScores to log execution metrics.
//
// @Deprecated since Redis 6.2.0, use ZRangeArgs instead.
func (c *CmdableWrapper) ZRangeByScoreWithScores(ctx context.Context, key string, opts *redis.ZRangeBy) *redis.ZSliceCmd {
	return c.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{Key: key, Start: opts.Min, Stop: opts.Max, Offset: opts.Offset, Count: opts.Count, ByScore: true})
}

// ZRangeStore overrides redis.Cmdable.ZRangeStore to log execution metrics.
func (c *CmdableWrapper) ZRangeStore(ctx context.Context, destKey string, args redis.ZRangeArgs) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrangeStore", m{
		"dest": destKey, "key": args.Key, "rev": args.Rev, "byScore": args.ByScore, "byLex": args.ByLex,
		"start": args.Start, "stop": args.Stop, "offset": args.Offset, "count": args.Count,
	}
	result := c.Cmdable.ZRangeStore(ctx, destKey, args)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRank overrides redis.Cmdable.ZRank to log execution metrics.
func (c *CmdableWrapper) ZRank(ctx context.Context, key, member string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrank", m{"key": key, "member": member}
	result := c.Cmdable.ZRank(ctx, key, member)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRem overrides redis.Cmdable.ZRem to log execution metrics.
func (c *CmdableWrapper) ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrem", m{"key": key, "members": members}
	result := c.Cmdable.ZRem(ctx, key, members)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRemRangeByLex overrides redis.Cmdable.ZRemRangeByLex to log execution metrics.
func (c *CmdableWrapper) ZRemRangeByLex(ctx context.Context, key, min, max string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zremRangeByLex", m{"key": key, "min": min, "max": max}
	result := c.Cmdable.ZRemRangeByLex(ctx, key, min, max)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRemRangeByRank overrides redis.Cmdable.ZRemRangeByRank to log execution metrics.
func (c *CmdableWrapper) ZRemRangeByRank(ctx context.Context, key string, start, stop int64) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zremRangeByRank", m{"key": key, "start": start, "stop": stop}
	result := c.Cmdable.ZRemRangeByRank(ctx, key, start, stop)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRemRangeByScore overrides redis.Cmdable.ZRemRangeByScore to log execution metrics.
func (c *CmdableWrapper) ZRemRangeByScore(ctx context.Context, key, min, max string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zremRangeByScore", m{"key": key, "min": min, "max": max}
	result := c.Cmdable.ZRemRangeByScore(ctx, key, min, max)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRevRange overrides redis.Cmdable.ZRevRange to log execution metrics.
//
// @Deprecated since Redis 6.2.0, use ZRangeArgs instead.
func (c *CmdableWrapper) ZRevRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrevRange", m{"key": key, "start": start, "stop": stop}
	result := c.Cmdable.ZRevRange(ctx, key, start, stop)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRevRangeWithScores overrides redis.Cmdable.ZRevRangeWithScores to log execution metrics.
//
// @Deprecated since Redis 6.2.0, use ZRangeArgs instead.
func (c *CmdableWrapper) ZRevRangeWithScores(ctx context.Context, key string, start, stop int64) *redis.ZSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrevRange", m{"key": key, "start": start, "stop": stop, "withScores": true}
	result := c.Cmdable.ZRevRangeWithScores(ctx, key, start, stop)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRevRangeByLex overrides redis.Cmdable.ZRevRangeByLex to log execution metrics.
//
// @Deprecated since Redis 6.2.0, use ZRangeArgs instead.
func (c *CmdableWrapper) ZRevRangeByLex(ctx context.Context, key string, opts *redis.ZRangeBy) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrevRangeByLex", m{"key": key, "max": opts.Max, "min": opts.Min, "offset": opts.Offset, "count": opts.Count}
	result := c.Cmdable.ZRevRangeByLex(ctx, key, opts)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRevRangeByScore overrides redis.Cmdable.ZRevRangeByScore to log execution metrics.
//
// @Deprecated since Redis 6.2.0, use ZRangeArgs instead.
func (c *CmdableWrapper) ZRevRangeByScore(ctx context.Context, key string, opts *redis.ZRangeBy) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrevRangeByScore", m{"key": key, "max": opts.Max, "min": opts.Min, "offset": opts.Offset, "count": opts.Count}
	result := c.Cmdable.ZRevRangeByScore(ctx, key, opts)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRevRangeByScoreWithScores overrides redis.Cmdable.ZRevRangeByScoreWithScores to log execution metrics.
//
// @Deprecated since Redis 6.2.0, use ZRangeArgs instead.
func (c *CmdableWrapper) ZRevRangeByScoreWithScores(ctx context.Context, key string, opts *redis.ZRangeBy) *redis.ZSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrevRangeByScore", m{"key": key, "max": opts.Max, "min": opts.Min, "offset": opts.Offset, "count": opts.Count, "withScores": true}
	result := c.Cmdable.ZRevRangeByScoreWithScores(ctx, key, opts)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRevRank overrides redis.Cmdable.ZRevRank to log execution metrics.
func (c *CmdableWrapper) ZRevRank(ctx context.Context, key string, member string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrevRank", m{"key": key, "member": member}
	result := c.Cmdable.ZRevRank(ctx, key, member)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZScan overrides redis.Cmdable.ZScan to log execution metrics.
func (c *CmdableWrapper) ZScan(ctx context.Context, key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zscan", m{"key": key, "cursor": cursor, "match": match, "count": count}
	result := c.Cmdable.ZScan(ctx, key, cursor, match, count)
	val, _, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZScore overrides redis.Cmdable.ZScore to log execution metrics.
func (c *CmdableWrapper) ZScore(ctx context.Context, key, member string) *redis.FloatCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zscore", m{"key": key, "member": member}
	result := c.Cmdable.ZScore(ctx, key, member)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZUnion overrides redis.Cmdable.ZUnion to log execution metrics.
func (c *CmdableWrapper) ZUnion(ctx context.Context, store redis.ZStore) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zunion", m{"keys": store.Keys, "aggregate": store.Aggregate, "weights": store.Weights}
	result := c.Cmdable.ZUnion(ctx, store)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZUnionWithScores overrides redis.Cmdable.ZUnionWithScores to log execution metrics.
func (c *CmdableWrapper) ZUnionWithScores(ctx context.Context, store redis.ZStore) *redis.ZSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zunion", m{"keys": store.Keys, "aggregate": store.Aggregate, "weights": store.Weights, "withScores": true}
	result := c.Cmdable.ZUnionWithScores(ctx, store)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZUnionStore overrides redis.Cmdable.ZUnionStore to log execution metrics.
func (c *CmdableWrapper) ZUnionStore(ctx context.Context, destKey string, store *redis.ZStore) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zunionStore", m{"dest": destKey, "keys": store.Keys, "aggregate": store.Aggregate, "weights": store.Weights}
	result := c.Cmdable.ZUnionStore(ctx, destKey, store)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

/*----- Stream-related commands -----*/

// XAck overrides redis.Cmdable.XAck to log execution metrics.
func (c *CmdableWrapper) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xack", m{"stream": stream, "group": stream, "ids": ids}
	result := c.Cmdable.XAck(ctx, stream, group, ids...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XAdd overrides redis.Cmdable.XAdd to log execution metrics.
func (c *CmdableWrapper) XAdd(ctx context.Context, args *redis.XAddArgs) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xadd", m{"stream": args.Stream, "noMkStream": args.NoMkStream, "maxLen": args.MaxLen,
		"minId": args.MinID, "approx": args.Approx, "limit": args.Limit, "id": args.ID, "values": args.Values}
	result := c.Cmdable.XAdd(ctx, args)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XAutoClaim overrides redis.Cmdable.XAutoClaim to log execution metrics.
func (c *CmdableWrapper) XAutoClaim(ctx context.Context, args *redis.XAutoClaimArgs) *redis.XAutoClaimCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xautoClaim", m{"stream": args.Stream, "group": args.Group, "minIdle": args.MinIdle,
		"start": args.Start, "count": args.Count, "consumer": args.Consumer}
	result := c.Cmdable.XAutoClaim(ctx, args)
	val, _, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XAutoClaimJustID overrides redis.Cmdable.XAutoClaimJustID to log execution metrics.
func (c *CmdableWrapper) XAutoClaimJustID(ctx context.Context, args *redis.XAutoClaimArgs) *redis.XAutoClaimJustIDCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xautoClaim", m{"stream": args.Stream, "group": args.Group, "minIdle": args.MinIdle,
		"start": args.Start, "count": args.Count, "consumer": args.Consumer, "justId": true}
	result := c.Cmdable.XAutoClaimJustID(ctx, args)
	val, _, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XDel overrides redis.Cmdable.XDel to log execution metrics.
func (c *CmdableWrapper) XDel(ctx context.Context, stream string, ids ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xdel", m{"stream": stream, "ids": ids}
	result := c.Cmdable.XDel(ctx, stream, ids...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XGroupCreate overrides redis.Cmdable.XGroupCreate to log execution metrics.
func (c *CmdableWrapper) XGroupCreate(ctx context.Context, stream, group, start string) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xgroupCreate", m{"stream": stream, "group": group, "start": start}
	result := c.Cmdable.XGroupCreate(ctx, stream, group, start)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XGroupCreateMkStream overrides redis.Cmdable.XGroupCreateMkStream to log execution metrics.
func (c *CmdableWrapper) XGroupCreateMkStream(ctx context.Context, stream, group, start string) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xgroupCreate", m{"stream": stream, "group": group, "start": start, "mkStream": true}
	result := c.Cmdable.XGroupCreateMkStream(ctx, stream, group, start)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XGroupCreateConsumer overrides redis.Cmdable.XGroupCreateConsumer to log execution metrics.
func (c *CmdableWrapper) XGroupCreateConsumer(ctx context.Context, stream, group, consumer string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xgroupCreateConsumer", m{"stream": stream, "group": group, "consumer": consumer}
	result := c.Cmdable.XGroupCreateConsumer(ctx, stream, group, consumer)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XGroupDelConsumer overrides redis.Cmdable.XGroupDelConsumer to log execution metrics.
func (c *CmdableWrapper) XGroupDelConsumer(ctx context.Context, stream, group, consumer string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xgroupDelConsumer", m{"stream": stream, "group": group, "consumer": consumer}
	result := c.Cmdable.XGroupDelConsumer(ctx, stream, group, consumer)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XGroupDestroy overrides redis.Cmdable.XGroupDestroy to log execution metrics.
func (c *CmdableWrapper) XGroupDestroy(ctx context.Context, stream, group string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xgroupDestroy", m{"stream": stream, "group": group}
	result := c.Cmdable.XGroupDestroy(ctx, stream, group)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XGroupSetID overrides redis.Cmdable.XGroupSetID to log execution metrics.
func (c *CmdableWrapper) XGroupSetID(ctx context.Context, stream, group, start string) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xgroupSetId", m{"stream": stream, "group": group, "start": start}
	result := c.Cmdable.XGroupSetID(ctx, stream, group, start)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XInfoConsumers overrides redis.Cmdable.XInfoConsumers to log execution metrics.
func (c *CmdableWrapper) XInfoConsumers(ctx context.Context, key, group string) *redis.XInfoConsumersCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xinfoConsumers", m{"key": key, "group": group}
	result := c.Cmdable.XInfoConsumers(ctx, key, group)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XInfoGroups overrides redis.Cmdable.XInfoGroups to log execution metrics.
func (c *CmdableWrapper) XInfoGroups(ctx context.Context, key string) *redis.XInfoGroupsCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xinfoGroups", m{"key": key}
	result := c.Cmdable.XInfoGroups(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XInfoStream overrides redis.Cmdable.XInfoStream to log execution metrics.
func (c *CmdableWrapper) XInfoStream(ctx context.Context, key string) *redis.XInfoStreamCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xinfoStream", m{"key": key}
	result := c.Cmdable.XInfoStream(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XInfoStreamFull overrides redis.Cmdable.XInfoStreamFull to log execution metrics.
func (c *CmdableWrapper) XInfoStreamFull(ctx context.Context, key string, count int) *redis.XInfoStreamFullCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xinfoStream", m{"key": key, "full": true, "count": count}
	result := c.Cmdable.XInfoStreamFull(ctx, key, count)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XLen overrides redis.Cmdable.XLen to log execution metrics.
func (c *CmdableWrapper) XLen(ctx context.Context, stream string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xlen", m{"stream": stream}
	result := c.Cmdable.XLen(ctx, stream)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XPending overrides redis.Cmdable.XPending to log execution metrics.
func (c *CmdableWrapper) XPending(ctx context.Context, stream, group string) *redis.XPendingCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xpending", m{"stream": stream, "group": group}
	result := c.Cmdable.XPending(ctx, stream, group)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XPendingExt overrides redis.Cmdable.XPendingExt to log execution metrics.
func (c *CmdableWrapper) XPendingExt(ctx context.Context, args *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xpending", m{"stream": args.Stream, "group": args.Group, "consumer": args.Consumer,
		"idle": args.Idle, "start": args.Start, "end": args.End, "count": args.Count}
	result := c.Cmdable.XPendingExt(ctx, args)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XRange overrides redis.Cmdable.XRange to log execution metrics.
func (c *CmdableWrapper) XRange(ctx context.Context, stream, start, stop string) *redis.XMessageSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xrange", m{"stream": stream, "start": start, "stop": stop}
	result := c.Cmdable.XRange(ctx, stream, start, stop)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XRangeN overrides redis.Cmdable.XRangeN to log execution metrics.
func (c *CmdableWrapper) XRangeN(ctx context.Context, stream, start, stop string, count int64) *redis.XMessageSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xrange", m{"stream": stream, "start": start, "stop": stop, "count": count}
	result := c.Cmdable.XRangeN(ctx, stream, start, stop, count)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XRead overrides redis.Cmdable.XRead to log execution metrics.
func (c *CmdableWrapper) XRead(ctx context.Context, args *redis.XReadArgs) *redis.XStreamSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xread", m{"streams": args.Streams, "block": args.Block, "count": args.Count}
	result := c.Cmdable.XRead(ctx, args)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XReadGroup overrides redis.Cmdable.XReadGroup to log execution metrics.
func (c *CmdableWrapper) XReadGroup(ctx context.Context, args *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xreadGroup", m{"group": args.Group, "consumer": args.Consumer, "streams": args.Streams,
		"block": args.Block, "count": args.Count, "noAck": args.NoAck}
	result := c.Cmdable.XReadGroup(ctx, args)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XRevRange overrides redis.Cmdable.XRevRange to log execution metrics.
func (c *CmdableWrapper) XRevRange(ctx context.Context, stream, start, stop string) *redis.XMessageSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xrevRange", m{"stream": stream, "start": start, "stop": stop}
	result := c.Cmdable.XRevRange(ctx, stream, start, stop)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XRevRangeN overrides redis.Cmdable.XRevRangeN to log execution metrics.
func (c *CmdableWrapper) XRevRangeN(ctx context.Context, stream, start, stop string, count int64) *redis.XMessageSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xrevRange", m{"stream": stream, "start": start, "stop": stop, "count": count}
	result := c.Cmdable.XRevRangeN(ctx, stream, start, stop, count)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Function XTrim is deprecated, use XTrimMaxLen

// Function XTrimApprox is deprecated, use XTrimMaxlenApprox

// XTrimMaxLen overrides redis.Cmdable.XTrimMaxLen to log execution metrics.
func (c *CmdableWrapper) XTrimMaxLen(ctx context.Context, key string, maxLen int64) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xtrim", m{"key": key, "maxLen": maxLen}
	result := c.Cmdable.XTrimMaxLen(ctx, key, maxLen)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XTrimMaxLenApprox overrides redis.Cmdable.XTrimMaxLenApprox to log execution metrics.
func (c *CmdableWrapper) XTrimMaxLenApprox(ctx context.Context, key string, maxLen, limit int64) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xtrim", m{"key": key, "maxLen": maxLen, "approx": true, "limit": limit}
	result := c.Cmdable.XTrimMaxLenApprox(ctx, key, maxLen, limit)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XTrimMinID overrides redis.Cmdable.XTrimMinID to log execution metrics.
func (c *CmdableWrapper) XTrimMinID(ctx context.Context, key, minId string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xtrim", m{"key": key, "minId": minId}
	result := c.Cmdable.XTrimMinID(ctx, key, minId)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XTrimMinIDApprox overrides redis.Cmdable.XTrimMinIDApprox to log execution metrics.
func (c *CmdableWrapper) XTrimMinIDApprox(ctx context.Context, key, minId string, limit int64) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xtrim", m{"key": key, "minId": minId, "limit": limit}
	result := c.Cmdable.XTrimMinIDApprox(ctx, key, minId, limit)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

/*----- String-related commands -----*/

// Append overrides redis.Cmdable.Append to log execution metrics.
func (c *CmdableWrapper) Append(ctx context.Context, key, value string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "append", m{"key": key, "value": value}
	result := c.Cmdable.Append(ctx, key, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Decr overrides redis.Cmdable.Decr to log execution metrics.
func (c *CmdableWrapper) Decr(ctx context.Context, key string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "decr", m{"key": key}
	result := c.Cmdable.Decr(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// DecrBy overrides redis.Cmdable.DecrBy to log execution metrics.
func (c *CmdableWrapper) DecrBy(ctx context.Context, key string, value int64) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "decrBy", m{"key": key, "value": value}
	result := c.Cmdable.DecrBy(ctx, key, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Get overrides redis.Cmdable.Get to log execution metrics.
func (c *CmdableWrapper) Get(ctx context.Context, key string) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "get", m{"key": key}
	result := c.Cmdable.Get(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// GetDel overrides redis.Cmdable.GetDel to log execution metrics.
func (c *CmdableWrapper) GetDel(ctx context.Context, key string) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "getDel", m{"key": key}
	result := c.Cmdable.GetDel(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// GetEx overrides redis.Cmdable.GetEx to log execution metrics.
func (c *CmdableWrapper) GetEx(ctx context.Context, key string, expiration time.Duration) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "getEx", m{"key": key, "expiration": expiration}
	result := c.Cmdable.GetEx(ctx, key, expiration)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// GetRange overrides redis.Cmdable.GetRange to log execution metrics.
func (c *CmdableWrapper) GetRange(ctx context.Context, key string, start, end int64) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "getRange", m{"key": key, "start": start, "end": end}
	result := c.Cmdable.GetRange(ctx, key, start, end)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// GetSet overrides redis.Cmdable.GetSet to log execution metrics.
func (c *CmdableWrapper) GetSet(ctx context.Context, key string, value interface{}) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "getSet", m{"key": key, "value": value}
	result := c.Cmdable.GetSet(ctx, key, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Incr overrides redis.Cmdable.Decr to log execution metrics.
func (c *CmdableWrapper) Incr(ctx context.Context, key string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "incr", m{"key": key}
	result := c.Cmdable.Incr(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// IncrBy overrides redis.Cmdable.IncrBy to log execution metrics.
func (c *CmdableWrapper) IncrBy(ctx context.Context, key string, value int64) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "incrBy", m{"key": key, "value": value}
	result := c.Cmdable.IncrBy(ctx, key, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// IncrByFloat overrides redis.Cmdable.IncrByFloat to log execution metrics.
func (c *CmdableWrapper) IncrByFloat(ctx context.Context, key string, value float64) *redis.FloatCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "incrByFloat", m{"key": key, "value": value}
	result := c.Cmdable.IncrByFloat(ctx, key, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// MGet overrides redis.Cmdable.MGet to log execution metrics.
func (c *CmdableWrapper) MGet(ctx context.Context, keys ...string) *redis.SliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "mget", m{"keys": keys}
	result := c.Cmdable.MGet(ctx, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// MSet overrides redis.Cmdable.MSet to log execution metrics.
func (c *CmdableWrapper) MSet(ctx context.Context, values ...interface{}) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "mset", m{"values": values}
	result := c.Cmdable.MSet(ctx, values...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// MSetNX overrides redis.Cmdable.MSetNX to log execution metrics.
func (c *CmdableWrapper) MSetNX(ctx context.Context, values ...interface{}) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "msetnx", m{"values": values}
	result := c.Cmdable.MSetNX(ctx, values...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// No function PSetEx available! Use SetEX

// Set overrides redis.Cmdable.Set to log execution metrics.
func (c *CmdableWrapper) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "set", m{"key": key, "value": value, "expiration": expiration}
	result := c.Cmdable.Set(ctx, key, value, expiration)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SetEX overrides redis.Cmdable.SetEX to log execution metrics.
func (c *CmdableWrapper) SetEX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "setex", m{"key": key, "value": value, "expiration": expiration}
	result := c.Cmdable.SetEX(ctx, key, value, expiration)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SetNX overrides redis.Cmdable.SetNX to log execution metrics.
func (c *CmdableWrapper) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "setnx", m{"key": key, "value": value, "expiration": expiration}
	result := c.Cmdable.SetNX(ctx, key, value, expiration)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SetRange overrides redis.Cmdable.SetRange to log execution metrics.
func (c *CmdableWrapper) SetRange(ctx context.Context, key string, offset int64, value string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "setRange", m{"key": key, "offset": offset, "value": value}
	result := c.Cmdable.SetRange(ctx, key, offset, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// StrLen overrides redis.Cmdable.StrLen to log execution metrics.
func (c *CmdableWrapper) StrLen(ctx context.Context, key string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "strLen", m{"key": key}
	result := c.Cmdable.StrLen(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Function SubStr is deprecated! Use GetRange.

/*----- Transaction-related commands -----*/

// TODO
