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
//
// @Redis: available since v3.0.0
func (cp *RedisClientProxy) Wait(ctx context.Context, numReplicas int, timeout time.Duration) *redis.IntCmd {
	cmd := cp.rc.NewCmdExecInfo()
	defer func() {
		cp.rc.LogMetrics(prom.MetricsCatAll, cmd)
		cp.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "wait", m{"num_replicas": numReplicas, "timeout": timeout}
	result := cp.Cmdable.(*redis.Client).Wait(ctx, numReplicas, timeout)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// PSubscribe overrides redis.Client/PSubscribe to log execution metrics.
//
// To unsubscribe from channels, call redis.PubSub/Close or redis.PubSub/Unsubscribe/PUnsubscribe/SUnsubscribe.
//
// @Redis: available since v2.0.0
func (cp *RedisClientProxy) PSubscribe(ctx context.Context, patterns ...string) *redis.PubSub {
	cmd := cp.rc.NewCmdExecInfo()
	defer func() {
		cp.rc.LogMetrics(prom.MetricsCatAll, cmd)
		cp.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "psubscribe", m{"patterns": patterns}
	result := cp.Cmdable.(*redis.Client).PSubscribe(ctx, patterns...)
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, nil)
	return result
}

// Subscribe overrides redis.Client/Subscribe to log execution metrics.
//
// To unsubscribe from channels, call redis.PubSub/Close or redis.PubSub/Unsubscribe/PUnsubscribe/SUnsubscribe.
//
// @Redis: available since v2.0.0
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

// SSubscribe overrides redis.Client/SSubscribe to log execution metrics.
//
// To unsubscribe from channels, call redis.PubSub/Close or redis.PubSub/Unsubscribe/PUnsubscribe/SUnsubscribe.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (cp *RedisClientProxy) SSubscribe(ctx context.Context, shardChannels ...string) *redis.PubSub {
	cmd := cp.rc.NewCmdExecInfo()
	defer func() {
		cp.rc.LogMetrics(prom.MetricsCatAll, cmd)
		cp.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "ssubscribe", m{"shard_channels": shardChannels}
	result := cp.Cmdable.(*redis.Client).SSubscribe(ctx, shardChannels...)
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

// ClusterFailover overrides redis.ClusterClient/ClusterFailover to log execution metrics.
//
// @Redis: available since v3.0.0
//
// @Available since <<VERSION>>
func (cp *RedisClusterClientProxy) ClusterFailover(ctx context.Context) *redis.StatusCmd {
	cmd := cp.rc.NewCmdExecInfo()
	defer func() {
		cp.rc.LogMetrics(prom.MetricsCatAll, cmd)
		cp.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "cluster_failover", nil
	result := cp.Cmdable.(*redis.ClusterClient).ClusterFailover(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Wait overrides redis.ClusterClient/Wait to log execution metrics.
//
// @Redis: available since v3.0.0
func (cp *RedisClusterClientProxy) Wait(ctx context.Context, numReplicas int, timeout time.Duration) *redis.IntCmd {
	cmd := cp.rc.NewCmdExecInfo()
	defer func() {
		cp.rc.LogMetrics(prom.MetricsCatAll, cmd)
		cp.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "wait", m{"num_replicas": numReplicas, "timeout": timeout}
	result := cp.Cmdable.(*redis.ClusterClient).Wait(ctx, numReplicas, timeout)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// PSubscribe overrides redis.ClusterClient/PSubscribe to log execution metrics.
//
// To unsubscribe from channels, call redis.PubSub/Close or redis.PubSub/Unsubscribe/PUnsubscribe/SUnsubscribe.
//
// @Redis: available since v2.0.0
func (cp *RedisClusterClientProxy) PSubscribe(ctx context.Context, patterns ...string) *redis.PubSub {
	cmd := cp.rc.NewCmdExecInfo()
	defer func() {
		cp.rc.LogMetrics(prom.MetricsCatAll, cmd)
		cp.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "psubscribe", m{"patterns": patterns}
	result := cp.Cmdable.(*redis.ClusterClient).PSubscribe(ctx, patterns...)
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, nil)
	return result
}

// Subscribe overrides redis.ClusterClient/Subscribe to log execution metrics.
//
// To unsubscribe from channels, call redis.PubSub/Close or redis.PubSub/Unsubscribe/PUnsubscribe/SUnsubscribe.
//
// @Redis: available since v2.0.0
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

// SSubscribe overrides redis.Client/SSubscribe to log execution metrics.
//
// To unsubscribe from channels, call redis.PubSub/Close or redis.PubSub/Unsubscribe/PUnsubscribe/SUnsubscribe.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (cp *RedisClusterClientProxy) SSubscribe(ctx context.Context, shardChannels ...string) *redis.PubSub {
	cmd := cp.rc.NewCmdExecInfo()
	defer func() {
		cp.rc.LogMetrics(prom.MetricsCatAll, cmd)
		cp.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "ssubscribe", m{"shard_channels": shardChannels}
	result := cp.Cmdable.(*redis.ClusterClient).SSubscribe(ctx, shardChannels...)
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
	cmd.CmdName, cmd.CmdRequest = "bit_count", m{"key": key, "args": bitCount}
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
	cmd.CmdName, cmd.CmdRequest = "bit_field", m{"key": key, "args": args}
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
	cmd.CmdName, cmd.CmdRequest = "bit_op", m{"op": "and", "dest_key": destKey, "keys": keys}
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
	cmd.CmdName, cmd.CmdRequest = "bit_op", m{"op": "or", "dest_key": destKey, "keys": keys}
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
	cmd.CmdName, cmd.CmdRequest = "bit_op", m{"op": "xor", "dest_key": destKey, "keys": keys}
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
	cmd.CmdName, cmd.CmdRequest = "bit_op", m{"op": "not", "dest_key": destKey, "key": key}
	result := c.Cmdable.BitOpNot(ctx, destKey, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// BitPos overrides redis.Cmdable/BitPos to log execution metrics.
//
// @Redis: available since v2.8.7
func (c *CmdableWrapper) BitPos(ctx context.Context, key string, bit int64, positions ...int64) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "bit_pos", m{"key": key, "bit": bit, "positions": positions}
	result := c.Cmdable.BitPos(ctx, key, bit, positions...)
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
	cmd.CmdName, cmd.CmdRequest = "get_bit", m{"key": key, "offset": offset}
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
	cmd.CmdName, cmd.CmdRequest = "set_bit", m{"key": key, "offset": offset, "value": value}
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
	cmd.CmdName, cmd.CmdRequest = "read_only", nil
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
	cmd.CmdName, cmd.CmdRequest = "read_write", nil
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
	cmd.CmdName, cmd.CmdRequest = "copy", m{"source": srcKey, "destination": destKey, "destination_db": destDb, "replace": replace}
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
func (c *CmdableWrapper) Expire(ctx context.Context, key string, dur time.Duration) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "expire", m{"key": key, "seconds": dur.Seconds()}
	result := c.Cmdable.Expire(ctx, key, dur)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ExpireGT overrides redis.Cmdable/ExpireGT to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) ExpireGT(ctx context.Context, key string, dur time.Duration) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "expire", m{"gt": true, "key": key, "seconds": dur.Seconds()}
	result := c.Cmdable.ExpireGT(ctx, key, dur)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ExpireLT overrides redis.Cmdable/ExpireLT to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) ExpireLT(ctx context.Context, key string, dur time.Duration) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "expire", m{"lt": true, "key": key, "seconds": dur.Seconds()}
	result := c.Cmdable.ExpireLT(ctx, key, dur)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ExpireNX overrides redis.Cmdable/ExpireNX to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) ExpireNX(ctx context.Context, key string, dur time.Duration) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "expire", m{"nx": true, "key": key, "seconds": dur.Seconds()}
	result := c.Cmdable.ExpireNX(ctx, key, dur)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ExpireXX overrides redis.Cmdable/ExpireXX to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) ExpireXX(ctx context.Context, key string, dur time.Duration) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "expire", m{"xx": true, "key": key, "seconds": dur.Seconds()}
	result := c.Cmdable.ExpireXX(ctx, key, dur)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ExpireAt overrides redis.Cmdable/ExpireAt to log execution metrics.
//
// @Redis: available since v1.2.0
func (c *CmdableWrapper) ExpireAt(ctx context.Context, key string, at time.Time) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "expire_at", m{"key": key, "unix_time_seconds": at.Unix()}
	result := c.Cmdable.ExpireAt(ctx, key, at)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// No function ExpireAt-GT/LT/NX/XX for now!

// ExpireTime overrides redis.Cmdable/ExpireTime to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) ExpireTime(ctx context.Context, key string) *redis.DurationCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "expire_time", m{"key": key}
	result := c.Cmdable.ExpireTime(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Keys overrides redis.Cmdable/Keys to log execution metrics.
//
// @Redis: available since v1.0.0
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

// Migrate overrides redis.Cmdable/Migrate to log execution metrics.
//
// @Redis: available since v2.6.0
func (c *CmdableWrapper) Migrate(ctx context.Context, host, port, key string, destDb int, timeout time.Duration) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "migrate", m{"host": host, "port": port, "key": key, "destination-db": destDb, "timeout": timeout}
	result := c.Cmdable.Migrate(ctx, host, port, key, destDb, timeout)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Move overrides redis.Cmdable/Move to log execution metrics.
//
// @Redis: available since v1.0.0
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

// ObjectEncoding overrides redis.Cmdable/ObjectEncoding to log execution metrics.
//
// @Redis: available since v2.2.3
func (c *CmdableWrapper) ObjectEncoding(ctx context.Context, key string) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "object_encoding", m{"key": key}
	result := c.Cmdable.ObjectEncoding(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// No function ObjectFreq for now!

// ObjectIdleTime overrides redis.Cmdable/ObjectIdleTime to log execution metrics.
//
// @Redis: available since v2.2.3
func (c *CmdableWrapper) ObjectIdleTime(ctx context.Context, key string) *redis.DurationCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "object_idle_time", m{"key": key}
	result := c.Cmdable.ObjectIdleTime(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ObjectRefCount overrides redis.Cmdable/ObjectRefCount to log execution metrics.
//
// @Redis: available since v2.2.3
func (c *CmdableWrapper) ObjectRefCount(ctx context.Context, key string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "object_ref_count", m{"key": key}
	result := c.Cmdable.ObjectRefCount(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Persist overrides redis.Cmdable/Persist to log execution metrics.
//
// @Redis: available since v2.2.0
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

// PExpire overrides redis.Cmdable/PExpire to log execution metrics.
//
// @Redis: available since v2.6.0
func (c *CmdableWrapper) PExpire(ctx context.Context, key string, dur time.Duration) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "pexpire", m{"key": key, "milliseconds": dur.Milliseconds()}
	result := c.Cmdable.PExpire(ctx, key, dur)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// No function PExpire-GT/LT/NX/XX for now!

// PExpireAt overrides redis.Cmdable/PExpireAt to log execution metrics.
//
// @Redis: available since v2.6.0
func (c *CmdableWrapper) PExpireAt(ctx context.Context, key string, at time.Time) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "pexpire_at", m{"key": key, "unix_time_milliseconds": at.UnixMilli()}
	result := c.Cmdable.PExpireAt(ctx, key, at)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// No function PExpireAt-GT/LT/NX/XX for now!

// PExpireTime overrides redis.Cmdable/PExpireTime to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) PExpireTime(ctx context.Context, key string) *redis.DurationCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "pexpire_time", m{"key": key}
	result := c.Cmdable.PExpireTime(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Ping overrides redis.Cmdable/Ping to log execution metrics.
//
// @Redis: available since v1.0.0
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

// PTTL overrides redis.Cmdable/PTTL to log execution metrics.
//
// @Redis: available since v2.6.0
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

// RandomKey overrides redis.Cmdable/RandomKey to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) RandomKey(ctx context.Context) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "random_key", nil
	result := c.Cmdable.RandomKey(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Rename overrides redis.Cmdable/Rename to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) Rename(ctx context.Context, key, newKey string) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "rename", m{"key": key, "new_key": newKey}
	result := c.Cmdable.Rename(ctx, key, newKey)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// RenameNX overrides redis.Cmdable/RenameNX to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) RenameNX(ctx context.Context, key, newKey string) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "rename_nx", m{"key": key, "new_key": newKey}
	result := c.Cmdable.RenameNX(ctx, key, newKey)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Restore overrides redis.Cmdable/Restore to log execution metrics.
//
// @Redis: available since v2.6.0
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

// RestoreReplace overrides redis.Cmdable/RestoreReplace to log execution metrics.
//
// @Redis: available since v2.6.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) RestoreReplace(ctx context.Context, key string, ttl time.Duration, value string) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "restore", m{"replace": true, "key": key, "ttl": ttl, "value": value}
	result := c.Cmdable.RestoreReplace(ctx, key, ttl, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Scan overrides redis.Cmdable/Scan to log execution metrics.
//
// @Redis: available since v2.8.0
func (c *CmdableWrapper) Scan(ctx context.Context, cursor uint64, pattern string, count int64) *redis.ScanCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "scan", m{"cursor": cursor, "pattern": pattern, "count": count}
	result := c.Cmdable.Scan(ctx, cursor, pattern, count)
	val, _, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ScanType overrides redis.Cmdable/ScanType to log execution metrics.
//
// @Redis: available since v2.8.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) ScanType(ctx context.Context, cursor uint64, pattern string, count int64, keyType string) *redis.ScanCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "scan", m{"cursor": cursor, "pattern": pattern, "count": count, "type": keyType}
	result := c.Cmdable.ScanType(ctx, cursor, pattern, count, keyType)
	val, _, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Sort overrides redis.Cmdable/Sort to log execution metrics.
//
// @Redis: available since v1.0.0
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

// SortRO overrides redis.Cmdable/SortRO to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) SortRO(ctx context.Context, key string, sort *redis.Sort) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "sort_ro", m{"key": key, "args": sort}
	result := c.Cmdable.SortRO(ctx, key, sort)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Touch overrides redis.Cmdable/Touch to log execution metrics.
//
// @Redis: available since v3.2.1
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

// TTL overrides redis.Cmdable/TTL to log execution metrics.
//
// @Redis: available since v1.0.0
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

// Type overrides redis.Cmdable/Type to log execution metrics.
//
// @Redis: available since v1.0.0
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

// Unlink overrides redis.Cmdable/Unlink to log execution metrics.
//
// @Redis: available since v4.0.0
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

// GeoAdd overrides redis.Cmdable/GeoAdd to log execution metrics.
//
// @Redis: available since v3.2.0
func (c *CmdableWrapper) GeoAdd(ctx context.Context, key string, geoLocations ...*redis.GeoLocation) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "geo_add", m{"key": key, "locations": geoLocations}
	result := c.Cmdable.GeoAdd(ctx, key, geoLocations...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// GeoDist overrides redis.Cmdable/GeoDist to log execution metrics.
//
// @Redis: available since v3.2.0
func (c *CmdableWrapper) GeoDist(ctx context.Context, key, member1, member2, unit string) *redis.FloatCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "geo_dist", m{"key": key, "member1": member1, "member2": member2, "unit": unit}
	result := c.Cmdable.GeoDist(ctx, key, member1, member2, unit)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// GeoHash overrides redis.Cmdable/GeoHash to log execution metrics.
//
// @Redis: available since v3.2.0
func (c *CmdableWrapper) GeoHash(ctx context.Context, key string, members ...string) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "geo_hash", m{"key": key, "members": members}
	result := c.Cmdable.GeoHash(ctx, key, members...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// GeoPos overrides redis.Cmdable/GeoPos to log execution metrics.
//
// @Redis: available since v3.2.0
func (c *CmdableWrapper) GeoPos(ctx context.Context, key string, members ...string) *redis.GeoPosCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "geo_pos", m{"key": key, "members": members}
	result := c.Cmdable.GeoPos(ctx, key, members...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// GeoRadius overrides redis.Cmdable/GeoRadius to log execution metrics.
//
// @Redis: available since v3.2.0 / deprecated since v6.2.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) GeoRadius(ctx context.Context, key string, longitude, latitude float64, query *redis.GeoRadiusQuery) *redis.GeoLocationCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "geo_radius", m{"key": key, "longitude": longitude, "latitude": latitude, "query": query}
	result := c.Cmdable.GeoRadius(ctx, key, longitude, latitude, query)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// GeoRadiusByMember overrides redis.Cmdable/GeoRadiusByMember to log execution metrics.
//
// @Redis: available since v3.2.0 / deprecated since v6.2.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) GeoRadiusByMember(ctx context.Context, key, member string, query *redis.GeoRadiusQuery) *redis.GeoLocationCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "geo_radius", m{"key": key, "member": member, "query": query}
	result := c.Cmdable.GeoRadiusByMember(ctx, key, member, query)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// GeoRadiusStore overrides redis.Cmdable/GeoRadiusStore to log execution metrics.
//
// @Redis: available since v3.2.0 / deprecated since v6.2.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) GeoRadiusStore(ctx context.Context, key string, longitude, latitude float64, query *redis.GeoRadiusQuery) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "geo_radius", m{"key": key, "longitude": longitude, "latitude": latitude, "query": query}
	result := c.Cmdable.GeoRadiusStore(ctx, key, longitude, latitude, query)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// GeoRadiusByMemberStore overrides redis.Cmdable/GeoRadiusByMemberStore to log execution metrics.
//
// @Redis: available since v3.2.0 / deprecated since v6.2.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) GeoRadiusByMemberStore(ctx context.Context, key, member string, query *redis.GeoRadiusQuery) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "geo_radius", m{"key": key, "member": member, "query": query}
	result := c.Cmdable.GeoRadiusByMemberStore(ctx, key, member, query)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// GeoSearch overrides redis.Cmdable/GeoSearch to log execution metrics.
//
// @Redis: available since v6.2.0
func (c *CmdableWrapper) GeoSearch(ctx context.Context, key string, query *redis.GeoSearchQuery) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "geo_search", m{"key": key, "query": query}
	result := c.Cmdable.GeoSearch(ctx, key, query)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// GeoSearchLocation overrides redis.Cmdable/GeoSearchLocation to log execution metrics.
//
// @Redis: available since v6.2.0
func (c *CmdableWrapper) GeoSearchLocation(ctx context.Context, key string, query *redis.GeoSearchLocationQuery) *redis.GeoSearchLocationCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "geo_search", m{"key": key, "query": query}
	result := c.Cmdable.GeoSearchLocation(ctx, key, query)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// GeoSearchStore overrides redis.Cmdable/GeoSearchStore to log execution metrics.
//
// @Redis: available since v6.2.0
func (c *CmdableWrapper) GeoSearchStore(ctx context.Context, key, destination string, query *redis.GeoSearchStoreQuery) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "geo_search_store", m{"source": key, "destination": destination, "query": query}
	result := c.Cmdable.GeoSearchStore(ctx, key, destination, query)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

/*----- Hash-related commands -----*/

// HDel overrides redis.Cmdable/HDel to log execution metrics.
//
// @Redis: available since v2.0.0
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

// HExists overrides redis.Cmdable/HExists to log execution metrics.
//
// @Redis: available since v2.0.0
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

// HGet overrides redis.Cmdable/HGet to log execution metrics.
//
// @Redis: available since v2.0.0
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

// HGetAll overrides redis.Cmdable/HGetAll to log execution metrics.
//
// @Redis: available since v2.0.0
func (c *CmdableWrapper) HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hget_all", m{"key": key}
	result := c.Cmdable.HGetAll(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// HIncrBy overrides redis.Cmdable/HIncrBy to log execution metrics.
//
// @Redis: available since v2.0.0
func (c *CmdableWrapper) HIncrBy(ctx context.Context, key, field string, increment int64) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hincr_by", m{"key": key, "field": field, "increment": increment}
	result := c.Cmdable.HIncrBy(ctx, key, field, increment)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// HIncrByFloat overrides redis.Cmdable/HIncrByFloat to log execution metrics.
//
// @Redis: available since v2.6.0
func (c *CmdableWrapper) HIncrByFloat(ctx context.Context, key, field string, increment float64) *redis.FloatCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hincr_by_float", m{"key": key, "field": field, "increment": increment}
	result := c.Cmdable.HIncrByFloat(ctx, key, field, increment)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// HKeys overrides redis.Cmdable/HKeys to log execution metrics.
//
// @Redis: available since v2.0.0
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

// HLen overrides redis.Cmdable/HLen to log execution metrics.
//
// @Redis: available since v2.0.0
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

// HMGet overrides redis.Cmdable/HMGet to log execution metrics.
//
// @Redis: available since v2.0.0
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

// HMSet overrides redis.Cmdable/HMSet to log execution metrics.
//
// @Redis: available since v2.0.0 / deprecated since v4.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) HMSet(ctx context.Context, key string, fieldsAndValues ...interface{}) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hmset", m{"key": key, "fields_values": fieldsAndValues}
	result := c.Cmdable.HMSet(ctx, key, fieldsAndValues...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// HRandField overrides redis.Cmdable/HRandField to log execution metrics.
//
// @Redis: available since v6.2.0
func (c *CmdableWrapper) HRandField(ctx context.Context, key string, count int) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hrand_field", m{"key": key, "count": count}
	result := c.Cmdable.HRandField(ctx, key, count)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// HRandFieldWithValues overrides redis.Cmdable/HRandFieldWithValues to log execution metrics.
//
// @Redis: available since v6.2.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) HRandFieldWithValues(ctx context.Context, key string, count int) *redis.KeyValueSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hrand_field", m{"key": key, "count": count, "with_values": true}
	result := c.Cmdable.HRandFieldWithValues(ctx, key, count)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// HScan overrides redis.Cmdable/HScan to log execution metrics.
//
// @Redis: available since v2.8.0
func (c *CmdableWrapper) HScan(ctx context.Context, key string, cursor uint64, pattern string, count int64) *redis.ScanCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hscan", m{"key": key, "cursor": cursor, "pattern": pattern, "count": count}
	result := c.Cmdable.HScan(ctx, key, cursor, pattern, count)
	val, _, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// HSet overrides redis.Cmdable/HSet to log execution metrics.
//
// @Redis: available since v2.0.0
func (c *CmdableWrapper) HSet(ctx context.Context, key string, fieldAndValues ...interface{}) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hset", m{"key": key, "fields_values": fieldAndValues}
	result := c.Cmdable.HSet(ctx, key, fieldAndValues...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// HSetNX overrides redis.Cmdable/HSetNX to log execution metrics.
//
// @Redis: available since v2.0.0
func (c *CmdableWrapper) HSetNX(ctx context.Context, key, field string, value interface{}) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "hset_nx", m{"key": key, "field": field, "value": value}
	result := c.Cmdable.HSetNX(ctx, key, field, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// No function HStrLen for now!

// HVals overrides redis.Cmdable/HVals to log execution metrics.
//
// @Redis: available since v2.0.0
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

// PFAdd overrides redis.Cmdable/PFAdd to log execution metrics.
//
// @Redis: available since v2.8.9
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

// PFCount overrides redis.Cmdable/PFCount to log execution metrics.
//
// @Redis: available since v2.8.9
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

// PFMerge overrides redis.Cmdable/PFMerge to log execution metrics.
//
// @Redis: available since v2.8.9
func (c *CmdableWrapper) PFMerge(ctx context.Context, destKey string, keys ...string) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "pfmerge", m{"dest_key": destKey, "source_keys": keys}
	result := c.Cmdable.PFMerge(ctx, destKey, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

/*----- List-related commands -----*/

// BLMove overrides redis.Cmdable/BLMove to log execution metrics.
//
// @Redis: available since v6.2.0
func (c *CmdableWrapper) BLMove(ctx context.Context, srcKey, destKey, srcPos, destPos string, timeout time.Duration) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "blmove", m{"source": srcKey, "destination": destKey, "source_position": srcPos, "destination_position": destPos, "timeout": timeout}
	result := c.Cmdable.BLMove(ctx, srcKey, destKey, srcPos, destPos, timeout)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// BLMPop overrides redis.Cmdable/BLMPop to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) BLMPop(ctx context.Context, timeout time.Duration, direction string, count int64, keys ...string) *redis.KeyValuesCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "blmpop", m{"timeout": timeout, "direction": direction, "count": count, "keys": keys}
	result := c.Cmdable.BLMPop(ctx, timeout, direction, count, keys...)
	key, val, err := result.Result()
	cmd.CmdResponse = m{key: val}
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// BLPop overrides redis.Cmdable/BLPop to log execution metrics.
//
// @Redis: available since v2.0.0
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

// BRPop overrides redis.Cmdable/BRPop to log execution metrics.
//
// @Redis: available since v2.0.0
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

// BRPopLPush overrides redis.Cmdable/BRPopLPush to log execution metrics.
//
// @Redis: available since v2.2.0 / deprecated since v6.2.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) BRPopLPush(ctx context.Context, source, destination string, timeout time.Duration) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "brpop_lpush", m{"source": source, "destination": destination, "timeout": timeout}
	result := c.Cmdable.BRPopLPush(ctx, source, destination, timeout)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// LIndex overrides redis.Cmdable/LIndex to log execution metrics.
//
// @Redis: available since v1.0.0
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

// LInsert overrides redis.Cmdable/LInsert to log execution metrics.
//
// @Redis: available since v2.2.0
func (c *CmdableWrapper) LInsert(ctx context.Context, key, position string, pivot, element interface{}) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "linsert", m{"key": key, "position": position, "pivot": pivot, "element": element}
	result := c.Cmdable.LInsert(ctx, key, position, pivot, element)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// LLen overrides redis.Cmdable/LLen to log execution metrics.
//
// @Redis: available since v1.0.0
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

// LMove overrides redis.Cmdable/LMove to log execution metrics.
//
// @Redis: available since v6.2.0
func (c *CmdableWrapper) LMove(ctx context.Context, srcKey, destKey, srcPos, destPos string) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "lmove", m{"source": srcKey, "destination": destKey, "source_position": srcPos, "destination_position": destPos}
	result := c.Cmdable.LMove(ctx, srcKey, destKey, srcPos, destPos)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// LMPop overrides redis.Cmdable/LMPop to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) LMPop(ctx context.Context, direction string, count int64, keys ...string) *redis.KeyValuesCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "lmpop", m{"direction": direction, "count": count, "keys": keys}
	result := c.Cmdable.LMPop(ctx, direction, count, keys...)
	key, val, err := result.Result()
	cmd.CmdResponse = m{key: val}
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// LPop overrides redis.Cmdable/LPop to log execution metrics.
//
// @Redis: available since v1.0.0
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

// LPos overrides redis.Cmdable/LPos to log execution metrics.
//
// @Redis: available since v6.0.6
func (c *CmdableWrapper) LPos(ctx context.Context, key, element string, args redis.LPosArgs) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "lpos", m{"key": key, "element": element, "args": args}
	result := c.Cmdable.LPos(ctx, key, element, args)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// LPush overrides redis.Cmdable/LPush to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) LPush(ctx context.Context, key string, elements ...interface{}) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "lpush", m{"key": key, "elements": elements}
	result := c.Cmdable.LPush(ctx, key, elements...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// LPushX overrides redis.Cmdable/LPushX to log execution metrics.
//
// @Redis: available since v2.2.0
func (c *CmdableWrapper) LPushX(ctx context.Context, key string, elements ...interface{}) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "lpushx", m{"key": key, "elements": elements}
	result := c.Cmdable.LPushX(ctx, key, elements...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// LRange overrides redis.Cmdable/LRange to log execution metrics.
//
// @Redis: available since v1.0.0
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

// LRem overrides redis.Cmdable/LRem to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) LRem(ctx context.Context, key string, count int64, element interface{}) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "lrem", m{"key": key, "count": count, "element": element}
	result := c.Cmdable.LRem(ctx, key, count, element)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// LSet overrides redis.Cmdable/LSet to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) LSet(ctx context.Context, key string, index int64, element interface{}) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "lset", m{"key": key, "index": index, "element": element}
	result := c.Cmdable.LSet(ctx, key, index, element)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// LTrim overrides redis.Cmdable/LTrim to log execution metrics.
//
// @Redis: available since v1.0.0
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

// RPop overrides redis.Cmdable/RPop to log execution metrics.
//
// @Redis: available since v1.0.0
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

// RPopCount overrides redis.Cmdable/RPopCount to log execution metrics.
//
// @Redis: available since v1.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) RPopCount(ctx context.Context, key string, count int) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "rpop", m{"key": key, "count": count}
	result := c.Cmdable.RPopCount(ctx, key, count)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// RPopLPush overrides redis.Cmdable/RPopLPush to log execution metrics.
//
// @Redis: available since v1.2.0 / deprecated since v6.2.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) RPopLPush(ctx context.Context, source, destination string) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "rpop_lpush", m{"source": source, "destination": destination}
	result := c.Cmdable.RPopLPush(ctx, source, destination)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// RPush overrides redis.Cmdable/RPush to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) RPush(ctx context.Context, key string, elements ...interface{}) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "rpush", m{"key": key, "elements": elements}
	result := c.Cmdable.RPush(ctx, key, elements...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// RPushX overrides redis.Cmdable/RPushX to log execution metrics.
//
// @Redis: available since v2.2.0
func (c *CmdableWrapper) RPushX(ctx context.Context, key string, elements ...interface{}) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "rpushx", m{"key": key, "elements": elements}
	result := c.Cmdable.RPushX(ctx, key, elements...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

/*----- Pub/Sub-related commands -----*/

// Function PSubscribe is overridden by each proxy!

// Publish overrides redis.Cmdable/Publish to log execution metrics.
//
// @Redis: available since v2.0.0
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

// PubSubChannels overrides redis.Cmdable/PubSubChannels to log execution metrics.
//
// @Redis: available since v2.8.0
func (c *CmdableWrapper) PubSubChannels(ctx context.Context, pattern string) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "pubsub_channels", m{"pattern": pattern}
	result := c.Cmdable.PubSubChannels(ctx, pattern)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// PubSubNumPat overrides redis.Cmdable/PubSubNumPat to log execution metrics.
//
// @Redis: available since v2.8.0
func (c *CmdableWrapper) PubSubNumPat(ctx context.Context) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "pubsub_num_pat", nil
	result := c.Cmdable.PubSubNumPat(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// PubSubNumSub overrides redis.Cmdable/PubSubNumSub to log execution metrics.
//
// @Redis: available since v2.8.0
func (c *CmdableWrapper) PubSubNumSub(ctx context.Context, channels ...string) *redis.MapStringIntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "pubsub_num_sub", m{"channels": channels}
	result := c.Cmdable.PubSubNumSub(ctx, channels...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// PubSubShardChannels overrides redis.Cmdable/PubSubShardChannels to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) PubSubShardChannels(ctx context.Context, pattern string) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "pubsub_shard_channels", m{"pattern": pattern}
	result := c.Cmdable.PubSubShardChannels(ctx, pattern)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// PubSubShardNumSub overrides redis.Cmdable/PubSubShardNumSub to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) PubSubShardNumSub(ctx context.Context, channels ...string) *redis.MapStringIntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "pubsub_shard_num_sub", m{"channels": channels}
	result := c.Cmdable.PubSubShardNumSub(ctx, channels...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Function PUnsubscribe: see RedisClientProxy.PSubscribe

// SPublish overrides redis.Cmdable/SPublish to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) SPublish(ctx context.Context, channel string, message interface{}) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "spublish", m{"channel": channel, "message": message}
	result := c.Cmdable.SPublish(ctx, channel, message)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Function SSubscribe is overridden by each proxy!

// Function Subscribe is overridden by each proxy!

// Function SUnsubscribe: see RedisClientProxy.SSubscribe

// Function Unsubscribe: see RedisClientProxy.Subscribe

/*----- Scripting-related commands -----*/

// Eval overrides redis.Cmdable/Eval to log execution metrics.
//
// @Redis: available since v2.6.0
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

// EvalRO overrides redis.Cmdable/EvalRO to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) EvalRO(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "eval_ro", m{"script": script, "keys": keys, "args": args}
	result := c.Cmdable.EvalRO(ctx, script, keys, args...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// EvalSha overrides redis.Cmdable/EvalSha to log execution metrics.
//
// @Redis: available since v2.6.0
func (c *CmdableWrapper) EvalSha(ctx context.Context, scriptSha string, keys []string, args ...interface{}) *redis.Cmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "eval_sha", m{"sha": scriptSha, "keys": keys, "args": args}
	result := c.Cmdable.EvalSha(ctx, scriptSha, keys, args...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// EvalShaRO overrides redis.Cmdable/EvalShaRO to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) EvalShaRO(ctx context.Context, scriptSha string, keys []string, args ...interface{}) *redis.Cmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "eval_sha_ro", m{"sha": scriptSha, "keys": keys, "args": args}
	result := c.Cmdable.EvalShaRO(ctx, scriptSha, keys, args...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// FCall overrides redis.Cmdable/FCall to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) FCall(ctx context.Context, function string, keys []string, args ...interface{}) *redis.Cmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "fcall", m{"function": function, "keys": keys, "args": args}
	result := c.Cmdable.FCall(ctx, function, keys, args...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// FCallRO overrides redis.Cmdable/FCallRO to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) FCallRO(ctx context.Context, function string, keys []string, args ...interface{}) *redis.Cmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "fcall_ro", m{"function": function, "keys": keys, "args": args}
	result := c.Cmdable.FCallRO(ctx, function, keys, args...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// FunctionDelete overrides redis.Cmdable/FunctionDelete to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) FunctionDelete(ctx context.Context, libName string) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "function_delete", m{"library_name": libName}
	result := c.Cmdable.FunctionDelete(ctx, libName)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// FunctionDump overrides redis.Cmdable/FunctionDump to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) FunctionDump(ctx context.Context) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "function_dump", nil
	result := c.Cmdable.FunctionDump(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// FunctionFlush overrides redis.Cmdable/FunctionFlush to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) FunctionFlush(ctx context.Context) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "function_flush", nil
	result := c.Cmdable.FunctionFlush(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// FunctionFlushAsync overrides redis.Cmdable/FunctionFlushAsync to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) FunctionFlushAsync(ctx context.Context) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "function_flush", m{"async": true}
	result := c.Cmdable.FunctionFlushAsync(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// FunctionKill overrides redis.Cmdable/FunctionKill to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) FunctionKill(ctx context.Context) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "function_kill", nil
	result := c.Cmdable.FunctionKill(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// FunctionList overrides redis.Cmdable/FunctionList to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) FunctionList(ctx context.Context, query redis.FunctionListQuery) *redis.FunctionListCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "function_list", m{"args": query}
	result := c.Cmdable.FunctionList(ctx, query)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// FunctionLoad overrides redis.Cmdable/FunctionLoad to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) FunctionLoad(ctx context.Context, code string) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "function_load", m{"function_code": code}
	result := c.Cmdable.FunctionLoad(ctx, code)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// FunctionLoadReplace overrides redis.Cmdable/FunctionLoadReplace to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) FunctionLoadReplace(ctx context.Context, code string) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "function_load", m{"function_code": code, "replace": true}
	result := c.Cmdable.FunctionLoadReplace(ctx, code)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// FunctionRestore overrides redis.Cmdable/FunctionRestore to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) FunctionRestore(ctx context.Context, libDump string) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "function_restore", m{"serialized_value": libDump}
	result := c.Cmdable.FunctionRestore(ctx, libDump)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// FunctionStats overrides redis.Cmdable/FunctionStats to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) FunctionStats(ctx context.Context) *redis.FunctionStatsCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "function_stats", nil
	result := c.Cmdable.FunctionStats(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ScriptExists overrides redis.Cmdable/ScriptExists to log execution metrics.
//
// @Redis: available since v2.6.0
func (c *CmdableWrapper) ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "script_exists", m{"hashes": hashes}
	result := c.Cmdable.ScriptExists(ctx, hashes...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ScriptFlush overrides redis.Cmdable/ScriptFlush to log execution metrics.
//
// @Redis: available since v2.6.0
func (c *CmdableWrapper) ScriptFlush(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "script_flush", nil
	result := c.Cmdable.ScriptFlush(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ScriptKill overrides redis.Cmdable/ScriptKill to log execution metrics.
//
// @Redis: available since v2.6.0
func (c *CmdableWrapper) ScriptKill(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "script_kill", nil
	result := c.Cmdable.ScriptKill(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ScriptLoad overrides redis.Cmdable/ScriptLoad to log execution metrics.
//
// @Redis: available since v2.6.0
func (c *CmdableWrapper) ScriptLoad(ctx context.Context, script string) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "script_load", m{"script": script}
	result := c.Cmdable.ScriptLoad(ctx, script)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

/*----- Server-related commands -----*/

// No function ACLXXX for now!

// BgRewriteAOF overrides redis.Cmdable/BgRewriteAOF to log execution metrics.
//
// @Redis: available since v1.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) BgRewriteAOF(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "bg_rewrite_aof", nil
	result := c.Cmdable.BgRewriteAOF(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// BgSave overrides redis.Cmdable/BgSave to log execution metrics.
//
// @Redis: available since v1.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) BgSave(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "bg_save", nil
	result := c.Cmdable.BgSave(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Command overrides redis.Cmdable/Command to log execution metrics.
//
// @Redis: available since v2.8.13
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) Command(ctx context.Context) *redis.CommandsInfoCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "command", nil
	result := c.Cmdable.Command(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// CommandGetKeys overrides redis.Cmdable/CommandGetKeys to log execution metrics.
//
// @Redis: available since v2.8.13
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) CommandGetKeys(ctx context.Context, commands ...interface{}) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "command_get_keys", m{"commands": commands}
	result := c.Cmdable.CommandGetKeys(ctx, commands...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// CommandGetKeysAndFlags overrides redis.Cmdable/CommandGetKeysAndFlags to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) CommandGetKeysAndFlags(ctx context.Context, commands ...interface{}) *redis.KeyFlagsCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "command_get_keys_and_flags", m{"commands": commands}
	result := c.Cmdable.CommandGetKeysAndFlags(ctx, commands...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// CommandList overrides redis.Cmdable/CommandList to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) CommandList(ctx context.Context, filter *redis.FilterBy) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "command_list", m{"args": filter}
	result := c.Cmdable.CommandList(ctx, filter)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ConfigGet overrides redis.Cmdable/ConfigGet to log execution metrics.
//
// @Redis: available since v2.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) ConfigGet(ctx context.Context, parameter string) *redis.MapStringStringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "config_get", m{"parameter": parameter}
	result := c.Cmdable.ConfigGet(ctx, parameter)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ConfigResetStat overrides redis.Cmdable/ConfigResetStat to log execution metrics.
//
// @Redis: available since v2.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) ConfigResetStat(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "config_reset_stat", nil
	result := c.Cmdable.ConfigResetStat(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ConfigRewrite overrides redis.Cmdable/ConfigRewrite to log execution metrics.
//
// @Redis: available since v2.8.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) ConfigRewrite(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "config_rewrite", nil
	result := c.Cmdable.ConfigRewrite(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ConfigSet overrides redis.Cmdable/ConfigSet to log execution metrics.
//
// @Redis: available since v2.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) ConfigSet(ctx context.Context, parameter, value string) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "config_set", m{"parameter": parameter, "value": value}
	result := c.Cmdable.ConfigSet(ctx, parameter, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// DBSize overrides redis.Cmdable/DBSize to log execution metrics.
//
// @Redis: available since v1.0.0
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

// No function Failover for now!

// FlushAll overrides redis.Cmdable/FlushAll to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) FlushAll(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "flush_all", nil
	result := c.Cmdable.FlushAll(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// FlushAllAsync overrides redis.Cmdable/FlushAllAsync to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) FlushAllAsync(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "flush_all", m{"async": true}
	result := c.Cmdable.FlushAllAsync(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// FlushDB overrides redis.Cmdable/FlushDB to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) FlushDB(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "flush_db", nil
	result := c.Cmdable.FlushDB(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// FlushDBAsync overrides redis.Cmdable/FlushDBAsync to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) FlushDBAsync(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "flush_db", m{"async": true}
	result := c.Cmdable.FlushDBAsync(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Info overrides redis.Cmdable/Info to log execution metrics.
//
// @Redis: available since v1.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) Info(ctx context.Context, sections ...string) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "info", m{"sections": sections}
	result := c.Cmdable.Info(ctx, sections...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// LastSave overrides redis.Cmdable/LastSave to log execution metrics.
//
// @Redis: available since v1.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) LastSave(ctx context.Context) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "last_save", nil
	result := c.Cmdable.LastSave(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// MemoryUsage overrides redis.Cmdable/MemoryUsage to log execution metrics.
//
// @Redis: available since v4.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) MemoryUsage(ctx context.Context, key string, samples ...int) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "memory_usage", m{"key": key, "samples": samples}
	result := c.Cmdable.MemoryUsage(ctx, key, samples...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Save overrides redis.Cmdable/Save to log execution metrics.
//
// @Redis: available since v1.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) Save(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "save", nil
	result := c.Cmdable.Save(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Shutdown overrides redis.Cmdable/Shutdown to log execution metrics.
//
// @Redis: available since v1.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) Shutdown(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "shutdown", m{}
	result := c.Cmdable.Shutdown(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ShutdownNoSave overrides redis.Cmdable/ShutdownNoSave to log execution metrics.
//
// @Redis: available since v1.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) ShutdownNoSave(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "shutdown", m{"save": false}
	result := c.Cmdable.ShutdownNoSave(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ShutdownSave overrides redis.Cmdable/ShutdownSave to log execution metrics.
//
// @Redis: available since v1.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) ShutdownSave(ctx context.Context) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "shutdown", m{"save": true}
	result := c.Cmdable.ShutdownSave(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SlaveOf overrides redis.Cmdable/SlaveOf to log execution metrics.
//
// @Redis: available since v1.0.0 / deprecated since v5.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) SlaveOf(ctx context.Context, host, port string) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "slave_of", m{"host": host, "port": port}
	result := c.Cmdable.SlaveOf(ctx, host, port)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SlowLogGet overrides redis.Cmdable/SlowLogGet to log execution metrics.
//
// @Redis: available since v2.2.12
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) SlowLogGet(ctx context.Context, count int64) *redis.SlowLogCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "slowlog_get", m{"count": count}
	result := c.Cmdable.SlowLogGet(ctx, count)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// No function SlowLogLen for now!

// No function SlowLogReset for now!

// No function SwapDb for now!

// No function Sync for now!

// Time overrides redis.Cmdable/Time to log execution metrics.
//
// @Redis: available since v2.6.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) Time(ctx context.Context) *redis.TimeCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "time", nil
	result := c.Cmdable.Time(ctx)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

/*----- Set-related commands -----*/

// SAdd overrides redis.Cmdable/SAdd to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "sadd", m{"key": key, "members": members}
	result := c.Cmdable.SAdd(ctx, key, members...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SCard overrides redis.Cmdable/SCard to log execution metrics.
//
// @Redis: available since v1.0.0
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

// SDiff overrides redis.Cmdable/SDiff to log execution metrics.
//
// @Redis: available since v1.0.0
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

// SDiffStore overrides redis.Cmdable/SDiffStore to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) SDiffStore(ctx context.Context, destKey string, keys ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "sdiff_store", m{"destination": destKey, "keys": keys}
	result := c.Cmdable.SDiffStore(ctx, destKey, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SInter overrides redis.Cmdable/SInter to log execution metrics.
//
// @Redis: available since v1.0.0
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

// SInterCard overrides redis.Cmdable/SInterCard to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) SInterCard(ctx context.Context, limit int64, keys ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "sinter_card", m{"limit": limit, "keys": keys}
	result := c.Cmdable.SInterCard(ctx, limit, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SInterStore overrides redis.Cmdable/SInterStore to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) SInterStore(ctx context.Context, destKey string, keys ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "sinter_store", m{"destination": destKey, "keys": keys}
	result := c.Cmdable.SInterStore(ctx, destKey, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SIsMember overrides redis.Cmdable/SIsMember to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) SIsMember(ctx context.Context, key string, member interface{}) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "sis_member", m{"key": key, "member": member}
	result := c.Cmdable.SIsMember(ctx, key, member)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SMembers overrides redis.Cmdable/SMembers to log execution metrics.
//
// @Redis: available since v1.0.0
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

// SMIsMember overrides redis.Cmdable/SMIsMember to log execution metrics.
//
// @Redis: available since v6.2.0
func (c *CmdableWrapper) SMIsMember(ctx context.Context, key string, members ...interface{}) *redis.BoolSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "smis_member", m{"key": key, "members": members}
	result := c.Cmdable.SMIsMember(ctx, key, members...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SMove overrides redis.Cmdable/SMove to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) SMove(ctx context.Context, srcKey, destKey string, member interface{}) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "smove", m{"source": srcKey, "destination": destKey, "member": member}
	result := c.Cmdable.SMove(ctx, srcKey, destKey, member)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SPop overrides redis.Cmdable/SPop to log execution metrics.
//
// @Redis: available since v1.0.0
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

// SPopN overrides redis.Cmdable/SPopN to log execution metrics.
//
// @Redis: available since v1.0.0
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

// SRandMember overrides redis.Cmdable/SRandMember to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) SRandMember(ctx context.Context, key string) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "srand_member", m{"key": key}
	result := c.Cmdable.SRandMember(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SRandMemberN overrides redis.Cmdable/SRandMemberN to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) SRandMemberN(ctx context.Context, key string, count int64) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "srand_member", m{"key": key, "count": count}
	result := c.Cmdable.SRandMemberN(ctx, key, count)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SRem overrides redis.Cmdable/SRem to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) SRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "srem", m{"key": key, "members": members}
	result := c.Cmdable.SRem(ctx, key, members...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SScan overrides redis.Cmdable/SScan to log execution metrics.
//
// @Redis: available since v2.8.0
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

// SUnion overrides redis.Cmdable/SUnion to log execution metrics.
//
// @Redis: available since v1.0.0
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

// SUnionStore overrides redis.Cmdable/SUnionStore to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) SUnionStore(ctx context.Context, destKey string, keys ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "sunion_store", m{"destination": destKey, "keys": keys}
	result := c.Cmdable.SUnionStore(ctx, destKey, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

/*----- Sorted set-related commands -----*/

// BZMPop overrides redis.Cmdable/BZMPop to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) BZMPop(ctx context.Context, timeout time.Duration, order string, count int64, keys ...string) *redis.ZSliceWithKeyCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "bzmpop", m{"keys": keys, "timeout": timeout, "count": count, "order": order}
	result := c.Cmdable.BZMPop(ctx, timeout, order, count, keys...)
	key, val, err := result.Result()
	cmd.CmdResponse = m{key: val}
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// BZPopMax overrides redis.Cmdable/BZPopMax to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) BZPopMax(ctx context.Context, timeout time.Duration, keys ...string) *redis.ZWithKeyCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "bzpop_max", m{"keys": keys, "timeout": timeout}
	result := c.Cmdable.BZPopMax(ctx, timeout, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// BZPopMin overrides redis.Cmdable/BZPopMin to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) BZPopMin(ctx context.Context, timeout time.Duration, keys ...string) *redis.ZWithKeyCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "bzpop_min", m{"keys": keys, "timeout": timeout}
	result := c.Cmdable.BZPopMin(ctx, timeout, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZAdd overrides redis.Cmdable/ZAdd to log execution metrics.
//
// @Redis: available since v1.2.0
func (c *CmdableWrapper) ZAdd(ctx context.Context, key string, scoresAndMembers ...redis.Z) *redis.IntCmd {
	// c.ZAdd(ctx, key, scoresAndMembers...)
	return c.ZAddArgs(ctx, key, redis.ZAddArgs{Members: scoresAndMembers})
}

// ZAddGT overrides redis.Cmdable/ZAddGT to log execution metrics.
//
// @Redis: available since v1.2.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) ZAddGT(ctx context.Context, key string, scoresAndMembers ...redis.Z) *redis.IntCmd {
	// c.ZAddGT(ctx, key, scoresAndMembers...)
	return c.ZAddArgs(ctx, key, redis.ZAddArgs{GT: true, Members: scoresAndMembers})
}

// ZAddLT overrides redis.Cmdable/ZAddLT to log execution metrics.
//
// @Redis: available since v1.2.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) ZAddLT(ctx context.Context, key string, scoresAndMembers ...redis.Z) *redis.IntCmd {
	// c.ZAddLT(ctx, key, scoresAndMembers...)
	return c.ZAddArgs(ctx, key, redis.ZAddArgs{LT: true, Members: scoresAndMembers})
}

// ZAddNX overrides redis.Cmdable/ZAddNX to log execution metrics.
//
// @Redis: available since v1.2.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) ZAddNX(ctx context.Context, key string, scoresAndMembers ...redis.Z) *redis.IntCmd {
	// c.ZAddNX(ctx, key, scoresAndMembers...)
	return c.ZAddArgs(ctx, key, redis.ZAddArgs{NX: true, Members: scoresAndMembers})
}

// ZAddXX overrides redis.Cmdable/ZAddXX to log execution metrics.
//
// @Redis: available since v1.2.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) ZAddXX(ctx context.Context, key string, scoresAndMembers ...redis.Z) *redis.IntCmd {
	// c.ZAddXX(ctx, key, scoresAndMembers...)
	return c.ZAddArgs(ctx, key, redis.ZAddArgs{XX: true, Members: scoresAndMembers})
}

// ZAddArgs overrides redis.Cmdable/ZAddArgs to log execution metrics.
//
// @GoRedis: replace functions ZAdd, ZAddGT, ZAddLT, ZAddNX, ZAddXX
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) ZAddArgs(ctx context.Context, key string, args redis.ZAddArgs) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zadd", m{"key": key, "scores_members": args.Members,
		"gt": args.GT, "lt": args.LT, "nx": args.NX, "xx": args.XX, "ch": args.Ch}
	result := c.Cmdable.ZAddArgs(ctx, key, args)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZAddArgsIncr overrides redis.Cmdable/ZAddArgsIncr to log execution metrics.
//
// @Redis: available since v1.2.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) ZAddArgsIncr(ctx context.Context, key string, args redis.ZAddArgs) *redis.FloatCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zadd", m{"key": key, "scores_members": args.Members,
		"incr": true, "gt": args.GT, "lt": args.LT, "nx": args.NX, "xx": args.XX, "ch": args.Ch}
	result := c.Cmdable.ZAddArgsIncr(ctx, key, args)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZCard overrides redis.Cmdable/ZCard to log execution metrics.
//
// @Redis: available since v1.2.0
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

// ZCount overrides redis.Cmdable/ZCount to log execution metrics.
//
// @Redis: available since v2.0.0
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

// ZDiff overrides redis.Cmdable/ZDiff to log execution metrics.
//
// @Redis: available since v6.2.0
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

// ZDiffWithScores overrides redis.Cmdable/ZDiffWithScores to log execution metrics.
//
// @Redis: available since v6.2.0
func (c *CmdableWrapper) ZDiffWithScores(ctx context.Context, keys ...string) *redis.ZSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zdiff", m{"keys": keys, "withscores": true}
	result := c.Cmdable.ZDiffWithScores(ctx, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZDiffStore overrides redis.Cmdable/ZDiffStore to log execution metrics.
//
// @Redis: available since v6.2.0
func (c *CmdableWrapper) ZDiffStore(ctx context.Context, destKey string, keys ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zdiff_store", m{"destination": destKey, "keys": keys}
	result := c.Cmdable.ZDiffStore(ctx, destKey, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZIncrBy overrides redis.Cmdable/ZIncrBy to log execution metrics.
//
// @Redis: available since v1.2.0
func (c *CmdableWrapper) ZIncrBy(ctx context.Context, key string, increment float64, member string) *redis.FloatCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zincr_by", m{"key": key, "increment": increment, "member": member}
	result := c.Cmdable.ZIncrBy(ctx, key, increment, member)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZInter overrides redis.Cmdable/ZInter to log execution metrics.
//
// @Redis: available since v6.2.0
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

// ZInterWithScores overrides redis.Cmdable/ZInterWithScores to log execution metrics.
//
// @Redis: available since v6.2.0
func (c *CmdableWrapper) ZInterWithScores(ctx context.Context, store *redis.ZStore) *redis.ZSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zinter", m{"keys": store.Keys, "weights": store.Weights, "aggregate": store.Aggregate, "withscores": true}
	result := c.Cmdable.ZInterWithScores(ctx, store)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZInterCard overrides redis.Cmdable/ZInterCard to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) ZInterCard(ctx context.Context, limit int64, keys ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zinter_card", m{"keys": keys, "limit": limit}
	result := c.Cmdable.ZInterCard(ctx, limit, keys...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZInterStore overrides redis.Cmdable/ZInterStore to log execution metrics.
//
// @Redis: available since v2.0.0
func (c *CmdableWrapper) ZInterStore(ctx context.Context, destKey string, store *redis.ZStore) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zinter_store", m{"destination": destKey, "keys": store.Keys, "weights": store.Weights, "aggregate": store.Aggregate}
	result := c.Cmdable.ZInterStore(ctx, destKey, store)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZLexCount overrides redis.Cmdable/ZLexCount to log execution metrics.
//
// @Redis: available since v2.8.9
func (c *CmdableWrapper) ZLexCount(ctx context.Context, key, min, max string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zlex_count", m{"key": key, "min": min, "max": max}
	result := c.Cmdable.ZLexCount(ctx, key, min, max)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZMPop overrides redis.Cmdable/ZMPop to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) ZMPop(ctx context.Context, order string, count int64, keys ...string) *redis.ZSliceWithKeyCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zmpop", m{"order": order, "count": count, "keys": keys}
	result := c.Cmdable.ZMPop(ctx, order, count, keys...)
	key, val, err := result.Result()
	cmd.CmdResponse = m{key: val}
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZMScore overrides redis.Cmdable/ZMScore to log execution metrics.
//
// @Redis: available since v6.2.0
func (c *CmdableWrapper) ZMScore(ctx context.Context, key string, members ...string) *redis.FloatSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zmscore", m{"key": key, "members": members}
	result := c.Cmdable.ZMScore(ctx, key, members...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZPopMax overrides redis.Cmdable/ZPopMax to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) ZPopMax(ctx context.Context, key string, count ...int64) *redis.ZSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zpop_max", m{"key": key, "count": count}
	result := c.Cmdable.ZPopMax(ctx, key, count...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZPopMin overrides redis.Cmdable/ZPopMin to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) ZPopMin(ctx context.Context, key string, count ...int64) *redis.ZSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zpop_min", m{"key": key, "count": count}
	result := c.Cmdable.ZPopMin(ctx, key, count...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRandMember overrides redis.Cmdable/ZRandMember to log execution metrics.
//
// @Redis: available since v6.2.0
func (c *CmdableWrapper) ZRandMember(ctx context.Context, key string, count int) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrand_member", m{"key": key, "count": count}
	result := c.Cmdable.ZRandMember(ctx, key, count)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRandMemberWithScores overrides redis.Cmdable/ZRandMemberWithScores to log execution metrics.
//
// @Redis: available since v6.2.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) ZRandMemberWithScores(ctx context.Context, key string, count int) *redis.ZSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrand_member", m{"key": key, "count": count, "withscores": true}
	result := c.Cmdable.ZRandMemberWithScores(ctx, key, count)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRange overrides redis.Cmdable/ZRange to log execution metrics.
//
// @Redis: available since v1.2.0
func (c *CmdableWrapper) ZRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	// c.ZRange(ctx, key, start, stop)
	return c.ZRangeArgs(ctx, redis.ZRangeArgs{Key: key, Start: start, Stop: stop})
}

// ZRangeWithScores overrides redis.Cmdable/ZRangeWithScores to log execution metrics.
//
// @Redis: available since v1.2.0
func (c *CmdableWrapper) ZRangeWithScores(ctx context.Context, key string, start, stop int64) *redis.ZSliceCmd {
	// c.ZRangeWithScores(ctx, key, start, stop)
	return c.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{Key: key, Start: start, Stop: stop})
}

// ZRangeByLex overrides redis.Cmdable/ZRangeByLex to log execution metrics.
//
// @Redis: available since v2.8.9 / deprecated since v6.2.0
func (c *CmdableWrapper) ZRangeByLex(ctx context.Context, key string, opts *redis.ZRangeBy) *redis.StringSliceCmd {
	// c.ZRangeByLex(ctx, key, opts)
	return c.ZRangeArgs(ctx, redis.ZRangeArgs{Key: key, Start: opts.Min, Stop: opts.Max, Offset: opts.Offset, Count: opts.Count, ByLex: true})
}

// ZRangeByScore overrides redis.Cmdable/ZRangeByScore to log execution metrics.
//
// @Redis: available since v1.0.5 / deprecated since v6.2.0
func (c *CmdableWrapper) ZRangeByScore(ctx context.Context, key string, opts *redis.ZRangeBy) *redis.StringSliceCmd {
	// c.ZRangeByScore(ctx, key, opts)
	return c.ZRangeArgs(ctx, redis.ZRangeArgs{Key: key, Start: opts.Min, Stop: opts.Max, Offset: opts.Offset, Count: opts.Count, ByScore: true})
}

// ZRangeByScoreWithScores overrides redis.Cmdable/ZRangeByScoreWithScores to log execution metrics.
//
// @Redis: available since v1.0.5 / deprecated since v6.2.0
func (c *CmdableWrapper) ZRangeByScoreWithScores(ctx context.Context, key string, opts *redis.ZRangeBy) *redis.ZSliceCmd {
	// c.ZRangeByScoreWithScores(ctx, key, opts)
	return c.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{Key: key, Start: opts.Min, Stop: opts.Max, Offset: opts.Offset, Count: opts.Count, ByScore: true})
}

// ZRangeArgs overrides redis.Cmdable/ZRangeArgs to log execution metrics.
//
// @GoRedis: replace functions ZRange, ZRangeByLex, ZRangeByScore
func (c *CmdableWrapper) ZRangeArgs(ctx context.Context, args redis.ZRangeArgs) *redis.StringSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrange", m{"key": args.Key, "rev": args.Rev,
		"byscore": args.ByScore, "bylex": args.ByLex,
		"start": args.Start, "stop": args.Stop, "offset": args.Offset, "count": args.Count,
	}
	result := c.Cmdable.ZRangeArgs(ctx, args)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRangeArgsWithScores overrides redis.Cmdable/ZRangeArgsWithScores to log execution metrics.
//
// @GoRedis: replace functions ZRangeWithScores, ZRangeByScoreWithScores
func (c *CmdableWrapper) ZRangeArgsWithScores(ctx context.Context, args redis.ZRangeArgs) *redis.ZSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrange", m{"key": args.Key, "rev": args.Rev,
		"byscore": args.ByScore, "bylex": args.ByLex, "withscores": true,
		"start": args.Start, "stop": args.Stop, "offset": args.Offset, "count": args.Count,
	}
	result := c.Cmdable.ZRangeArgsWithScores(ctx, args)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRangeStore overrides redis.Cmdable/ZRangeStore to log execution metrics.
//
// @Redis: available since v6.2.0
func (c *CmdableWrapper) ZRangeStore(ctx context.Context, destKey string, args redis.ZRangeArgs) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrange_store", m{"source": args.Key, "destination": destKey, "rev": args.Rev,
		"byscore": args.ByScore, "bylex": args.ByLex,
		"start": args.Start, "stop": args.Stop, "offset": args.Offset, "count": args.Count,
	}
	result := c.Cmdable.ZRangeStore(ctx, destKey, args)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRank overrides redis.Cmdable/ZRank to log execution metrics.
//
// @Redis: available since v2.0.0
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

// ZRankWithScore overrides redis.Cmdable/ZRankWithScore to log execution metrics.
//
// @Redis: available since v2.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) ZRankWithScore(ctx context.Context, key, member string) *redis.RankWithScoreCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrank", m{"key": key, "member": member, "withscore": true}
	result := c.Cmdable.ZRankWithScore(ctx, key, member)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRem overrides redis.Cmdable/ZRem to log execution metrics.
//
// @Redis: available since v1.2.0
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

// ZRemRangeByLex overrides redis.Cmdable/ZRemRangeByLex to log execution metrics.
//
// @Redis: available since v2.8.9
func (c *CmdableWrapper) ZRemRangeByLex(ctx context.Context, key, min, max string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrem_range_by_lex", m{"key": key, "min": min, "max": max}
	result := c.Cmdable.ZRemRangeByLex(ctx, key, min, max)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRemRangeByRank overrides redis.Cmdable/ZRemRangeByRank to log execution metrics.
//
// @Redis: available since v2.0.0
func (c *CmdableWrapper) ZRemRangeByRank(ctx context.Context, key string, start, stop int64) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrem_range_by_rank", m{"key": key, "start": start, "stop": stop}
	result := c.Cmdable.ZRemRangeByRank(ctx, key, start, stop)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRemRangeByScore overrides redis.Cmdable/ZRemRangeByScore to log execution metrics.
//
// @Redis: available since v1.2.0
func (c *CmdableWrapper) ZRemRangeByScore(ctx context.Context, key, min, max string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrem_range_by_score", m{"key": key, "min": min, "max": max}
	result := c.Cmdable.ZRemRangeByScore(ctx, key, min, max)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRevRange overrides redis.Cmdable/ZRevRange to log execution metrics.
//
// @Redis: available since v1.2.0 / deprecated since v6.2.0
func (c *CmdableWrapper) ZRevRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	// c.ZRevRange(ctx, key, start, stop)
	return c.ZRangeArgs(ctx, redis.ZRangeArgs{Key: key, Start: start, Stop: stop, Rev: true})
}

// ZRevRangeWithScores overrides redis.Cmdable/ZRevRangeWithScores to log execution metrics.
//
// @Redis: available since v1.2.0 / deprecated since v6.2.0
func (c *CmdableWrapper) ZRevRangeWithScores(ctx context.Context, key string, start, stop int64) *redis.ZSliceCmd {
	// c.ZRevRangeWithScores(ctx, key, start, stop)
	return c.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{Key: key, Start: start, Stop: stop, Rev: true})
}

// ZRevRangeByLex overrides redis.Cmdable/ZRevRangeByLex to log execution metrics.
//
// @Redis: available since v2.8.9 / deprecated since v6.2.0
func (c *CmdableWrapper) ZRevRangeByLex(ctx context.Context, key string, opts *redis.ZRangeBy) *redis.StringSliceCmd {
	// c.ZRevRangeByLex(ctx, key, opts)
	return c.ZRangeArgs(ctx, redis.ZRangeArgs{Key: key, Start: opts.Min, Stop: opts.Max, Offset: opts.Offset, Count: opts.Count, Rev: true, ByLex: true})
}

// ZRevRangeByScore overrides redis.Cmdable/ZRevRangeByScore to log execution metrics.
//
// @Redis: available since v2.2.0 / deprecated since v6.2.0
func (c *CmdableWrapper) ZRevRangeByScore(ctx context.Context, key string, opts *redis.ZRangeBy) *redis.StringSliceCmd {
	// c.ZRevRangeByScore(ctx, key, opts)
	return c.ZRangeArgs(ctx, redis.ZRangeArgs{Key: key, Start: opts.Min, Stop: opts.Max, Offset: opts.Offset, Count: opts.Count, Rev: true, ByScore: true})
}

// ZRevRangeByScoreWithScores overrides redis.Cmdable/ZRevRangeByScoreWithScores to log execution metrics.
//
// @Redis: available since v2.2.0 / deprecated since v6.2.0
func (c *CmdableWrapper) ZRevRangeByScoreWithScores(ctx context.Context, key string, opts *redis.ZRangeBy) *redis.ZSliceCmd {
	// c.ZRevRangeByScoreWithScores(ctx, key, opts)
	return c.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{Key: key, Start: opts.Min, Stop: opts.Max, Offset: opts.Offset, Count: opts.Count, Rev: true, ByScore: true})
}

// ZRevRank overrides redis.Cmdable/ZRevRank to log execution metrics.
//
// @Redis: available since v2.0.0
func (c *CmdableWrapper) ZRevRank(ctx context.Context, key string, member string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrev_rank", m{"key": key, "member": member}
	result := c.Cmdable.ZRevRank(ctx, key, member)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZRevRankWithScore overrides redis.Cmdable/ZRevRankWithScore to log execution metrics.
//
// @Redis: available since v2.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) ZRevRankWithScore(ctx context.Context, key string, member string) *redis.RankWithScoreCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zrev_rank", m{"key": key, "member": member, "withscore": true}
	result := c.Cmdable.ZRevRankWithScore(ctx, key, member)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZScan overrides redis.Cmdable/ZScan to log execution metrics.
//
// @Redis: available since v2.0.0
func (c *CmdableWrapper) ZScan(ctx context.Context, key string, cursor uint64, pattern string, count int64) *redis.ScanCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zscan", m{"key": key, "cursor": cursor, "pattern": pattern, "count": count}
	result := c.Cmdable.ZScan(ctx, key, cursor, pattern, count)
	val, _, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZScore overrides redis.Cmdable/ZScore to log execution metrics.
//
// @Redis: available since v1.2.0
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

// ZUnion overrides redis.Cmdable/ZUnion to log execution metrics.
//
// @Redis: available since v6.2.0
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

// ZUnionWithScores overrides redis.Cmdable/ZUnionWithScores to log execution metrics.
//
// @Redis: available since v6.2.0
func (c *CmdableWrapper) ZUnionWithScores(ctx context.Context, store redis.ZStore) *redis.ZSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zunion", m{"keys": store.Keys, "aggregate": store.Aggregate, "weights": store.Weights, "withscores": true}
	result := c.Cmdable.ZUnionWithScores(ctx, store)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// ZUnionStore overrides redis.Cmdable/ZUnionStore to log execution metrics.
//
// @Redis: available since v2.0.0
func (c *CmdableWrapper) ZUnionStore(ctx context.Context, destKey string, store *redis.ZStore) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "zunion_store", m{"destination": destKey, "keys": store.Keys, "aggregate": store.Aggregate, "weights": store.Weights}
	result := c.Cmdable.ZUnionStore(ctx, destKey, store)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

/*----- Stream-related commands -----*/

// XAck overrides redis.Cmdable/XAck to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XAck(ctx context.Context, key, group string, ids ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xack", m{"key": key, "group": group, "ids": ids}
	result := c.Cmdable.XAck(ctx, key, group, ids...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XAdd overrides redis.Cmdable/XAdd to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XAdd(ctx context.Context, args *redis.XAddArgs) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xadd", m{"key": args.Stream, "no_mk_stream": args.NoMkStream,
		"max_len": args.MaxLen, "min_id": args.MinID, "approx": args.Approx, "count": args.Limit,
		"id": args.ID, "fields_values": args.Values}
	result := c.Cmdable.XAdd(ctx, args)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XAutoClaim overrides redis.Cmdable/XAutoClaim to log execution metrics.
//
// @Redis: available since v6.2.0
func (c *CmdableWrapper) XAutoClaim(ctx context.Context, args *redis.XAutoClaimArgs) *redis.XAutoClaimCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xauto_claim", m{"key": args.Stream, "group": args.Group, "consumer": args.Consumer,
		"min_idle_time": args.MinIdle, "start": args.Start, "count": args.Count}
	result := c.Cmdable.XAutoClaim(ctx, args)
	val, start, err := result.Result()
	cmd.CmdResponse = m{"messages": val, "start": start}
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XAutoClaimJustID overrides redis.Cmdable/XAutoClaimJustID to log execution metrics.
//
// @Redis: available since v6.2.0
func (c *CmdableWrapper) XAutoClaimJustID(ctx context.Context, args *redis.XAutoClaimArgs) *redis.XAutoClaimJustIDCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xauto_claim", m{"key": args.Stream, "group": args.Group, "consumer": args.Consumer,
		"min_idle_time": args.MinIdle, "start": args.Start, "count": args.Count, "justid": true}
	result := c.Cmdable.XAutoClaimJustID(ctx, args)
	val, start, err := result.Result()
	cmd.CmdResponse = m{"messages": val, "start": start}
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XClaim overrides redis.Cmdable/XClaim to log execution metrics.
//
// @Redis: available since v5.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) XClaim(ctx context.Context, args *redis.XClaimArgs) *redis.XMessageSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xauto_claim", m{"key": args.Stream, "group": args.Group, "consumer": args.Consumer,
		"min_idle_time": args.MinIdle, "messages": args.Messages}
	result := c.Cmdable.XClaim(ctx, args)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XDel overrides redis.Cmdable/XDel to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XDel(ctx context.Context, key string, ids ...string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xdel", m{"key": key, "ids": ids}
	result := c.Cmdable.XDel(ctx, key, ids...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XGroupCreate overrides redis.Cmdable/XGroupCreate to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XGroupCreate(ctx context.Context, key, group, id string) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xgroup_create", m{"key": key, "group": group, "id": id}
	result := c.Cmdable.XGroupCreate(ctx, key, group, id)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XGroupCreateMkStream overrides redis.Cmdable/XGroupCreateMkStream to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XGroupCreateMkStream(ctx context.Context, key, group, id string) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xgroup_create", m{"key": key, "group": group, "id": id, "mk_stream": true}
	result := c.Cmdable.XGroupCreateMkStream(ctx, key, group, id)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XGroupCreateConsumer overrides redis.Cmdable/XGroupCreateConsumer to log execution metrics.
//
// @Redis: available since v6.2.0
func (c *CmdableWrapper) XGroupCreateConsumer(ctx context.Context, key, group, consumer string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xgroup_create_consumer", m{"key": key, "group": group, "consumer": consumer}
	result := c.Cmdable.XGroupCreateConsumer(ctx, key, group, consumer)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XGroupDelConsumer overrides redis.Cmdable/XGroupDelConsumer to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XGroupDelConsumer(ctx context.Context, key, group, consumer string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatOther, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xgroup_del_consumer", m{"key": key, "group": group, "consumer": consumer}
	result := c.Cmdable.XGroupDelConsumer(ctx, key, group, consumer)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XGroupDestroy overrides redis.Cmdable/XGroupDestroy to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XGroupDestroy(ctx context.Context, key, group string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xgroup_destroy", m{"key": key, "group": group}
	result := c.Cmdable.XGroupDestroy(ctx, key, group)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XGroupSetID overrides redis.Cmdable/XGroupSetID to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XGroupSetID(ctx context.Context, key, group, id string) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDDL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xgroup_set_id", m{"key": key, "group": group, "id": id}
	result := c.Cmdable.XGroupSetID(ctx, key, group, id)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XInfoConsumers overrides redis.Cmdable/XInfoConsumers to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XInfoConsumers(ctx context.Context, key, group string) *redis.XInfoConsumersCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xinfo_consumers", m{"key": key, "group": group}
	result := c.Cmdable.XInfoConsumers(ctx, key, group)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XInfoGroups overrides redis.Cmdable/XInfoGroups to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XInfoGroups(ctx context.Context, key string) *redis.XInfoGroupsCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xinfo_groups", m{"key": key}
	result := c.Cmdable.XInfoGroups(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XInfoStream overrides redis.Cmdable/XInfoStream to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XInfoStream(ctx context.Context, key string) *redis.XInfoStreamCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xinfo_stream", m{"key": key}
	result := c.Cmdable.XInfoStream(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XInfoStreamFull overrides redis.Cmdable/XInfoStreamFull to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XInfoStreamFull(ctx context.Context, key string, count int) *redis.XInfoStreamFullCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xinfo_stream", m{"key": key, "full": true, "count": count}
	result := c.Cmdable.XInfoStreamFull(ctx, key, count)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XLen overrides redis.Cmdable/XLen to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XLen(ctx context.Context, key string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xlen", m{"key": key}
	result := c.Cmdable.XLen(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XPending overrides redis.Cmdable/XPending to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XPending(ctx context.Context, key, group string) *redis.XPendingCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xpending", m{"key": key, "group": group}
	result := c.Cmdable.XPending(ctx, key, group)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XPendingExt overrides redis.Cmdable/XPendingExt to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XPendingExt(ctx context.Context, args *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xpending", m{"key": args.Stream, "group": args.Group, "consumer": args.Consumer,
		"min_idle_time": args.Idle, "start": args.Start, "end": args.End, "count": args.Count}
	result := c.Cmdable.XPendingExt(ctx, args)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XRange overrides redis.Cmdable/XRange to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XRange(ctx context.Context, key, start, end string) *redis.XMessageSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xrange", m{"key": key, "start": start, "end": end}
	result := c.Cmdable.XRange(ctx, key, start, end)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XRangeN overrides redis.Cmdable/XRangeN to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XRangeN(ctx context.Context, key, start, end string, count int64) *redis.XMessageSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xrange", m{"key": key, "start": start, "end": end, "count": count}
	result := c.Cmdable.XRangeN(ctx, key, start, end, count)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XRead overrides redis.Cmdable/XRead to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XRead(ctx context.Context, args *redis.XReadArgs) *redis.XStreamSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xread", m{"keys": args.Streams, "block_milliseconds": args.Block, "count": args.Count}
	result := c.Cmdable.XRead(ctx, args)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XReadGroup overrides redis.Cmdable/XReadGroup to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XReadGroup(ctx context.Context, args *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xread_group", m{"group": args.Group, "consumer": args.Consumer, "keys": args.Streams,
		"block_milliseconds": args.Block, "count": args.Count, "noack": args.NoAck}
	result := c.Cmdable.XReadGroup(ctx, args)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XRevRange overrides redis.Cmdable/XRevRange to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XRevRange(ctx context.Context, key, start, end string) *redis.XMessageSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xrev_range", m{"key": key, "start": start, "end": end}
	result := c.Cmdable.XRevRange(ctx, key, start, end)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XRevRangeN overrides redis.Cmdable/XRevRangeN to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XRevRangeN(ctx context.Context, key, start, end string, count int64) *redis.XMessageSliceCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xrev_range", m{"key": key, "start": start, "end": end, "count": count}
	result := c.Cmdable.XRevRangeN(ctx, key, start, end, count)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// No function XSetID for now!

// XTrimMaxLen overrides redis.Cmdable/XTrimMaxLen to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XTrimMaxLen(ctx context.Context, key string, maxLen int64) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xtrim", m{"key": key, "maxlen": maxLen}
	result := c.Cmdable.XTrimMaxLen(ctx, key, maxLen)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XTrimMaxLenApprox overrides redis.Cmdable/XTrimMaxLenApprox to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XTrimMaxLenApprox(ctx context.Context, key string, maxLen, limit int64) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xtrim", m{"key": key, "maxlen": maxLen, "approx": true, "count": limit}
	result := c.Cmdable.XTrimMaxLenApprox(ctx, key, maxLen, limit)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XTrimMinID overrides redis.Cmdable/XTrimMinID to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XTrimMinID(ctx context.Context, key, minId string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xtrim", m{"key": key, "minid": minId}
	result := c.Cmdable.XTrimMinID(ctx, key, minId)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// XTrimMinIDApprox overrides redis.Cmdable/XTrimMinIDApprox to log execution metrics.
//
// @Redis: available since v5.0.0
func (c *CmdableWrapper) XTrimMinIDApprox(ctx context.Context, key, minId string, limit int64) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "xtrim", m{"key": key, "minid": minId, "approx": true, "limit": limit}
	result := c.Cmdable.XTrimMinIDApprox(ctx, key, minId, limit)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

/*----- String-related commands -----*/

// Append overrides redis.Cmdable/Append to log execution metrics.
//
// @Redis: available since v2.0.0
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

// Decr overrides redis.Cmdable/Decr to log execution metrics.
//
// @Redis: available since v1.0.0
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

// DecrBy overrides redis.Cmdable/DecrBy to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) DecrBy(ctx context.Context, key string, value int64) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "decr_by", m{"key": key, "decrement": value}
	result := c.Cmdable.DecrBy(ctx, key, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Get overrides redis.Cmdable/Get to log execution metrics.
//
// @Redis: available since v1.0.0
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

// GetDel overrides redis.Cmdable/GetDel to log execution metrics.
//
// @Redis: available since v6.2.0
func (c *CmdableWrapper) GetDel(ctx context.Context, key string) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "get_del", m{"key": key}
	result := c.Cmdable.GetDel(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// GetEx overrides redis.Cmdable/GetEx to log execution metrics.
//
// @Redis: available since v6.2.0
func (c *CmdableWrapper) GetEx(ctx context.Context, key string, expiration time.Duration) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "get_ex", m{"key": key, "expiration": expiration}
	result := c.Cmdable.GetEx(ctx, key, expiration)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// GetRange overrides redis.Cmdable/GetRange to log execution metrics.
//
// @Redis: available since v2.4.0
func (c *CmdableWrapper) GetRange(ctx context.Context, key string, start, end int64) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "get_range", m{"key": key, "start": start, "end": end}
	result := c.Cmdable.GetRange(ctx, key, start, end)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// GetSet overrides redis.Cmdable/GetSet to log execution metrics.
//
// @Redis: available since v1.0.0 / deprecated since v6.2.0
func (c *CmdableWrapper) GetSet(ctx context.Context, key string, value interface{}) *redis.StringCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "get_set", m{"key": key, "value": value}
	result := c.Cmdable.GetSet(ctx, key, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// Incr overrides redis.Cmdable/Incr to log execution metrics.
//
// @Redis: available since v1.0.0
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

// IncrBy overrides redis.Cmdable/IncrBy to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) IncrBy(ctx context.Context, key string, value int64) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "incr_by", m{"key": key, "increment": value}
	result := c.Cmdable.IncrBy(ctx, key, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// IncrByFloat overrides redis.Cmdable/IncrByFloat to log execution metrics.
//
// @Redis: available since v2.6.0
func (c *CmdableWrapper) IncrByFloat(ctx context.Context, key string, value float64) *redis.FloatCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "incr_by_float", m{"key": key, "increment": value}
	result := c.Cmdable.IncrByFloat(ctx, key, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// LCS overrides redis.Cmdable/LCS to log execution metrics.
//
// @Redis: available since v7.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) LCS(ctx context.Context, query *redis.LCSQuery) *redis.LCSCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "lcs", m{"key1": query.Key1, "key2": query.Key2, "len": query.Len, "idx": query.Idx,
		"min_match_len": query.MinMatchLen, "with_match_len": query.WithMatchLen}
	result := c.Cmdable.LCS(ctx, query)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// MGet overrides redis.Cmdable/MGet to log execution metrics.
//
// @Redis: available since v1.0.0
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

// MSet overrides redis.Cmdable/MSet to log execution metrics.
//
// @Redis: available since v1.0.1
func (c *CmdableWrapper) MSet(ctx context.Context, keysValues ...interface{}) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "mset", m{"keys_values": keysValues}
	result := c.Cmdable.MSet(ctx, keysValues...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// MSetNX overrides redis.Cmdable/MSetNX to log execution metrics.
//
// @Redis: available since v1.0.1
func (c *CmdableWrapper) MSetNX(ctx context.Context, keysValues ...interface{}) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "mset_nx", m{"keys_values": keysValues}
	result := c.Cmdable.MSetNX(ctx, keysValues...)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// No function PSetEx available! Use SetEX

// Set overrides redis.Cmdable/Set to log execution metrics.
//
// @Redis: available since v1.0.0
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

// SetArgs overrides redis.Cmdable/SetArgs to log execution metrics.
//
// @Redis: available since v1.0.0
//
// @Available since <<VERSION>>
func (c *CmdableWrapper) SetArgs(ctx context.Context, key string, value interface{}, args redis.SetArgs) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "set", m{"key": key, "value": value, "args": args}
	result := c.Cmdable.SetArgs(ctx, key, value, args)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SetEX overrides redis.Cmdable/SetEX to log execution metrics.
//
// @Redis: available since v2.0.0 / deprecated since v2.6.12
func (c *CmdableWrapper) SetEX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "set_ex", m{"key": key, "value": value, "expiration": expiration}
	result := c.Cmdable.SetEx(ctx, key, value, expiration)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SetNX overrides redis.Cmdable/SetNX to log execution metrics.
//
// @Redis: available since v1.0.0 / deprecated since v2.6.12
func (c *CmdableWrapper) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "set_nx", m{"key": key, "value": value, "expiration": expiration}
	result := c.Cmdable.SetNX(ctx, key, value, expiration)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SetXX overrides redis.Cmdable/SetXX to log execution metrics.
//
// @Redis: available since v1.0.0
func (c *CmdableWrapper) SetXX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "set", m{"key": key, "value": value, "expiration": expiration, "xx": true}
	result := c.Cmdable.SetXX(ctx, key, value, expiration)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// SetRange overrides redis.Cmdable/SetRange to log execution metrics.
//
// @Redis: available since v2.2.0
func (c *CmdableWrapper) SetRange(ctx context.Context, key string, offset int64, value string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDML, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "set_range", m{"key": key, "offset": offset, "value": value}
	result := c.Cmdable.SetRange(ctx, key, offset, value)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// StrLen overrides redis.Cmdable/StrLen to log execution metrics.
//
// @Redis: available since v2.2.0
func (c *CmdableWrapper) StrLen(ctx context.Context, key string) *redis.IntCmd {
	cmd := c.rc.NewCmdExecInfo()
	defer func() {
		c.rc.LogMetrics(prom.MetricsCatAll, cmd)
		c.rc.LogMetrics(prom.MetricsCatDQL, cmd)
	}()
	cmd.CmdName, cmd.CmdRequest = "strlen", m{"key": key}
	result := c.Cmdable.StrLen(ctx, key)
	val, err := result.Result()
	cmd.CmdResponse = val
	cmd.EndWithCostAsExecutionTime(prom.CmdResultOk, prom.CmdResultError, err)
	return result
}

// No function SubStr available! Use GetRange

/*----- Transaction-related commands -----*/

// TODO
