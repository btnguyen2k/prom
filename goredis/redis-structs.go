package goredis

import (
	"github.com/btnguyen2k/consu/semver"
	"regexp"
	"strings"
)

var (
	v6_0_0 = semver.ParseSemver("6.0.0")
	v6_2_0 = semver.ParseSemver("6.2.0")
	v7_0_0 = semver.ParseSemver("7.0.0")
	v7_2_0 = semver.ParseSemver("7.2.0")
)

// InfoRedisSection captures a section block from the output of command INFO.
type InfoRedisSection map[string]string

type infoRedis map[string]InfoRedisSection

func (ir infoRedis) EnsureSection(key string) InfoRedisSection {
	section, ok := ir[key]
	if !ok {
		section = make(InfoRedisSection)
		ir[key] = section
	}
	return section
}

var (
	reLines = regexp.MustCompile(`[\r\n]+`)
)

// ParseRedisInfo parses the output from a success call of Redis command INFO.
//
// @Available since <<VERSION>>
func ParseRedisInfo(input string) *RedisInfo {
	lines := reLines.Split(input, -1)
	info := make(infoRedis)
	var infoSection InfoRedisSection
	for _, l := range lines {
		line := strings.TrimSpace(l)
		if strings.HasPrefix(line, "#") {
			infoSection = info.EnsureSection(strings.TrimSpace(line[1:]))
		} else if line != "" {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				infoSection[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}
	}
	return &RedisInfo{
		info: info,
	}
}

// RedisInfo encapsulates the information about a Redis instance (e.g. output from Redis command INFO).
//
// @See https://redis.io/commands/INFO
//
// @Available since <<VERSION>>
type RedisInfo struct {
	info infoRedis
}

// GetSection returns a section block from the output of command INFO.
func (ri *RedisInfo) GetSection(key string) InfoRedisSection {
	return ri.info[key]
}
