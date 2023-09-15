package goredis

import (
	"context"
	"testing"
)

func TestRedisStruct_RedisInfo(t *testing.T) {
	testName := "TestRedisStruct_RedisInfo"
	teardownTest := setupTest(t, testName, _setupTestRedisProxy, _teardownTestRedisProxy)
	defer teardownTest(t)
	for _, tc := range _testList {
		t.Run(tc, func(t *testing.T) {
			_, c := _getRedisConnectAndCmdable(tc, "mykey")
			resultInfo := c.Info(context.TODO())
			if resultInfo.Err() != nil {
				t.Fatalf("%s failed: %s", testName, resultInfo.Err())
			}
			redisInfo := ParseRedisInfo(resultInfo.Val())
			infoSectionServer := redisInfo.GetSection("Server")
			if infoSectionServer == nil {
				t.Fatalf("%s failed: nil", testName)
			}
		})
	}
}
