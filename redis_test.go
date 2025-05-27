package belajargolangredis

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

var client = redis.NewClient(&redis.Options{
	Addr: "localhost:6379",
	DB:   0,
})

func TestConnection(t *testing.T) {
	assert.NotNil(t, client)

	err := client.Close()
	assert.Nil(t, err)
}

var ctx = context.Background()

func TestPing(t *testing.T) {
	result, err := client.Ping(ctx).Result()
	assert.Nil(t, err)
	assert.Equal(t, "PONG", result)
}

func TestString(t *testing.T) {
	client.SetEx(ctx, "name", "Salman Seif", 3*time.Second)

	result, err := client.Get(ctx, "name").Result()
	assert.Nil(t, err)
	assert.Equal(t, "Salman Seif", result)

	time.Sleep(4 * time.Second)
	result, err = client.Get(ctx, "name").Result()
	assert.NotNil(t, err)
}

func TestList(t *testing.T) {
	client.RPush(ctx, "name", "Salman")
	client.RPush(ctx, "name", "Seif")

	assert.Equal(t, "Salman", client.LPop(ctx, "name").Val())
	assert.Equal(t, "Seif", client.LPop(ctx, "name").Val())
}

func TestSet(t *testing.T) {
	client.SAdd(ctx, "name", "Salman")
	client.SAdd(ctx, "name", "Seif")

	assert.Equal(t, 2, int(client.SCard(ctx, "name").Val()))
	assert.Equal(t, []string{"Salman", "Seif"}, client.SMembers(ctx, "name").Val())
}

func TestSortedSet(t *testing.T) {
	client.ZAdd(ctx, "scores", redis.Z{Score: 100, Member: "Salman"})
	client.ZAdd(ctx, "scores", redis.Z{Score: 200, Member: "Seif"})

	assert.Equal(t, []string{"Salman", "Seif"}, client.ZRange(ctx, "scores", 0, -1).Val())

	assert.Equal(t, "Salman", client.ZPopMin(ctx, "scores").Val()[0].Member)
	assert.Equal(t, "Seif", client.ZPopMin(ctx, "scores").Val()[0].Member)
}

func TestHash(t *testing.T) {
	client.HSet(ctx, "user:1", "id", "1")
	client.HSet(ctx, "user:1", "name", "Salman")
	client.HSet(ctx, "user:1", "email", "salman@example.com")

	user := client.HGetAll(ctx, "user:1").Val()
	assert.Equal(t, "1", user["id"])
	assert.Equal(t, "Salman", user["name"])
	assert.Equal(t, "salman@example.com", user["email"])

	client.Del(ctx, "user:1")
}

func TestGeoPoint(t *testing.T) {
	client.GeoAdd(ctx, "sellers", &redis.GeoLocation{
		Name:      "Toko A",
		Longitude: 109.040218,
		Latitude:  -6.873069,
	})
	client.GeoAdd(ctx, "sellers", &redis.GeoLocation{
		Name:      "Toko B",
		Longitude: 109.060972,
		Latitude:  -6.876426,
	})

	distance := client.GeoDist(ctx, "sellers", "Toko A", "Toko B", "km").Val()
	assert.Equal(t, 2.3221, distance)

	sellers := client.GeoSearch(ctx, "sellers", &redis.GeoSearchQuery{
		Longitude:  109.012625,
		Latitude:   -6.865635,
		Radius:     5,
		RadiusUnit: "km",
	}).Val()

	assert.Equal(t, []string{"Toko A"}, sellers)
}

func TestHyperLogLog(t *testing.T) {
	client.PFAdd(ctx, "visitors", "salman", "seif")

	assert.Equal(t, int64(2), client.PFCount(ctx, "visitors").Val())
}

func TestPipeline(t *testing.T) {
	client.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.SetEx(ctx, "name", "Salman", time.Second*5)
		pipeliner.SetEx(ctx, "address", "Indonesia", time.Second*5)

		return nil
	})

	assert.Equal(t, "Salman", client.Get(ctx, "name").Val())
	assert.Equal(t, "Indonesia", client.Get(ctx, "address").Val())
}

func TestTransaction(t *testing.T) {
	client.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.SetEx(ctx, "name", "Salman", time.Second*5)
		pipeliner.SetEx(ctx, "address", "Indonesia", time.Second*5)

		return nil
	})

	assert.Equal(t, "Salman", client.Get(ctx, "name").Val())
	assert.Equal(t, "Indonesia", client.Get(ctx, "address").Val())
}

func TestPublishStream(t *testing.T) {
	for i := 0; i < 10; i++ {
		err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: "members",
			Values: map[string]interface{}{
				"name":    "salman",
				"address": "indonesia",
			},
		}).Err()
		assert.Nil(t, err)
	}
}

func TestCreateConsumerGroup(t *testing.T) {
	client.XGroupCreate(ctx, "members", "group-1", "0")
	client.XGroupCreateConsumer(ctx, "members", "group-1", "consumer-1")
	client.XGroupCreateConsumer(ctx, "members", "group-1", "consumer-2")
}

func TestGetStream(t *testing.T) {
	streams := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "group-1",
		Consumer: "consumer-1",
		Streams:  []string{"members", ">"},
		Count:    2,
		Block:    5 * time.Second,
	}).Val()

	for _, stream := range streams {
		for _, message := range stream.Messages {
			fmt.Println(message.ID)
			fmt.Println(message.Values)
		}
	}
}

func TestSubscribePubSub(t *testing.T) {
	pubSub := client.Subscribe(ctx, "channel-1")
	defer pubSub.Close()

	for i := 0; i < 10; i++ {
		message, _ := pubSub.ReceiveMessage(ctx)
		fmt.Println(message.Payload)
	}
}

func TestPublishPubSub(t *testing.T) {
	for i := 0; i < 10; i++ {
		err := client.Publish(ctx, "channel-1", "Hello "+strconv.Itoa(i)).Err()
		assert.Nil(t, err)
	}
}
