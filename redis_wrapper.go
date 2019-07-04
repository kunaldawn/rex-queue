package rex_queue

import (
	"fmt"
	"gopkg.in/redis.v5"
	"log"
	"time"
)

type RedisWrapper struct {
	rawClient *redis.Client
}

func (wrapper RedisWrapper) Set(key string, value string, expiration time.Duration) bool {
	err := wrapper.rawClient.Set(key, value, expiration).Err()
	for isFetalErr(err) {
		time.Sleep(time.Second)
		err = wrapper.rawClient.Set(key, value, expiration).Err()
	}

	return checkErr(err)
}

func (wrapper RedisWrapper) Del(key string) (affected int, ok bool) {
	n, err := wrapper.rawClient.Del(key).Result()
	for isFetalErr(err) {
		time.Sleep(time.Second)
		n, err = wrapper.rawClient.Del(key).Result()
	}

	ok = checkErr(err)
	if !ok {
		return 0, false
	}
	return int(n), ok
}

func (wrapper RedisWrapper) TTL(key string) (ttl time.Duration, ok bool) {
	ttl, err := wrapper.rawClient.TTL(key).Result()
	for isFetalErr(err) {
		time.Sleep(time.Second)
		ttl, err = wrapper.rawClient.TTL(key).Result()
	}

	ok = checkErr(err)
	if !ok {
		return 0, false
	}
	return ttl, ok
}

func (wrapper RedisWrapper) LPush(key, value string) bool {
	err := wrapper.rawClient.LPush(key, value).Err()
	for isFetalErr(err) {
		time.Sleep(time.Second)
		err = wrapper.rawClient.LPush(key, value).Err()
	}

	return checkErr(err)
}

func (wrapper RedisWrapper) LLen(key string) (affected int, ok bool) {
	n, err := wrapper.rawClient.LLen(key).Result()
	for isFetalErr(err) {
		time.Sleep(time.Second)
		n, err = wrapper.rawClient.LLen(key).Result()
	}

	ok = checkErr(err)
	if !ok {
		return 0, false
	}
	return int(n), ok
}

func (wrapper RedisWrapper) LRem(key string, count int, value string) (affected int, ok bool) {
	n, err := wrapper.rawClient.LRem(key, int64(count), value).Result()
	for isFetalErr(err) {
		time.Sleep(time.Second)
		n, err = wrapper.rawClient.LRem(key, int64(count), value).Result()
	}

	return int(n), checkErr(err)
}

func (wrapper RedisWrapper) LTrim(key string, start, stop int) {
	err := wrapper.rawClient.LTrim(key, int64(start), int64(stop)).Err()
	for isFetalErr(err) {
		time.Sleep(time.Second)
		err = wrapper.rawClient.LTrim(key, int64(start), int64(stop)).Err()
	}

	checkErr(err)
}

func (wrapper RedisWrapper) RPopLPush(source, destination string) (value string, ok bool) {
	value, err := wrapper.rawClient.RPopLPush(source, destination).Result()
	for isFetalErr(err) {
		time.Sleep(time.Second)
		value, err = wrapper.rawClient.RPopLPush(source, destination).Result()
	}

	return value, checkErr(err)
}

func (wrapper RedisWrapper) SAdd(key, value string) bool {
	err := wrapper.rawClient.SAdd(key, value).Err()
	for isFetalErr(err) {
		time.Sleep(time.Second)
		err = wrapper.rawClient.SAdd(key, value).Err()
	}

	return checkErr(err)
}

func (wrapper RedisWrapper) SMembers(key string) []string {
	members, err := wrapper.rawClient.SMembers(key).Result()
	for isFetalErr(err) {
		time.Sleep(time.Second)
		members, err = wrapper.rawClient.SMembers(key).Result()
	}

	if ok := checkErr(err); !ok {
		return []string{}
	}
	return members
}

func (wrapper RedisWrapper) SRem(key, value string) (affected int, ok bool) {
	n, err := wrapper.rawClient.SRem(key, value).Result()
	for isFetalErr(err) {
		time.Sleep(time.Second)
		n, err = wrapper.rawClient.SRem(key, value).Result()
	}

	ok = checkErr(err)
	if !ok {
		return 0, false
	}
	return int(n), ok
}

func (wrapper RedisWrapper) FlushDb() {
	wrapper.rawClient.FlushDb()
}

// checkErr returns true if there is no error, false if the result error is nil and panics if there's another error
func checkErr(err error) (ok bool) {
	switch err {
	case nil:
		return true
	case redis.Nil:
		return false
	default:
		log.Println(fmt.Sprintf("final error is not nil %s", err))
		return false
	}
}

func isFetalErr(err error) (ok bool) {
	switch err {
	case nil:
		return false
	case redis.Nil:
		return false
	default:
		log.Println(fmt.Sprintf("runtime error detected %s", err))
		return true
	}
}
