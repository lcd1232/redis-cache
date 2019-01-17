package rcache

import (
	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
	"sync/atomic"
	"time"
)

var (
	ErrCacheMiss = errors.New("cache: key is missing")
)

type MarshalFunc func(interface{}) ([]byte, error)
type UnmarshalFunc func([]byte, interface{}) error

type Cache struct {
	Redis     *redis.Pool
	Marshal   func(interface{}) ([]byte, error)
	Unmarshal func([]byte, interface{}) error

	conn   redis.Conn
	hits   uint64
	misses uint64
}

type Item struct {
	Key        string
	Object     interface{}
	Expiration time.Duration
}

func NewRedisCache(redis *redis.Pool, marshalFunc MarshalFunc, unmarshalFunc UnmarshalFunc) *Cache {
	return &Cache{
		Redis:     redis,
		Marshal:   marshalFunc,
		Unmarshal: unmarshalFunc,
	}
}

func (c *Cache) getConn() (redis.Conn, error) {
	if c.conn == nil {
		conn := c.Redis.Get()
		if err := conn.Err(); err != nil {
			return conn, errors.WithStack(err)
		}
	}
	return c.conn, nil
}

func (c *Cache) Set(item *Item) error {
	b, err := c.Marshal(item.Object)
	if err != nil {
		return errors.Wrap(err, "marshal failed")
	}

	conn, err := c.getConn()
	if err != nil {
		return errors.Wrap(err, "getConn failed")
	}
	defer conn.Close()

	expire := item.Expiration
	if item.Expiration < time.Second {
		expire = 2 * time.Minute
	}
	args := []interface{}{
		item.Key,
		int(expire.Seconds()),
		b,
	}
	if _, err := conn.Do("SETEX", args...); err != nil {
		return errors.Wrap(err, "Redis SETEX failed")
	}
	return nil
}

func (c *Cache) Get(key string, object interface{}) error {
	conn, err := c.getConn()
	if err != nil {
		return errors.Wrap(err, "getConn failed")
	}
	defer conn.Close()

	b, err := redis.Bytes(conn.Do("GET", key))
	if err != nil {
		if err == redis.ErrNil {
			atomic.AddUint64(&c.misses, 1)
			return ErrCacheMiss
		}
		return errors.Wrap(err, "Redis GET failed")
	}
	atomic.AddUint64(&c.hits, 1)
	if len(b) == 0 {
		return nil
	}
	if err := c.Unmarshal(b, object); err != nil {
		return errors.Wrap(err, "unmarshal failed")
	}
	return nil
}

type Stats struct {
	Hits   uint64
	Misses uint64
}

func (c *Cache) Stats() *Stats {
	return &Stats{
		Hits:   atomic.LoadUint64(&c.hits),
		Misses: atomic.LoadUint64(&c.misses),
	}
}
