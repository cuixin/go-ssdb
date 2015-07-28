package ssdb

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

var pool *Pool
var poolErr error

func init() {
	pool, poolErr = NewPool(
		&Options{
			Addr:           "localhost:8888",
			Network:        "tcp",
			PoolSize:       16,
			ConnectTimeout: time.Duration(time.Second),
			ReadTimeout:    time.Duration(time.Second * 3),
			WriteTimeout:   time.Duration(time.Second * 2),
			IdleTimeout:    time.Duration(time.Second * 60),
			OnConnEvent: func(msg string) {
				fmt.Println(msg)
			},
		})
	if poolErr != nil {
		panic(poolErr)
	}
}

func TestData(t *testing.T) {
	var (
		reply *Reply
	)

	reply = pool.Do("hset", "aaaaa", 1000, 1000)
	t.Log(reply.String())
	data := []byte("hello world")
	wData := make([]byte, len(data)*1000)
	s := len(data)

	for i := 0; i < 2; i++ {
		j := i * s
		copy(wData[j:j+s], data)
	}

	for i := 0; i < 10; i++ {
		reply = pool.Do("hset", "bbbbb", i, wData)
		t.Log(reply.String())
	}
	reply = pool.Do("hclear", "aaaaa")
	t.Log(reply.String())
	reply = pool.Do("hclear", "bbbbb")
	t.Log(reply.String())
}

var routineWait = &sync.WaitGroup{}

func doTimes(size int) {
	if size > 1 {
		for i := 0; i < size; i++ {
			pool.Do("set", "test", "test")
		}
	} else {
		pool.Do("set", "test", "test")
	}
	routineWait.Done()
}

func TestGoroutine(t *testing.T) {
	size := 30
	routineWait.Add(size)
	for i := 0; i < size; i++ {
		go doTimes(10000)
	}
	routineWait.Wait()
	pool.Release()
	fmt.Println("Routine is OK")
}
