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
	timeout := func(opt *Option) {
		opt.ConnectTimeout = time.Duration(time.Second)
		opt.ReadTimeout = time.Duration(time.Second * 3)
		opt.WriteTimeout = time.Duration(time.Second * 2)
		opt.IdleTimeout = time.Duration(time.Second * 60)
	}

	hostAddr := func(opt *Option) {
		opt.Addr = "localhost:8888"
		opt.Network = "tcp"
	}
	poolSize := func(opt *Option) {
		opt.PoolSize = 16
	}
	onConnEvent := func(opt *Option) {
		opt.OnConnEvent = func(msg string) {
			fmt.Println(msg)
		}
	}
	pool, poolErr = NewPool(hostAddr, timeout, poolSize, onConnEvent)
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
		go doTimes(1000)
	}
	routineWait.Wait()
	fmt.Println("Routine is OK")
}

func TestMulti_Get_Order(t *testing.T) {
	for i := 0; i < 100; i++ {
		pool.Do("hset", "test", i, i)
	}
	target := make([]string, 100)
	for i := 0; i < 100; i++ {
		target[i] = fmt.Sprintf("%d", i)
	}
	reply := pool.Do("multi_hget", "test", target)
	strs := reply.Strings()
	for i := 0; i < len(strs); i += 2 {
		t.Log("Result", strs[i], strs[i+1])
	}
	pool.Do("hclear", "test")
}
