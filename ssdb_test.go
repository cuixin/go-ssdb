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
	pool, poolErr = NewPool("192.168.1.23:8888",
		time.Duration(time.Second),
		time.Duration(time.Second*3),
		time.Duration(time.Second*2),
		time.Duration(time.Second*60),
		64)
	if poolErr != nil {
		panic(poolErr)
	}
}

func TestData(t *testing.T) {
	var (
		reply *Reply
		err   error
	)

	reply, err = pool.Do("hset", "aaaaa", 1000, 1000)
	t.Log(reply.String(), err)
	data := []byte("hello world")
	wData := make([]byte, len(data)*1000)
	s := len(data)

	for i := 0; i < 2; i++ {
		j := i * s
		copy(wData[j:j+s], data)
	}

	for i := 0; i < 10; i++ {
		reply, err = pool.Do("hset", "bbbbb", i, wData)
		t.Log(reply.String(), err)
	}
	reply, err = pool.Do("hclear", "aaaaa")
	t.Log(reply.String(), err)
	reply, err = pool.Do("hclear", "bbbbb")
	t.Log(reply.String(), err)
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
	pool.Close()
	fmt.Println("Routine is OK")
}
