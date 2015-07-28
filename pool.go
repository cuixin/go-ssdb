package ssdb

import (
	"sync"
	"time"
)

type Pool struct {
	mu     sync.Mutex
	cons   []*conn
	curr   int
	ticker *time.Ticker
}

var (
	releaseWait sync.WaitGroup
)

func NewPool(opt *Options) (*Pool, error) {
	options = opt
	pool := &Pool{
		mu:     sync.Mutex{},
		cons:   make([]*conn, options.PoolSize),
		curr:   0,
		ticker: time.NewTicker(time.Duration(5 * time.Second)),
	}
	for j := 0; j < options.PoolSize; j++ {
		netcon, err := dial()
		if err != nil {
			return nil, err
		}
		pool.cons[j] = newConn(netcon)
	}
	go func() {
		for t := range pool.ticker.C {
			// check alive
			for _, c := range pool.cons {
				c.Ping(t)
			}
		}
	}()
	return pool, nil
}

func (p *Pool) Release() {
	p.ticker.Stop()
	releaseWait.Wait()
	for _, c := range p.cons {
		c.close()
	}
	return
}

func (p *Pool) Do(cmd string, args ...interface{}) *Reply {
	p.mu.Lock()
	if p.curr == len(p.cons) {
		p.curr = 0
	}
	c := p.cons[p.curr]
	p.curr++
	p.mu.Unlock()
	releaseWait.Add(1)
	reply := c.Do(cmd, args...)
	releaseWait.Done()
	return reply
}
