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

func NewPool(addr string, c, r, w, i time.Duration, maxCons int) (*Pool, error) {
	pool := &Pool{
		mu:     sync.Mutex{},
		cons:   make([]*conn, maxCons),
		curr:   0,
		ticker: time.NewTicker(time.Duration(5 * time.Second)),
	}
	for j := 0; j < maxCons; j++ {
		conn, err := DialTimeout("tcp", addr, c, r, w, i)
		if err != nil {
			return nil, err
		}
		pool.cons[j] = conn
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

func (p *Pool) Close() {
	p.ticker.Stop()
	for _, c := range p.cons {
		c.Close()
	}
	return
}

func (p *Pool) Do(cmd string, args ...interface{}) (*Reply, error) {
	p.mu.Lock()

	if p.curr == len(p.cons) {
		p.curr = 0
	}
	c := p.cons[p.curr]
	p.curr++
	p.mu.Unlock()
	return c.Do(cmd, args...)
}
