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
			for i, c := range pool.cons {
				err := c.Ping(t)
				if err != nil {
					// reconnect
					if options.OnConnEvent != nil {
						options.OnConnEvent("Ping failed, start to reconnect!")
					}
					newcon, newerr := dial()
					if newerr == nil {
						pool.cons[i].mu.Lock()
						pool.cons[i].con = newcon
						pool.cons[i].mu.Unlock()
						if options.OnConnEvent != nil {
							options.OnConnEvent("Start reconnecting successful on ping!")
						}
					}
				}
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

func (p *Pool) Do(cmd string, args ...interface{}) (*Reply, error) {
	p.mu.Lock()

	if p.curr == len(p.cons) {
		p.curr = 0
	}
	c := p.cons[p.curr]
	p.curr++
	p.mu.Unlock()
	releaseWait.Add(1)
	reply, err := c.Do(cmd, args...)
	releaseWait.Done()
	return reply, err
}
