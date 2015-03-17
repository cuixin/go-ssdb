package ssdb

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

var (
	_EMPTY_DATA = [][]byte{}
)

type conn struct {
	con          net.Conn
	readTimeout  time.Duration
	recv_buf     bytes.Buffer
	writeTimeout time.Duration
	idleTimeout  time.Duration
	mu           sync.Mutex
	lastDbTime   time.Time
}

func Dial(network, address string) (*conn, error) {
	c, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}

	return newConn(c, 0, 0, time.Duration(60*time.Second)), nil
}

func DialTimeout(network, address string, connectTimeout, readTimeout, writeTimeout, idleTimeout time.Duration) (*conn, error) {
	var c net.Conn
	var err error
	if connectTimeout > 0 {
		c, err = net.DialTimeout(network, address, connectTimeout)
	} else {
		c, err = net.Dial(network, address)
	}
	if err != nil {
		return nil, err
	}
	return newConn(c, readTimeout, writeTimeout, idleTimeout), nil
}

func newConn(netConn net.Conn, readTimeout, writeTimeout, idleTimeout time.Duration) *conn {
	return &conn{
		con:          netConn,
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
		mu:           sync.Mutex{},
		idleTimeout:  idleTimeout,
	}
}

func (c *conn) Close() error {
	return c.con.Close()
}

func (c *conn) fatal(err error) error {
	c.con.Close()
	return err
}

func (c *conn) parse() [][]byte {
	resp := [][]byte{}
	buf := c.recv_buf.Bytes()
	var idx, offset int
	idx = 0
	offset = 0

	for {
		idx = bytes.IndexByte(buf[offset:], '\n')
		if idx == -1 {
			break
		}
		p := buf[offset : offset+idx]
		offset += idx + 1
		//		fmt.Printf("> [%s]\n", p)
		if len(p) == 0 || (len(p) == 1 && p[0] == '\r') {
			if len(resp) == 0 {
				continue
			} else {
				c.recv_buf.Next(offset)
				return resp
			}
		}

		size, err := strconv.Atoi(string(p))
		if err != nil || size < 0 {
			return nil
		}
		if offset+size >= c.recv_buf.Len() {
			break
		}

		v := buf[offset : offset+size]
		resp = append(resp, v)
		offset += size + 1
	}
	return [][]byte{}
}

func (c *conn) recv() ([][]byte, error) {
	var tmp [8192]byte
	for {
		n, err := c.con.Read(tmp[0:])
		if err != nil {
			return nil, err
		}
		c.recv_buf.Write(tmp[0:n])
		resp := c.parse()
		if resp == nil || len(resp) > 0 {
			return resp, nil
		}
	}
}

func (c *conn) readReply() (reply *Reply, err error) {
	var resp [][]byte
	resp, err = c.recv()
	if err != nil {
		return
	}
	if len(resp) < 1 {
		return nil, fmt.Errorf("ssdb: parse error")
	}
	reply = new(Reply)
	reply.toState(resp[0])
	if len(resp) > 1 {
		reply.data = resp[1:]
	} else {
		reply.data = _EMPTY_DATA
	}
	return
}

func (c *conn) writeCommand(cmd string, args []interface{}) error {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%d", len(cmd)))
	buf.WriteByte('\n')
	buf.WriteString(cmd)
	buf.WriteByte('\n')

	for _, arg := range args {
		var s string
		switch arg := arg.(type) {
		case string:
			s = arg
		case []byte:
			s = string(arg)
		case int, int8, int16, int32, int64:
			s = fmt.Sprintf("%d", arg)
		case uint, uint8, uint16, uint32, uint64:
			s = fmt.Sprintf("%d", arg)
		case float32, float64:
			s = fmt.Sprintf("%f", arg)
		case bool:
			if arg {
				s = "1"
			} else {
				s = "0"
			}
		case nil:
			s = ""
		default:
			return fmt.Errorf("bad arguments")
		}
		buf.WriteString(fmt.Sprintf("%d", len(s)))
		buf.WriteByte('\n')
		buf.WriteString(s)
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')
	//	fmt.Println("write", buf.Bytes())
	_, err := c.con.Write(buf.Bytes())
	return err
}

func (c *conn) Ping(now time.Time) {
	if now.After(c.lastDbTime.Add(c.idleTimeout)) {
		c.Do("ping")
	}
}

func (c *conn) Do(cmd string, args ...interface{}) (*Reply, error) {
	if cmd == "" {
		return nil, nil
	}

	if c.writeTimeout != 0 {
		c.con.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	c.mu.Lock()
	if cmd != "" {
		c.writeCommand(cmd, args)
	}

	if c.readTimeout != 0 {
		c.con.SetReadDeadline(time.Now().Add(c.readTimeout))
	}

	//	fmt.Println("Do ", cmd, args, pending)
	var err error
	var reply *Reply
	if reply, err = c.readReply(); err != nil {
		c.lastDbTime = time.Now()
		c.mu.Unlock()
		return nil, c.fatal(err)
	} else {
		c.lastDbTime = time.Now()
		c.mu.Unlock()
		return reply, nil
	}
}
