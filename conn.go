package ssdb

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"sync"
	"time"
)

var (
	emptyData = [][]byte{}
)

type conn struct {
	con          net.Conn
	readTimeout  time.Duration
	recvIn       *bufio.Reader
	writeBuf     *bytes.Buffer
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
	c := &conn{
		con:          netConn,
		writeBuf:     bytes.NewBuffer(make([]byte, 8192)),
		recvIn:       bufio.NewReaderSize(netConn, 8192),
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
		mu:           sync.Mutex{},
		idleTimeout:  idleTimeout,
	}
	return c
}

func (c *conn) Close() error {
	return c.con.Close()
}

func (c *conn) fatal(err error) error {
	c.con.Close()
	return err
}

func (c *conn) readBlock() ([]byte, error) {
	var (
		len int
		err error
		d   byte
	)
	len = 0
	d, err = c.recvIn.ReadByte()
	if err != nil {
		return nil, err
	}
	if d == '\n' {
		return nil, nil
	} else if d >= '0' && d <= '9' {
		len = len*10 + int(d-'0')
	} else {
		return nil, fmt.Errorf("protocol error. unexpect byte=%d", d)
	}
	for {
		d, err = c.recvIn.ReadByte()
		if err != nil {
			return nil, err
		}
		if d >= '0' && d <= '9' {
			len = len*10 + int(d-'0')
		} else if d == '\n' {
			break
		} else {
			return nil, fmt.Errorf("protocol error. unexpect byte=%d", d)
		}
	}
	data := make([]byte, len)
	if len > 0 {
		count := 0
		r := 0
		for count < len {
			r, err = c.recvIn.Read(data[count:])
			if err != nil {
				return nil, err
			}
			count += r
		}
	}
	d, err = c.recvIn.ReadByte()
	if err != nil {
		return nil, err
	}
	if d != '\n' {
		return nil, fmt.Errorf("protocol error. unexpect byte=%d", d)
	}
	return data, nil
}

func (c *conn) readReply() (reply *Reply, err error) {
	var resp [][]byte
	var data []byte
	data, err = c.readBlock()
	if err != nil {
		return
	}
	resp = append(resp, data)
	for {
		data, err = c.readBlock()
		if err != nil {
			return
		}
		if data == nil {
			break
		}
		resp = append(resp, data)
	}
	if len(resp) < 1 {
		return nil, fmt.Errorf("ssdb: parse error")
	}
	reply = new(Reply)
	reply.toState(resp[0])
	if len(resp) > 1 {
		reply.data = resp[1:]
	} else {
		reply.data = emptyData
	}
	return
}

func (c *conn) writeBlock(cmd string) {
	c.writeBuf.WriteString(fmt.Sprintf("%d", len(cmd)))
	c.writeBuf.WriteByte('\n')
	c.writeBuf.WriteString(cmd)
	c.writeBuf.WriteByte('\n')
}

func (c *conn) writeCommand(cmd string, args []interface{}) error {
	c.writeBuf.Reset()
	c.writeBlock(cmd)
	for _, arg := range args {
		var s string
		switch arg := arg.(type) {
		case string:
			s = arg
		case []string:
			for i, _ := range arg {
				c.writeBlock(arg[i])
			}
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
		c.writeBlock(s)
	}
	c.writeBuf.WriteByte('\n')
	_, err := c.con.Write(c.writeBuf.Bytes())
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
	c.writeCommand(cmd, args)

	if c.readTimeout != 0 {
		c.con.SetReadDeadline(time.Now().Add(c.readTimeout))
	}

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
