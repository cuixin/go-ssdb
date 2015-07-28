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
	options   *Options
)

type (
	conn struct {
		sock       net.Conn
		recvBuf    *bufio.Reader
		writeBuf   *bytes.Buffer
		mu         sync.Mutex
		lastDbTime time.Time
	}
)

func dial() (net.Conn, error) {
	var c net.Conn
	var err error
	if options.ConnectTimeout > 0 {
		c, err = net.DialTimeout(options.Network, options.Addr, options.ConnectTimeout)
	} else {
		c, err = net.Dial(options.Network, options.Addr)
	}
	return c, err
}

func newConn(netConn net.Conn) *conn {
	c := &conn{
		sock:       netConn,
		writeBuf:   bytes.NewBuffer(make([]byte, 8192)),
		recvBuf:    bufio.NewReaderSize(netConn, 8192),
		mu:         sync.Mutex{},
		lastDbTime: time.Now(),
	}
	return c
}

func (c *conn) close() error {
	return c.sock.Close()
}

func (c *conn) fatal(err error) error {
	c.sock.Close()
	return err
}

func (c *conn) readBlock() ([]byte, error) {
	var (
		len int
		err error
		d   byte
	)
	len = 0
	d, err = c.recvBuf.ReadByte()
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
		d, err = c.recvBuf.ReadByte()
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
			r, err = c.recvBuf.Read(data[count:])
			if err != nil {
				return nil, err
			}
			count += r
		}
	}
	d, err = c.recvBuf.ReadByte()
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
	reply = new(Reply)
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
	_, err := c.sock.Write(c.writeBuf.Bytes())
	return err
}

func (c *conn) Ping(now time.Time) {
	if now.After(c.lastDbTime.Add(options.IdleTimeout)) {
		c.Do("ping")
	}
}

func doReconnect(c *conn, err error, cmd string) {
	for {
		if options.OnConnEvent != nil {
			options.OnConnEvent(fmt.Sprintf("On write command error [%v] [%v]", err, cmd))
		}
		time.Sleep(100 * time.Microsecond)
		netC, err := dial()
		if err == nil {
			options.OnConnEvent(fmt.Sprintf("On reconnected successful! [%v]", cmd))
			c.sock = netC
			c.recvBuf = bufio.NewReaderSize(c.sock, 8192)
			break
		}
	}
}

func (c *conn) Do(cmd string, args ...interface{}) *Reply {
	var (
		err   error
		reply *Reply
	)
	c.mu.Lock()
	for {
		if options.WriteTimeout != 0 {
			c.sock.SetWriteDeadline(time.Now().Add(options.WriteTimeout))
		}
		err = c.writeCommand(cmd, args)
		if err != nil {
			doReconnect(c, err, cmd)
		}
		if options.ReadTimeout != 0 {
			c.sock.SetReadDeadline(time.Now().Add(options.ReadTimeout))
		}

		reply, err = c.readReply()
		c.lastDbTime = time.Now()
		if err != nil {
			doReconnect(c, err, cmd)
		} else {
			break
		}
	}
	c.mu.Unlock()
	return reply
}
