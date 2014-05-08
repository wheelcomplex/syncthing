package sut

import "net"

type Connection struct {
	conn    *net.UDPConn
	dst     *net.UDPAddr
	nextseq int
	sndwnd  int

	outbox chan []byte
	inbox  chan []byte
	buffer []byte
}

func NewConnection(u *net.UDPConn) *Connection {
	c := &Connection{
		conn:   u,
		outbox: make(chan []byte, 16),
		inbox:  make(chan []byte, 16),
		buffer: make([]byte, 65536),
	}

	go c.reader()
	go c.writer()

	return c
}

func (c *Connection) reader() {
	for {
		buffer := make([]byte, 65536)
		n, _ := c.conn.Read(buffer)
		c.inbox <- buffer[:n]
	}
}

func (c *Connection) writer() {
	for {
		buffer := <-c.outbox
		c.conn.WriteTo(buffer, c.dst)
	}
}

func (c *Connection) Write(bs []byte) (int, error) {
	c.outbox <- bs
	return len(bs), nil
}

func (c *Connection) Read(bs []byte) (int, error) {
	if len(c.buffer) > 0 {
		n := copy(bs, c.buffer)
		r := len(c.buffer[n:])
		copy(c.buffer, c.buffer[n:])
		c.buffer = c.buffer[:r]
		return n, nil
	} else {
		in := <-c.inbox
		if n := len(in); n <= len(bs) {
			copy(bs, in)
			return n, nil
		} else {
			copy(bs, in)
			c.buffer = append(c.buffer, in[len(bs):]...)
			return len(bs), nil
		}
	}
}
