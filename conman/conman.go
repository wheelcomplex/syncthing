package conman

import (
	"io"
	"sync"

	"github.com/calmh/syncthing/model2"
	"github.com/calmh/syncthing/protocol"
)

type Connection interface {
	ID() string
	Index([]protocol.FileInfo)
	Request(name string, offset int64, size uint32, hash []byte) ([]byte, error)
	Statistics() protocol.Statistics
	Option(key string) string
}

type ConMan struct {
	mutex sync.Mutex
	model model2.Model

	nodeIDcID map[string]int
	cIDnodeID [64]string
	conns     [64]Connections
	socks     [64]io.Closer
}

func (c *ConMan) AddConnection(sock io.Closer, conn Connection) {
	c.mutex.Lock()
	cID := c.newCid(conn.ID())
	c.socks[cID] = sock
	c.conns[cID] = conn
	c.mutex.Unlock()
}

func (c *ConMan) newCid(nodeID string) int {
	for i := 1; i < 64; i++ {
		if len(c.cIDnodeID[i]) == 0 {
			c.cIDnodeID[i] = nodeID
			c.nodeIDcID[nodeID] = i
			return i
		}
	}
	return 0
}
