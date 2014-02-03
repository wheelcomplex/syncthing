package conman

import (
	"io"
	"sync"

	model "github.com/calmh/syncthing/model2"
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
	model model.Model

	nodeIDcID map[string]uint
	cIDnodeID [64]string
	conns     [64]Connection
	socks     [64]io.Closer
}

func (c *ConMan) AddConnection(sock io.Closer, conn Connection) {
	c.mutex.Lock()
	cID := c.newCid(conn.ID())
	c.socks[cID] = sock
	c.conns[cID] = conn
	c.mutex.Unlock()
}

func (c *ConMan) newCid(nodeID string) uint {
	for i := uint(1); i < 64; i++ {
		if len(c.cIDnodeID[i]) == 0 {
			c.cIDnodeID[i] = nodeID
			c.nodeIDcID[nodeID] = i
			return i
		}
	}
	return 0
}

// An index was received from the peer node
func (c *ConMan) Index(nodeID string, files []protocol.FileInfo) {
	cid, ok := c.nodeIDcID[nodeID]
	if !ok {
		panic("Unknown node ID " + nodeID)
	}

	fs := modelFiles(files)
	c.model.SetRemote(cid, fs)
}

// An index update was received from the peer node
func (c *ConMan) IndexUpdate(nodeID string, files []protocol.FileInfo) {
	cid, ok := c.nodeIDcID[nodeID]
	if !ok {
		panic("Unknown node ID " + nodeID)
	}

	fs := modelFiles(files)
	c.model.AddRemote(cid, fs)
}

// A request was made by the peer node
func (c *ConMan) Request(nodeID, name string, offset int64, size uint32, hash []byte) ([]byte, error) {
	return nil, nil
}

// The peer node closed the connection
func (c *ConMan) Close(nodeID string, err error) {
}

func modelFiles(files []protocol.FileInfo) []model.File {
	var fs = make([]model.File, len(files))
	for i, f := range files {
		fs[i] = model.File{
			Key: model.Key{
				Name:     f.Name,
				Modified: f.Modified,
				Version:  f.Version,
				Flags:    f.Flags,
			},
			Data: f.Blocks,
		}
	}
}
