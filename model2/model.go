package model

import "sync"

type File struct {
	Key  Key
	Data interface{}
}

type Key struct {
	Name     string
	Modified int64
	Version  uint32
	Flags    uint32
}

type globalRecord struct {
	key          Key
	availability availability
}

type dataRecord struct {
	Usage int
	Data  interface{}
}

type availability uint64

func (a Key) newerThan(b Key) bool {
	switch {
	case a.Modified > b.Modified:
		return true
	case a.Modified == b.Modified && a.Version > b.Version:
		return true
	default:
		return false
	}
}

type Model struct {
	mutex     sync.RWMutex
	blocksize int
	blocks    map[Key]dataRecord
	files     [64]map[string]Key
	global    map[string]globalRecord
}

func (m *Model) AddLocal(fs []File) {
	m.mutex.Lock()
	m.unlockedAddRemote(0, fs)
	m.mutex.Unlock()
}

func (m *Model) SetLocal(fs []File) {
	m.mutex.Lock()
	m.unlockedSetRemote(0, fs)
	m.mutex.Unlock()
}

func (m *Model) AddRemote(cid uint, fs []File) {
	m.mutex.Lock()
	m.unlockedAddRemote(cid, fs)
	m.mutex.Unlock()
}

func (m *Model) SetRemote(cid uint, fs []File) {
	m.mutex.Lock()
	m.unlockedSetRemote(cid, fs)
	m.mutex.Unlock()
}

func (m *Model) unlockedAddRemote(cid uint, fs []File) {
	fm := m.files[cid]
	for _, f := range fs {
		n := f.Key.Name

		if ck := fm[n]; ck.Version == f.Key.Version && ck.Modified == f.Key.Modified {
			continue
		}
		fm[n] = f.Key

		// Keep the block list or increment the usage
		if br, ok := m.blocks[f.Key]; !ok {
			m.blocks[f.Key] = dataRecord{
				Usage: 1,
				Data:  f.Data,
			}
		} else {
			br.Usage++
			m.blocks[f.Key] = br
		}

		// Update global view
		gf, ok := m.global[n]
		switch {
		case ok && f.Key.Version == gf.key.Version && f.Key.Modified == gf.key.Modified:
			gf.availability |= 1 << cid
			m.global[n] = gf
		case f.Key.newerThan(gf.key):
			m.global[n] = globalRecord{
				key:          f.Key,
				availability: 1 << cid,
			}
		}
	}
}

func (m *Model) unlockedSetRemote(cid uint, fs []File) {
	for _, fk := range m.files[cid] {
		// "Garbage collect" the blocks
		br, ok := m.blocks[fk]
		switch {
		case ok && br.Usage == 1:
			delete(m.blocks, fk)
		case ok && br.Usage > 1:
			br.Usage--
			m.blocks[fk] = br
		}
	}

	// Clear existing remote files
	m.files[cid] = make(map[string]Key)

	// Lazy init global & blocks
	if m.global == nil {
		m.global = make(map[string]globalRecord)
	}
	if m.blocks == nil {
		m.blocks = make(map[Key]dataRecord)
	}

	// Recalculate global based on all remaining files
	for n := range m.global {
		gf := globalRecord{}

		// Find the new latest file for the global map
		for i, rem := range m.files {
			i := uint(i)
			rf, ok := rem[n]
			switch {
			case ok && rf.newerThan(gf.key):
				gf = globalRecord{
					key:          rf,
					availability: 1 << i,
				}
			case ok && rf == gf.key:
				gf.availability |= 1 << i
			}
		}
		if gf.key != (Key{}) {
			m.global[n] = gf
		} else {
			delete(m.global, n)
		}
	}

	// Add new remote files to the mix
	m.unlockedAddRemote(cid, fs)
}

func (m *Model) Need(cid uint) []File {
	var fs []File
	m.mutex.Lock()

	for name, gf := range m.global {
		if gf.key.newerThan(m.files[cid][name]) {
			fs = append(fs, File{gf.key, m.blocks[gf.key].Data})
		}
	}

	m.mutex.Unlock()
	return fs
}
