package model

import "sync"

type GlobalFile struct {
	Key          FileKey
	Availability Availability
}

type BlockRecord struct {
	Blocks []Block
	Usage  int
}

type File struct {
	Key    FileKey
	Blocks []Block
}

type FileKey struct {
	Name     string
	Modified int64
	Version  uint32
	Flags    uint32
}

func (a FileKey) NewerThan(b FileKey) bool {
	switch {
	case a.Modified > b.Modified:
		return true
	case a.Modified == b.Modified && a.Version > b.Version:
		return true
	default:
		return false
	}
}

type Block struct {
	Offset int64
	Size   uint32
	Hash   []byte
}

type Availability uint64

type Model struct {
	mutex  sync.RWMutex
	blocks map[FileKey]BlockRecord
	files  [64]map[string]FileKey
	global map[string]GlobalFile
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
			m.blocks[f.Key] = BlockRecord{
				Blocks: f.Blocks,
				Usage:  1,
			}
		} else {
			br.Usage++
			m.blocks[f.Key] = br
		}

		// Update global view
		gf, ok := m.global[n]
		switch {
		case ok && f.Key.Version == gf.Key.Version && f.Key.Modified == gf.Key.Modified:
			gf.Availability |= 1 << cid
			m.global[n] = gf
		case f.Key.NewerThan(gf.Key):
			m.global[n] = GlobalFile{
				Key:          f.Key,
				Availability: 1 << cid,
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
	m.files[cid] = make(map[string]FileKey)

	// Lazy init global & blocks
	if m.global == nil {
		m.global = make(map[string]GlobalFile)
	}
	if m.blocks == nil {
		m.blocks = make(map[FileKey]BlockRecord)
	}

	// Recalculate global based on all remaining files
	for n := range m.global {
		gf := GlobalFile{}

		// Find the new latest file for the global map
		for i, rem := range m.files {
			i := uint(i)
			rf, ok := rem[n]
			switch {
			case ok && rf.NewerThan(gf.Key):
				gf = GlobalFile{
					Key:          rf,
					Availability: 1 << i,
				}
			case ok && rf == gf.Key:
				gf.Availability |= 1 << i
			}
		}
		if gf.Key != (FileKey{}) {
			m.global[n] = gf
		} else {
			delete(m.global, n)
		}
	}

	// Add new remote files to the mix
	m.unlockedAddRemote(cid, fs)
}

func (m *Model) Need() []File {
	var fs []File
	m.mutex.Lock()

	for name, gf := range m.global {
		if gf.Key.NewerThan(m.files[0][name]) {
			fs = append(fs, File{gf.Key, m.blocks[gf.Key].Blocks})
		}
	}

	m.mutex.Unlock()
	return fs
}
