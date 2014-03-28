// Package files provides a set type to track local/remote files with newness checks.
package files

import (
	"fmt"
	"sync"

	"github.com/calmh/syncthing/cid"
	"github.com/calmh/syncthing/protocol"
	"github.com/calmh/syncthing/scanner"
	"github.com/calmh/syncthing/vc"
)

type key struct {
	Name    string
	Version []int64
}

func (k key) String() string {
	return fmt.Sprintf("%s:%v", k.Name, k.Version)
}

type fileRecord struct {
	Usage int
	File  scanner.File
}

type bitset uint64

func keyFor(f scanner.File) key {
	return key{
		Name:    f.Name,
		Version: f.Version,
	}
}

func (a key) newerThan(b key) bool {
	return vc.Compare(a.Version, b.Version) == vc.Greater
}

func (a key) equals(b key) bool {
	return vc.Compare(a.Version, b.Version) == vc.Equal
}

func updateClock(id int, fs []scanner.File) {
	for i := range fs {
		if fs[i].Changed {
			fs[i].Changed = false
			vc.Inc(id, fs[i].Version)
		}
	}
}

type Set struct {
	sync.Mutex
	files              map[string]fileRecord
	remoteKey          [64]map[string]key
	changes            [64]uint64
	globalAvailability map[string]bitset
	globalKey          map[string]key
}

func NewSet() *Set {
	var m = Set{
		files:              make(map[string]fileRecord),
		globalAvailability: make(map[string]bitset),
		globalKey:          make(map[string]key),
	}
	return &m
}

func (m *Set) Replace(id uint, fs []scanner.File) {
	if debug {
		dlog.Printf("Replace(%d, [%d])", id, len(fs))
	}
	if id > 63 {
		panic("Connection ID must be in the range 0 - 63 inclusive")
	}

	m.Lock()
	if len(fs) == 0 || !m.equals(id, fs) {
		m.changes[id]++
		m.replace(id, fs)
	}
	m.Unlock()
}

func (m *Set) ReplaceWithDelete(id uint, fs []scanner.File) {
	if debug {
		dlog.Printf("ReplaceWithDelete(%d, [%d])", id, len(fs))
	}
	if id > 63 {
		panic("Connection ID must be in the range 0 - 63 inclusive")
	}

	m.Lock()
	if len(fs) == 0 || !m.equals(id, fs) {
		m.changes[id]++

		var nf = make(map[string]key, len(fs))
		for _, f := range fs {
			nf[f.Name] = keyFor(f)
		}

		// For previously existing files not in the list, add them to the list
		// with the relevant delete flags etc set.

		for _, ck := range m.remoteKey[cid.LocalID] {
			if _, ok := nf[ck.Name]; !ok {
				cf := m.files[ck.String()].File
				cf.Flags = protocol.FlagDeleted
				cf.Blocks = nil
				cf.Size = 0
				vc.Inc(int(id), cf.Version)
				fs = append(fs, cf)
				if debug {
					dlog.Println("deleted:", ck.Name)
				}
			}
		}

		m.replace(id, fs)
	}
	m.Unlock()
}

func (m *Set) Update(id uint, fs []scanner.File) {
	if debug {
		dlog.Printf("Update(%d, [%d])", id, len(fs))
	}
	m.Lock()
	m.update(id, fs)
	m.changes[id]++
	m.Unlock()
}

func (m *Set) Need(id uint) []scanner.File {
	if debug {
		dlog.Printf("Need(%d)", id)
	}
	var fs []scanner.File
	m.Lock()
	for name, gk := range m.globalKey {
		if gk.newerThan(m.remoteKey[id][name]) {
			fs = append(fs, m.files[gk.String()].File)
		}
	}
	m.Unlock()
	return fs
}

func (m *Set) Have(id uint) []scanner.File {
	if debug {
		dlog.Printf("Have(%d)", id)
	}
	var fs []scanner.File
	m.Lock()
	for _, rk := range m.remoteKey[id] {
		fs = append(fs, m.files[rk.String()].File)
	}
	m.Unlock()
	return fs
}

func (m *Set) Global() []scanner.File {
	if debug {
		dlog.Printf("Global()")
	}
	var fs []scanner.File
	m.Lock()
	for _, rk := range m.globalKey {
		fs = append(fs, m.files[rk.String()].File)
	}
	m.Unlock()
	return fs
}

func (m *Set) Get(id uint, file string) scanner.File {
	m.Lock()
	defer m.Unlock()
	if debug {
		dlog.Printf("Get(%d, %q)", id, file)
	}
	return m.files[m.remoteKey[id][file].String()].File
}

func (m *Set) GetGlobal(file string) scanner.File {
	m.Lock()
	defer m.Unlock()
	if debug {
		dlog.Printf("GetGlobal(%q)", file)
	}
	return m.files[m.globalKey[file].String()].File
}

func (m *Set) Availability(name string) bitset {
	m.Lock()
	defer m.Unlock()
	av := m.globalAvailability[name]
	if debug {
		dlog.Printf("Availability(%q) = %0x", name, av)
	}
	return av
}

func (m *Set) Changes(id uint) uint64 {
	m.Lock()
	defer m.Unlock()
	if debug {
		dlog.Printf("Changes(%d)", id)
	}
	return m.changes[id]
}

func (m *Set) equals(id uint, fs []scanner.File) bool {
	s := m.remoteKey[id]
	if len(s) != len(fs) {
		return false
	}
	for _, f := range fs {
		if !s[f.Name].equals(keyFor(f)) {
			return false
		}
	}
	return true
}

func (m *Set) update(cid uint, fs []scanner.File) {
	remFiles := m.remoteKey[cid]
	for _, f := range fs {
		n := f.Name
		fk := keyFor(f)
		fks := fk.String()

		if ck, ok := remFiles[n]; ok && ck.equals(fk) {
			// The remote already has exactly this file, skip it
			continue
		}

		remFiles[n] = fk

		// Keep the block list or increment the usage
		if br, ok := m.files[fks]; !ok {
			m.files[fks] = fileRecord{
				Usage: 1,
				File:  f,
			}
		} else {
			br.Usage++
			m.files[fks] = br
		}

		// Update global view
		gk, ok := m.globalKey[n]
		switch {
		case ok && fk.equals(gk):
			av := m.globalAvailability[n]
			av |= 1 << cid
			m.globalAvailability[n] = av
		case fk.newerThan(gk):
			m.globalKey[n] = fk
			m.globalAvailability[n] = 1 << cid
		}
	}
}

func (m *Set) replace(cid uint, fs []scanner.File) {
	// Decrement usage for all files belonging to this remote, and remove
	// those that are no longer needed.
	for _, fk := range m.remoteKey[cid] {
		fks := fk.String()
		br, ok := m.files[fks]
		switch {
		case ok && br.Usage == 1:
			delete(m.files, fks)
		case ok && br.Usage > 1:
			br.Usage--
			m.files[fks] = br
		}
	}

	// Clear existing remote remoteKey
	m.remoteKey[cid] = make(map[string]key)

	// Recalculate global based on all remaining remoteKey
	for n := range m.globalKey {
		var nk key    // newest key
		var na bitset // newest availability

		for i, rem := range m.remoteKey {
			if rk, ok := rem[n]; ok {
				switch {
				case rk.equals(nk):
					na |= 1 << uint(i)
				case rk.newerThan(nk):
					nk = rk
					na = 1 << uint(i)
				}
			}
		}

		if na != 0 {
			// Someone had the file
			m.globalKey[n] = nk
			m.globalAvailability[n] = na
		} else {
			// Noone had the file
			delete(m.globalKey, n)
			delete(m.globalAvailability, n)
		}
	}

	// Add new remote remoteKey to the mix
	m.update(cid, fs)
}
