package fdcache

import (
	"os"
	"sync"
	"time"
)

var (
	readFiles     = make(map[string]*readFile)
	readFilesLock sync.RWMutex
)

func init() {
	go func() {
		for {
			time.Sleep(time.Second)
			readFilesLock.Lock()
			for _, f := range readFiles {
				if f.usage == 0 {
					println("close", f.name)
					f.File.Close()
					delete(readFiles, f.name)
				}
			}
			readFilesLock.Unlock()
		}
	}()
}

type ReaderAtCloser interface {
	ReadAt(p []byte, off int64) (n int, err error)
	Close() error
}

type readFile struct {
	os.File
	name  string
	usage int32
}

func (f *readFile) Close() error {
	readFilesLock.Lock()
	f.usage--
	readFilesLock.Unlock()
	return nil
}

func Open(name string) (ReaderAtCloser, error) {
	readFilesLock.Lock()
	defer readFilesLock.Unlock()

	if rf, ok := readFiles[name]; ok {
		rf.usage++
		println("reuse", name, rf.usage)
		return rf, nil
	}

	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	rf := &readFile{*f, name, 1}
	readFiles[name] = rf
	println("new", name, rf.usage)
	return rf, nil
}
