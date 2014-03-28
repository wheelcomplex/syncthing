package scanner

import (
	"fmt"

	"github.com/calmh/syncthing/vc"
)

type File struct {
	Name       string
	Flags      uint32
	Modified   int64
	Version    []int64
	Size       int64
	Blocks     []Block
	Suppressed bool
	Changed    bool
}

func (f File) String() string {
	return fmt.Sprintf("File{Name:%q, Flags:0%o, Modified:%d, Version:%v, Size:%d, NumBlocks:%d}",
		f.Name, f.Flags, f.Modified, f.Version, f.Size, len(f.Blocks))
}

func (f File) Equals(o File) bool {
	return vc.Compare(f.Version, o.Version) == vc.Equal
}

func (f File) NewerThan(o File) bool {
	return vc.Compare(f.Version, o.Version) == vc.Greater
}
