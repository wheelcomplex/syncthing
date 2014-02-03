package conman

// Implements the ReaderAt interface for a given remote file.
type requestProxy struct {
	cm   *ConMan
	name string
}

func (r requestProxy) ReadAt(bs []byte, offset int) (int, error) {
	rbs, err := r.cm.Request("", r.name, int64(offset), uint32(len(bs)), nil)
	copy(bs, rbs)
	return len(rbs), err
}
