package model

import (
	"crypto/sha256"
	"fmt"
	"io"
)

// Hash returns the size and blockwise hash of the reader.
func Hash(r io.Reader, blocksize int) (int64, [][]byte, error) {
	var blocks [][]byte
	var size int64

	for {
		lr := &io.LimitedReader{r, int64(blocksize)}
		hf := sha256.New()
		n, err := io.Copy(hf, lr)
		if err != nil {
			return 0, nil, err
		}

		if n == 0 {
			break
		}

		hash := hf.Sum(nil)
		blocks = append(blocks, hash)
		size += int64(n)
	}

	if len(blocks) == 0 {
		// Empty file
		blocks = append(blocks, []uint8{0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55})
	}

	return size, blocks, nil
}

// Copy copies r (the source) to w (the destination), while reusing blocks from
// c (the cache) as far as possible. wb is the list of blocks we want (and
// assume available at r), cb is the list of blocks available in c.
func BlockCopy(w io.Writer, r, c io.ReaderAt, wb, cb [][]byte, blocksize int) error {
	// Stringify the hashes for easy comparison.

	var wbs = make([]string, len(wb))
	var cbs = make([]string, len(cb))
	var cbOffset = make(map[string]int64)
	for i, b := range wb {
		wbs[i] = fmt.Sprintf("%x", b)
	}
	for i, b := range cb {
		s := fmt.Sprintf("%x", b)
		cbs[i] = s
		cbOffset[s] = int64(i * blocksize)
	}

	// For each block, read it from c if it's available there.
	// Otherwise get it from r. Then write it.

	var buf = make([]byte, blocksize)
	var err error
	for i, b := range wbs {
		if offs, ok := cbOffset[b]; ok {
			_, err = c.ReadAt(buf, offs)
		} else {
			_, err = r.ReadAt(buf, int64(i*blocksize))
		}
		if err != nil {
			return err
		}
		_, err = w.Write(buf)
		if err != nil {
			return err
		}
	}

	return nil
}
