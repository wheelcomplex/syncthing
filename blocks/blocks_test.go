package model

import (
	"bytes"
	"fmt"
	"testing"
)

var testdata = []struct {
	data      []byte
	blocksize int
	hash      []string
}{
	{[]byte(""), 1024, []string{
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}},
	{[]byte("contents"), 1024, []string{
		"d1b2a59fbea7e20077af9f91b27e95e865061b270be03ff539ab3b73587882e8"}},
	{[]byte("contents"), 9, []string{
		"d1b2a59fbea7e20077af9f91b27e95e865061b270be03ff539ab3b73587882e8"}},
	{[]byte("contents"), 8, []string{
		"d1b2a59fbea7e20077af9f91b27e95e865061b270be03ff539ab3b73587882e8"}},
	{[]byte("contents"), 7, []string{
		"ed7002b439e9ac845f22357d822bac1444730fbdb6016d3ec9432297b9ec9f73",
		"043a718774c572bd8a25adbeb1bfcd5c0256ae11cecf9f9c3f925d0e52beaf89"},
	},
	{[]byte("contents"), 3, []string{
		"1143da2bc54c495c4be31d3868785d39ffdfd56df5668f0645d8f14d47647952",
		"e4432baa90819aaef51d2a7f8e148bf7e679610f3173752fabb4dcb2d0f418d3",
		"44ad63f60af0f6db6fdde6d5186ef78176367df261fa06be3079b6c80c8adba4"},
	},
	{[]byte("conconts"), 3, []string{
		"1143da2bc54c495c4be31d3868785d39ffdfd56df5668f0645d8f14d47647952",
		"1143da2bc54c495c4be31d3868785d39ffdfd56df5668f0645d8f14d47647952",
		"44ad63f60af0f6db6fdde6d5186ef78176367df261fa06be3079b6c80c8adba4"},
	},
	{[]byte("contenten"), 3, []string{
		"1143da2bc54c495c4be31d3868785d39ffdfd56df5668f0645d8f14d47647952",
		"e4432baa90819aaef51d2a7f8e148bf7e679610f3173752fabb4dcb2d0f418d3",
		"e4432baa90819aaef51d2a7f8e148bf7e679610f3173752fabb4dcb2d0f418d3"},
	},
}

func TestHash(t *testing.T) {
	for _, test := range testdata {
		buf := bytes.NewBuffer(test.data)
		size, blocks, err := Hash(buf, test.blocksize)

		if err != nil {
			t.Fatal(err)
		}

		if size != int64(len(test.data)) {
			t.Fatalf("Incorrect size returned, %d != %d", size, len(test.data))
		}
		if l := len(blocks); l != len(test.hash) {
			t.Fatalf("Incorrect number of blocks %d != %d", l, len(test.hash))
		}

		for i, corblock := range test.hash {
			if block := fmt.Sprintf("%x", blocks[i]); block != corblock {
				t.Errorf("Incorrect block hash %q != %q", block, corblock)
			}
		}
	}
}
