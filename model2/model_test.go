package model

import (
	"fmt"
	"reflect"
	"testing"
)

func TestGlobalSet(t *testing.T) {
	m := Model{}

	local := []File{
		File{Key{"a", 1000, 0, 0}, nil},
		File{Key{"b", 1000, 0, 0}, nil},
		File{Key{"c", 1000, 0, 0}, nil},
		File{Key{"d", 1000, 0, 0}, nil},
	}

	remote := []File{
		File{Key{"a", 1000, 0, 0}, nil},
		File{Key{"b", 1001, 0, 0}, nil},
		File{Key{"c", 1000, 1, 0}, nil},
		File{Key{"e", 1000, 0, 0}, nil},
	}

	expectedGlobal := map[string]globalRecord{
		"a": globalRecord{key: local[0].Key, availability: 3},
		"b": globalRecord{key: remote[1].Key, availability: 2},
		"c": globalRecord{key: remote[2].Key, availability: 2},
		"d": globalRecord{key: local[3].Key, availability: 1},
		"e": globalRecord{key: remote[3].Key, availability: 2},
	}

	m.SetLocal(local)
	m.SetRemote(1, remote)

	if !reflect.DeepEqual(m.global, expectedGlobal) {
		t.Errorf("Global incorrect;\n%v !=\n%v", m.global, expectedGlobal)
	}

	if lb := len(m.blocks); lb != 7 {
		t.Errorf("Num blocks incorrect %d != 7\n%v", lb, m.blocks)
	}
}

func BenchmarkSetLocal10k(b *testing.B) {
	m := Model{}

	var local []File
	for i := 0; i < 10000; i++ {
		local = append(local, File{Key{fmt.Sprintf("file%d"), 1000, 0, 0}, nil})
	}

	var remote []File
	for i := 0; i < 10000; i++ {
		remote = append(remote, File{Key{fmt.Sprintf("file%d"), 1000, 0, 0}, nil})
	}

	m.SetRemote(1, remote)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.SetLocal(local)
	}
}

func BenchmarkSetLocal10(b *testing.B) {
	m := Model{}

	var local []File
	for i := 0; i < 10; i++ {
		local = append(local, File{Key{fmt.Sprintf("file%d"), 1000, 0, 0}, nil})
	}

	var remote []File
	for i := 0; i < 10000; i++ {
		remote = append(remote, File{Key{fmt.Sprintf("file%d"), 1000, 0, 0}, nil})
	}

	m.SetRemote(1, remote)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.SetLocal(local)
	}
}

func BenchmarkAddLocal10k(b *testing.B) {
	m := Model{}

	var local []File
	for i := 0; i < 10000; i++ {
		local = append(local, File{Key{fmt.Sprintf("file%d"), 1000, 0, 0}, nil})
	}

	var remote []File
	for i := 0; i < 10000; i++ {
		remote = append(remote, File{Key{fmt.Sprintf("file%d"), 1000, 0, 0}, nil})
	}

	m.SetRemote(1, remote)
	m.SetLocal(local)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for j := range local {
			local[j].Key.Version++
		}
		b.StartTimer()
		m.AddLocal(local)
	}
}

func BenchmarkAddLocal10(b *testing.B) {
	m := Model{}

	var local []File
	for i := 0; i < 10; i++ {
		local = append(local, File{Key{fmt.Sprintf("file%d"), 1000, 0, 0}, nil})
	}

	var remote []File
	for i := 0; i < 10000; i++ {
		remote = append(remote, File{Key{fmt.Sprintf("file%d"), 1000, 0, 0}, nil})
	}

	m.SetRemote(1, remote)
	m.SetLocal(local)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range local {
			local[j].Key.Version++
		}
		m.AddLocal(local)
	}
}

func TestGlobalReset(t *testing.T) {
	m := Model{}

	local := []File{
		File{Key{"a", 1000, 0, 0}, nil},
		File{Key{"b", 1000, 0, 0}, nil},
		File{Key{"c", 1000, 0, 0}, nil},
		File{Key{"d", 1000, 0, 0}, nil},
	}

	remote := []File{
		File{Key{"a", 1000, 0, 0}, nil},
		File{Key{"b", 1001, 0, 0}, nil},
		File{Key{"c", 1000, 1, 0}, nil},
		File{Key{"e", 1000, 0, 0}, nil},
	}

	expectedGlobal := map[string]globalRecord{
		"a": globalRecord{key: local[0].Key, availability: 1},
		"b": globalRecord{key: local[1].Key, availability: 1},
		"c": globalRecord{key: local[2].Key, availability: 1},
		"d": globalRecord{key: local[3].Key, availability: 1},
	}

	m.SetLocal(local)
	m.SetRemote(1, remote)
	m.SetRemote(1, nil)

	if !reflect.DeepEqual(m.global, expectedGlobal) {
		t.Errorf("Global incorrect;\n%v !=\n%v", m.global, expectedGlobal)
	}

	if lb := len(m.blocks); lb != 4 {
		t.Errorf("Num blocks incorrect %d != 4\n%v", lb, m.blocks)
	}
}

func TestNeed(t *testing.T) {
	m := Model{}

	local := []File{
		File{Key{"a", 1000, 0, 0}, nil},
		File{Key{"b", 1000, 0, 0}, nil},
		File{Key{"c", 1000, 0, 0}, nil},
		File{Key{"d", 1000, 0, 0}, nil},
	}

	remote := []File{
		File{Key{"a", 1000, 0, 0}, nil},
		File{Key{"b", 1001, 0, 0}, nil},
		File{Key{"c", 1000, 1, 0}, nil},
		File{Key{"e", 1000, 0, 0}, nil},
	}

	shouldNeed := []File{
		File{Key{"b", 1001, 0, 0}, nil},
		File{Key{"c", 1000, 1, 0}, nil},
		File{Key{"e", 1000, 0, 0}, nil},
	}

	m.SetLocal(local)
	m.SetRemote(1, remote)

	need := m.Need()
	if !reflect.DeepEqual(need, shouldNeed) {
		t.Errorf("Need incorrect;\n%v !=\n%v", need, shouldNeed)
	}
}
