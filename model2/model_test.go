package model

import (
	"fmt"
	"reflect"
	"testing"
)

func TestGlobalSet(t *testing.T) {
	m := Model{}

	local := []File{
		File{FileKey{"a", 1000, 0, 0}, nil},
		File{FileKey{"b", 1000, 0, 0}, nil},
		File{FileKey{"c", 1000, 0, 0}, nil},
		File{FileKey{"d", 1000, 0, 0}, nil},
	}

	remote := []File{
		File{FileKey{"a", 1000, 0, 0}, nil},
		File{FileKey{"b", 1001, 0, 0}, nil},
		File{FileKey{"c", 1000, 1, 0}, nil},
		File{FileKey{"e", 1000, 0, 0}, nil},
	}

	expectedGlobal := map[string]GlobalFile{
		"a": GlobalFile{Key: local[0].Key, Availability: 3},
		"b": GlobalFile{Key: remote[1].Key, Availability: 2},
		"c": GlobalFile{Key: remote[2].Key, Availability: 2},
		"d": GlobalFile{Key: local[3].Key, Availability: 1},
		"e": GlobalFile{Key: remote[3].Key, Availability: 2},
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
		local = append(local, File{FileKey{fmt.Sprintf("file%d"), 1000, 0, 0}, []Block{}})
	}

	var remote []File
	for i := 0; i < 10000; i++ {
		remote = append(remote, File{FileKey{fmt.Sprintf("file%d"), 1000, 0, 0}, []Block{}})
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
		local = append(local, File{FileKey{fmt.Sprintf("file%d"), 1000, 0, 0}, []Block{}})
	}

	var remote []File
	for i := 0; i < 10000; i++ {
		remote = append(remote, File{FileKey{fmt.Sprintf("file%d"), 1000, 0, 0}, []Block{}})
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
		local = append(local, File{FileKey{fmt.Sprintf("file%d"), 1000, 0, 0}, []Block{}})
	}

	var remote []File
	for i := 0; i < 10000; i++ {
		remote = append(remote, File{FileKey{fmt.Sprintf("file%d"), 1000, 0, 0}, []Block{}})
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
		local = append(local, File{FileKey{fmt.Sprintf("file%d"), 1000, 0, 0}, []Block{}})
	}

	var remote []File
	for i := 0; i < 10000; i++ {
		remote = append(remote, File{FileKey{fmt.Sprintf("file%d"), 1000, 0, 0}, []Block{}})
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
		File{FileKey{"a", 1000, 0, 0}, nil},
		File{FileKey{"b", 1000, 0, 0}, nil},
		File{FileKey{"c", 1000, 0, 0}, nil},
		File{FileKey{"d", 1000, 0, 0}, nil},
	}

	remote := []File{
		File{FileKey{"a", 1000, 0, 0}, nil},
		File{FileKey{"b", 1001, 0, 0}, nil},
		File{FileKey{"c", 1000, 1, 0}, nil},
		File{FileKey{"e", 1000, 0, 0}, nil},
	}

	expectedGlobal := map[string]GlobalFile{
		"a": GlobalFile{Key: local[0].Key, Availability: 1},
		"b": GlobalFile{Key: local[1].Key, Availability: 1},
		"c": GlobalFile{Key: local[2].Key, Availability: 1},
		"d": GlobalFile{Key: local[3].Key, Availability: 1},
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
		File{FileKey{"a", 1000, 0, 0}, nil},
		File{FileKey{"b", 1000, 0, 0}, nil},
		File{FileKey{"c", 1000, 0, 0}, nil},
		File{FileKey{"d", 1000, 0, 0}, nil},
	}

	remote := []File{
		File{FileKey{"a", 1000, 0, 0}, nil},
		File{FileKey{"b", 1001, 0, 0}, nil},
		File{FileKey{"c", 1000, 1, 0}, nil},
		File{FileKey{"e", 1000, 0, 0}, nil},
	}

	shouldNeed := []File{
		File{FileKey{"b", 1001, 0, 0}, nil},
		File{FileKey{"c", 1000, 1, 0}, nil},
		File{FileKey{"e", 1000, 0, 0}, nil},
	}

	m.SetLocal(local)
	m.SetRemote(1, remote)

	need := m.Need()
	if !reflect.DeepEqual(need, shouldNeed) {
		t.Errorf("Need incorrect;\n%v !=\n%v", need, shouldNeed)
	}
}
