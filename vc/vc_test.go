package vc_test

import (
	"testing"

	"github.com/calmh/syncthing/vc"
)

var orderTests = []struct {
	a, b []int64
	o    vc.Order
}{
	{
		nil,
		[]int64{1, 2, 3, 4},
		vc.Lesser,
	},
	{
		[]int64{1, 2, 3, 4},
		nil,
		vc.Greater,
	},
	{
		[]int64{0, 1, 2, 3},
		[]int64{1, 2, 3, 4},
		vc.Lesser,
	},
	{
		[]int64{0, 1, 2, 3},
		[]int64{0, 9, 2, 3},
		vc.Lesser,
	},
	{
		[]int64{1, 2, 3, 4},
		[]int64{0, 1, 2, 3},
		vc.Greater,
	},
	{
		[]int64{0, 1, 9, 3},
		[]int64{0, 1, 2, 3},
		vc.Greater,
	},
	{
		[]int64{1, 1, 3, 4},
		[]int64{1, 1, 3, 4},
		vc.Equal,
	},
	{
		[]int64{4, 2, 2, 1},
		[]int64{4, 2, 2, 1},
		vc.Equal,
	},
	{
		[]int64{4, 1, 2, 1},
		[]int64{4, 2, 1, 1},
		vc.Conflicting,
	},
	{
		[]int64{9, 3, 3, 1},
		[]int64{2, 1, 1, 9},
		vc.Conflicting,
	},
}

func TestOrdering(t *testing.T) {
	for i, tc := range orderTests {
		r := vc.Compare(tc.a, tc.b)
		if r != tc.o {
			t.Errorf("%d: %v.compareTo(%v) => %d != %d", i, tc.a, tc.b, r, tc.o)
		}
	}
}

func TestInc(t *testing.T) {
	a := []int64{0, 0, 0}
	for i := 0; i < 1000; i++ {
		b := vc.Copy(a)
		vc.Inc(i%3, b)
		if vc.Compare(b, a) != vc.Greater {
			t.Fatalf("IncCopy %d: %v, %v", i, b, a)
		}
		a = b
	}
	for i := 0; i < 1000; i++ {
		b := vc.Copy(a)
		vc.Inc(1, b)
		if vc.Compare(b, a) != vc.Greater {
			t.Fatalf("IncCopy %d: %v, %v", i, b, a)
		}
		a = b
	}
}
