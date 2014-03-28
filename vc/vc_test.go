package vc_test

import (
	"testing"

	"github.com/calmh/syncthing/vc"
)

var orderTests = []struct {
	a, b vc.Clock
	o    vc.Order
}{
	{
		vc.Clock([]int64{0, 1, 2, 3}),
		vc.Clock([]int64{1, 2, 3, 4}),
		vc.Lesser,
	},
	{
		vc.Clock([]int64{0, 1, 2, 3}),
		vc.Clock([]int64{0, 9, 2, 3}),
		vc.Lesser,
	},
	{
		vc.Clock([]int64{1, 2, 3, 4}),
		vc.Clock([]int64{0, 1, 2, 3}),
		vc.Greater,
	},
	{
		vc.Clock([]int64{0, 1, 9, 3}),
		vc.Clock([]int64{0, 1, 2, 3}),
		vc.Greater,
	},
	{
		vc.Clock([]int64{1, 1, 3, 4}),
		vc.Clock([]int64{1, 1, 3, 4}),
		vc.Equal,
	},
	{
		vc.Clock([]int64{4, 2, 2, 1}),
		vc.Clock([]int64{4, 2, 2, 1}),
		vc.Equal,
	},
	{
		vc.Clock([]int64{4, 1, 2, 1}),
		vc.Clock([]int64{4, 2, 1, 1}),
		vc.Conflicting,
	},
	{
		vc.Clock([]int64{9, 3, 3, 1}),
		vc.Clock([]int64{2, 1, 1, 9}),
		vc.Conflicting,
	},
}

func TestOrdering(t *testing.T) {
	for i, tc := range orderTests {
		r := tc.a.CompareTo(tc.b)
		if r != tc.o {
			t.Errorf("%d: %v.compareTo(%v) => %d != %d", i, tc.a, tc.b, r, tc.o)
		}
	}
}

func TestInc(t *testing.T) {
	a := vc.Clock([]int64{0, 0, 0})
	for i := 0; i < 1000; i++ {
		b := a.IncCopy(i % 3)
		if b.CompareTo(a) != vc.Greater {
			t.Fatalf("IncCopy %d: %v, %v", i, b, a)
		}
		a = b
	}
	for i := 0; i < 1000; i++ {
		b := a.IncCopy(1)
		if b.CompareTo(a) != vc.Greater {
			t.Fatalf("IncCopy %d: %v, %v", i, b, a)
		}
		a = b
	}
}
