package vc

type Order int

const (
	Lesser      Order = -1
	Equal             = 0
	Greater           = 1
	Conflicting       = 2
)

func Inc(i int, c []int64) {
	c[i]++
}

func Copy(c []int64) []int64 {
	v := make([]int64, len(c))
	copy(v, c)
	return v
}

func Compare(a, b []int64) Order {
	var r Order
	if a == nil {
		return Lesser
	}
	if b == nil {
		return Greater
	}
	if len(a) != len(b) {
		panic("different length clocks are incomparable")
	}
	for i := range a {
		switch {
		case a[i] < b[i] && (r == Lesser || r == Equal):
			r = Lesser
		case a[i] > b[i] && (r == Greater || r == Equal):
			r = Greater
		case a[i] != b[i]:
			return Conflicting
		}
	}

	return r
}
