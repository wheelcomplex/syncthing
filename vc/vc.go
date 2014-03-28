package vc

import "time"

type Order int

const (
	Lesser      Order = -1
	Equal             = 0
	Greater           = 1
	Conflicting       = 2
)

type Clock []int64

func (c Clock) Inc(i int) {
	t := time.Now().UnixNano()
	if c[i] < t {
		c[i] = t
	} else {
		c[i]++
	}
}

func (c Clock) IncCopy(i int) Clock {
	cn := make(Clock, len(c))
	copy(cn, c)
	cn.Inc(i)
	return cn
}

func (c Clock) CompareTo(o Clock) Order {
	var r Order
	if len(c) != len(o) {
		panic("different length clocks are incomparable")
	}
	for i := range c {
		switch {
		case c[i] < o[i] && (r == Lesser || r == Equal):
			r = Lesser
		case c[i] > o[i] && (r == Greater || r == Equal):
			r = Greater
		case c[i] != o[i]:
			return Conflicting
		}
	}

	return r
}
