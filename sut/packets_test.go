package sut

import (
	"bytes"
	"testing"
)

func TestHeaderMarsal(t *testing.T) {
	h := header{
		ptype: 0x34,
		seqno: 0x987654,
		wnd:   0x75,
		ackno: 0x826313,
	}

	actual := marshalHeader(h)
	expected := []byte{0x34, 0x98, 0x76, 0x54, 0x75, 0x82, 0x63, 0x13}

	if bytes.Compare(actual, expected) != 0 {
		t.Errorf("%x != %x", actual, expected)
	}
}

func TestHeaderUnmarsal(t *testing.T) {
	bs := []byte{0x34, 0x98, 0x76, 0x54, 0x75, 0x82, 0x63, 0x13}

	actual := unmarshalHeader(bs)
	expected := header{
		ptype: 0x34,
		seqno: 0x987654,
		wnd:   0x75,
		ackno: 0x826313,
	}

	if actual != expected {
		t.Errorf("%v != %v", actual, expected)
	}
}
