package sut

type header struct {
	ptype int // 8 bits
	seqno int // 24 bits
	wnd   int // 8 bits
	ackno int // 24 bits
}

func marshalHeader(h header) []byte {
	var bs [8]byte
	bs[0] = uint8(h.ptype)
	bs[1] = uint8(h.seqno >> 16)
	bs[2] = uint8(h.seqno >> 8)
	bs[3] = uint8(h.seqno)
	bs[4] = uint8(h.wnd)
	bs[5] = uint8(h.ackno >> 16)
	bs[6] = uint8(h.ackno >> 8)
	bs[7] = uint8(h.ackno)
	return bs[:]
}

func unmarshalHeader(bs []byte) header {
	var h header
	h.ptype = int(bs[0])
	h.seqno = int(bs[1])<<16 + int(bs[2])<<8 + int(bs[3])
	h.wnd = int(bs[4])
	h.ackno = int(bs[5])<<16 + int(bs[6])<<8 + int(bs[7])
	return h
}
