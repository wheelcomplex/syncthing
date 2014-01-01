package fdcache

import (
	"bytes"
	"testing"
)

func TestOpen(t *testing.T) {
	var buf = make([]byte, 5)
	var correct = []byte("hello")

	f1, err := Open("testdata/foo")
	if err != nil {
		t.Fatal(err)
	}
	if u := f1.(*readFile).usage; u != 1 {
		t.Errorf("Incorrect f1 usage %d != 1", u)
	}

	n, err := f1.ReadAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(correct) || bytes.Compare(buf, correct) != 0 {
		t.Error("Incorrect data in read")
	}

	f2, err := Open("testdata/foo")
	if err != nil {
		t.Fatal(err)
	}
	if u := f1.(*readFile).usage; u != 2 {
		t.Errorf("Incorrect f1 usage %d != 2", u)
	}
	if u := f2.(*readFile).usage; u != 2 {
		t.Errorf("Incorrect f2 usage %d != 2", u)
	}

	n, err = f1.ReadAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(correct) || bytes.Compare(buf, correct) != 0 {
		t.Error("Incorrect data in read")
	}

	n, err = f2.ReadAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(correct) || bytes.Compare(buf, correct) != 0 {
		t.Error("Incorrect data in read")
	}

	f1.Close()

	if u := f1.(*readFile).usage; u != 1 {
		t.Errorf("Incorrect f1 usage %d != 1", u)
	}
	if u := f2.(*readFile).usage; u != 1 {
		t.Errorf("Incorrect f2 usage %d != 1", u)
	}

	n, err = f2.ReadAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(correct) || bytes.Compare(buf, correct) != 0 {
		t.Error("Incorrect data in read")
	}

	f2.Close()
	_, err = f2.ReadAt(buf, 0)
	if err == nil {
		t.Error("Unexpected successfull read after close")
	}
}
