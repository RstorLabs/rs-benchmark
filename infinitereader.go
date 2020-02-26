package main

import (
	"fmt"
	"io"
	"math/rand"
)

type (
	// InfiniteReader it is bigger on the inside
	InfiniteReader struct {
		Size  int64
		wrote int64
	}
)

const (
	// This value is the maximum. Some go internal functions cannot use a higher value
	BlockSize = 512
)

func (t *InfiniteReader) Read(p []byte) (n int, err error) {
	var toWrite int64
	if t.wrote == t.Size {
		return 0, io.EOF
	}
	if t.wrote+BlockSize < t.Size {
		toWrite = BlockSize
	} else {
		toWrite = t.Size - t.wrote
	}

	p = p[0:toWrite]
	w, err := rand.Read(p)
	if err != nil {
		t.wrote = t.wrote + int64(w)
		return w, err
	}

	t.wrote = t.wrote + toWrite
	return int(toWrite), nil
}

func (t *InfiniteReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		t.wrote = offset
	case io.SeekCurrent:
		t.wrote = t.wrote + offset
	case io.SeekEnd:
		t.wrote = t.Size + offset
	default:
		return 0, fmt.Errorf("unhandled whence %d", whence)
	}
	return 0, nil
}
