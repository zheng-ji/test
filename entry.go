package bcastkv

import (
	"errors"
	"fmt"
)

type Entry struct {
	fp     *fileWrapper
	vsize  int32
	vpos   int32
	tstamp int64
}

func (entry *Entry) readValue() (value []byte, err error) {
	value = make([]byte, entry.vsize)
	var length int
	length, err = entry.fp.file.ReadAt(value, int64(entry.vpos))
	if int32(length) != entry.vsize {
		err = errors.New(fmt.Sprintf("Expected %d bytes got %d", entry.vsize, length))
	}
	return
}
