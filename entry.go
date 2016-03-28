package bcastkv

type Entry struct {
	fp     *fileWrapper
	vsize  int32
	vpos   int32
	tstamp int64
}

func (e *Entry) readValue() (value []byte, err error) {
	value = make([]byte, e.vsize)
	var length int
	length, err = e.fp.file.ReadAt(value, int64(e.vpos))
	if int32(length) != e.vsize {
		err = errors.New(fmt.Sprintf("Expected %d bytes got %d", e.vsize, length))
	}
	return
}
