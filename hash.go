package bcastkv

type Hash struct {
	keys map[string]*Entry
}

func newHash() *Hash {
	h := new(hash)
	h.keys = make(map[string]*Entry)
	return h
}

func (h *Hash) writeTo(f *fileWrapper, key string, value []byte, expire int32) error {
	entry := new(Entry)
	var err error

	if f == nil || f.file == nil {
		panic("file is nil")
	}

	entry.vpos, entry.vsize, err = f.storeData(key, value, expire)

	entry.fp = f
	entry.tstamp = 0
	if len(value) == 0 {
		delete(h.keys, key)
	} else {
		h.keys[key] = entry
	}
	return err
}
