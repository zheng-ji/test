package bcastkv

import (
	"encoding/json"
	"io"
	"os"
	"time"
)

type BcastKv struct {
	filename string
	activefp *fileWrapper
	keyhash  *Hash
}

func NewBcastKv(filename string) (kv *BcastKv, err error) {
	kv = new(BcastKv)
	kv.filename = filename
	err = kv.init()
	return kv, err
}

// open KV store.
func (kv *BcastKv) init() (err error) {
	var activeFile *os.File
	kv.keyhash = NewHash()
	activeFile, err = os.OpenFile(kv.filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)
	if err != nil {
		return err
	}
	kv.activefp = NewfileWrapper(activeFile)
	err = kv.load2hash()
	return err
}

func (kv *BcastKv) Close() {
	kv.isInit()
	if kv.activefp != nil {
		kv.activefp.file.Close()
	}
}

func (kv *BcastKv) isInit() {
	if kv.keyhash == nil {
		panic("hashkey is invalid")
	}
	if kv.activefp == nil {
		panic("activefile is not defined")
	}
}

func (kv *BcastKv) Get(key string, value interface{}) error {
	kv.isInit()
	e := kv.keyhash.keys[key]
	if e == nil {
		return ErrKeyNotFound
	} else {
		bytes, err := e.readValue()
		if err != nil {
			return err
		}
		json.Unmarshal(bytes, &value)
	}
	return nil
}

func (kv *BcastKv) Delete(key string) error {
	kv.isInit()
	bytes := []byte{}
	return kv.keyhash.insert(kv.activefp, key, bytes, 0)
}

func (kv *BcastKv) Put(key string, value interface{}) error {
	kv.isInit()
	if key == "" {
		return ErrBlankKey
	}

	bytes, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return kv.keyhash.insert(kv.activefp, key, bytes, 0)
}

func (kv *BcastKv) Exist(key string) bool {
	kv.isInit()
	if e := kv.keyhash.keys[key]; e == nil {
		return false
	}
	return true
}

func (kv *BcastKv) load2hash() (ret error) {
	hash := kv.keyhash
	kv.activefp.file.Seek(0, 0) /* place the cursor in the begin of the file */
	seconds := time.Now().Unix()
	today := int32(seconds / 86400)

	for {
		_, tstamp, _, vsz, vpos, keydata, err := kv.activefp.readHeader()

		if err != nil && err != io.EOF {
			ret = err
			break
		} else if err == io.EOF {
			break
		}

		key := string(keydata)
		entry := new(Entry)
		entry.vpos = vpos
		entry.vsize = vsz
		entry.tstamp = 0
		entry.fp = kv.activefp

		if vsz == 0 { // this is deleted value
			delete(hash.keys, key)
		} else if tstamp != 0 && tstamp < today { // this value has expired
			delete(hash.keys, key)
		} else {
			hash.keys[key] = entry
		}
		if err == io.EOF {
			break
		}
	}
	return ret
}
