package bcastkv

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
)

type fileWrapper struct {
	file        *os.File
	current_pos int32
}

func NewfileWrapper(f *os.File) *fileWrapper {
	return &fileWrapper{f, 0}
}

func (fp *fileWrapper) storeData(key string, value []byte, expire int32) (vpos int32, vsize int32, err error) {

	buff := new(bytes.Buffer)

	keydata := []byte(key)
	binary.Write(buff, binary.BigEndian, expire)
	binary.Write(buff, binary.BigEndian, int32(len(key)))
	binary.Write(buff, binary.BigEndian, int32(len(value)))
	buff.Write(keydata)
	buff.Write(value)

	crc := crc32.ChecksumIEEE(buff.Bytes())

	vpos = int32(fp.current_pos + RecordHeaderSize + int32(len(keydata)))
	buff2 := new(bytes.Buffer)
	binary.Write(buff2, binary.BigEndian, crc)
	buff2.Write(buff.Bytes())

	var size int
	size, err = fp.file.Write(buff2.Bytes())
	vsize = int32(len(value))
	fp.current_pos += int32(size)
	return vpos, vsize, err
}

func (fp *fileWrapper) readHeader() (crc, tstamp, klen, vlen, vpos int32, key []byte, err error) {
	/* crc + tstamp + len key data + len value */
	var headerbuff []byte = make([]byte, RecordHeaderSize)
	var sz int
	sz, err = fp.file.Read(headerbuff)

	if err != nil {
		return
	}

	if int32(sz) != RecordHeaderSize {
		err = errors.New(fmt.Sprintf("Invalid header size. Expected %d got %d bytes", RecordHeaderSize, sz))
	}

	buff := bufio.NewReader(bytes.NewBuffer(headerbuff))
	binary.Read(buff, binary.BigEndian, &crc)
	binary.Read(buff, binary.BigEndian, &tstamp)
	binary.Read(buff, binary.BigEndian, &klen)
	binary.Read(buff, binary.BigEndian, &vlen)

	key = make([]byte, klen)
	sz, err = fp.file.Read(key)

	if err != nil {
		return
	}

	if int32(sz) != klen {
		err = errors.New(fmt.Sprintf("Invalid key size. Expected %d got %d bytes", klen, sz))
		return
	}

	fp.file.Seek(int64(vlen), 1)
	vpos = fp.current_pos + RecordHeaderSize + klen
	fp.current_pos += int32(RecordHeaderSize + klen + vlen)
	return
}
