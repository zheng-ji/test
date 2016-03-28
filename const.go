package bcastkv

import (
	"errors"
)

const (
	RecordHeaderSize int32 = 16
)

var (
	ErrBlankKey    = errors.New("rkv: key can not be blank")
	ErrKeyNotFound = errors.New("rkv: key not found")
)
