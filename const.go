package bcastkv

import (
	"errors"
)

const (
	RecordHeaderSize int32 = 16
)

var (
	ErrBlankKey    = errors.New("blank key not allowed")
	ErrKeyNotFound = errors.New("key not found")
)
