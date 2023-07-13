package utils

import "github.com/hardcore-os/corekv/utils/codec"

type Iterator interface {
	Next()
	Valid() bool
	Rewind()
	Item() Item
	Close() error
	Seek(key []byte)
}

// Item _
type Item interface {
	Entry() *codec.Entry
}

// Options _
// TODO 可能被重构
type Options struct {
	Prefix []byte
	IsAsc  bool
}
