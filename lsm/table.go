// Copyright 2021 hardcore-os Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lsm

import (
	"encoding/binary"
	"fmt"
	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec"
	"github.com/hardcore-os/corekv/utils/codec/pb"
	"github.com/pkg/errors"
	"io"
	"math"
	"os"
	"sort"
	"sync/atomic"
)

// TODO LAB 这里实现 table

type table struct {
	ss  *file.SSTable
	lm  *levelManager
	fid uint64
	ref int32 // For file garbage collection. Atomic.
}

type tableIterator struct {
	it       utils.Item
	opt      *utils.Options
	t        *table
	blockPos int
	bi       *blockIterator
	err      error
}

func (it tableIterator) Next() {
	it.err = nil

	if it.blockPos >= len(it.t.ss.Indexs().GetOffsets()) {
		it.err = io.EOF
		return
	}

	if len(it.bi.data) == 0 {
		block, err := it.t.block(it.blockPos)
		if err != nil {
			it.err = err
			return
		}
		it.bi.tableID = it.t.fid
		it.bi.blockID = it.blockPos
		it.bi.setBlock(block)
		it.bi.seekToFirst()
		it.err = it.bi.Error()
		return
	}

	it.bi.Next()
	if !it.bi.Valid() {
		it.blockPos++
		it.bi.data = nil
		it.Next()
		return
	}
	it.it = it.bi.it
}

func (it tableIterator) Valid() bool {
	return it.err != io.EOF // 如果没有的时候 则是EOF
}

func (it tableIterator) Rewind() {
	if it.opt.IsAsc {
		it.seekToFirst()
	} else {
		it.seekToLast()
	}
}

func (it tableIterator) Item() utils.Item {
	return it.it
}

func (it tableIterator) Close() error {
	if err := it.bi.Close(); err != nil {
		return err
	}
	return it.t.DecrRef()
}

// Seek
// 二分法搜索 offsets
// 如果idx == 0 说明key只能在第一个block中 block[0].MinKey <= key
// 否则 block[0].MinKey > key
// 如果在 idx-1 的block中未找到key 那才可能在 idx 中
// 如果都没有，则当前key不再此table
func (it tableIterator) Seek(key []byte) {
	var ko pb.BlockOffset
	idx := sort.Search(len(it.t.ss.Indexs().GetOffsets()), func(idx int) bool {
		utils.CondPanic(!it.t.offsets(&ko, idx), fmt.Errorf("tableutils.Seek idx < 0 || idx > len(index.GetOffsets()"))
		if idx == len(it.t.ss.Indexs().GetOffsets()) {
			return true
		}
		return utils.CompareKeys(ko.GetKey(), key) > 0
	})
	if idx == 0 {
		it.seekHelper(0, key)
		return
	}
	it.seekHelper(idx-1, key)
}

func (it *tableIterator) seekHelper(blockIdx int, key []byte) {
	it.blockPos = blockIdx
	// 获取block
	block, err := it.t.block(blockIdx)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seek(key)
	it.err = it.bi.Error()
	it.it = it.bi.Item()
}

func (it *tableIterator) seekToFirst() {
	numBlocks := len(it.t.ss.Indexs().Offsets)
	if numBlocks == 0 {
		it.err = io.EOF
		return
	}
	it.blockPos = 0
	block, err := it.t.block(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seekToFirst()
	it.it = it.bi.Item()
	it.err = it.bi.Error()
}

func (it *tableIterator) seekToLast() {
	numBlocks := len(it.t.ss.Indexs().Offsets)
	if numBlocks == 0 {
		it.err = io.EOF
		return
	}
	it.blockPos = numBlocks - 1
	block, err := it.t.block(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seekToLast()
	it.it = it.bi.Item()
	it.err = it.bi.Error()
}

func (t *table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

// DecrRef decrements the refcount and possibly deletes the table
func (t *table) DecrRef() error {
	newRef := atomic.AddInt32(&t.ref, -1)
	if newRef == 0 {
		// TODO 从缓存中删除
		for i := 0; i < len(t.ss.Indexs().GetOffsets()); i++ {
			t.lm.cache.blocks.Del(t.blockCacheKey(i))
		}
		if err := t.Delete(); err != nil {
			return err
		}
	}
	return nil
}

func (t *table) offsets(ko *pb.BlockOffset, i int) bool {
	index := t.ss.Indexs()
	if i < 0 || i > len(index.GetOffsets()) {
		return false
	}
	if i == len(index.GetOffsets()) {
		return true
	}
	*ko = *index.GetOffsets()[i]
	return true
}

// Size is its file size in bytes
func (t *table) Size() int64 { return int64(t.ss.Size()) }

func (t *table) NewIterator(options *utils.Options) utils.Iterator {
	t.IncrRef()
	return &tableIterator{
		opt: options,
		t:   t,
		bi:  &blockIterator{},
	}
}

// 加载sst对应的block
func (t *table) block(idx int) (*block, error) {
	utils.CondPanic(idx < 0, fmt.Errorf("idx=%d", idx))
	if idx >= len(t.ss.Indexs().Offsets) {
		return nil, errors.New("block out of index")
	}
	// 1. 从 block cache 中拿到block
	var b *block
	key := t.blockCacheKey(idx)
	blk, ok := t.lm.cache.blocks.Get(key)
	if ok && blk != nil {
		b, _ = blk.(*block)
		return b, nil
	}

	// 2. 如果没有则从文件中加载
	var ko pb.BlockOffset
	utils.CondPanic(!t.offsets(&ko, idx), fmt.Errorf("block t.offset id=%d", idx))
	b = &block{
		offset: int(ko.GetOffset()),
	}

	// 2.1 根据 block_offset 中 offset 对象和 len 从 sst 中读取字节数组
	var err error
	if b.data, err = t.read(b.offset, int(ko.GetLen())); err != nil {
		return nil, errors.Wrapf(err,
			"failed to read from sstable: %d at offset: %d, len: %d",
			t.ss.FID(), b.offset, ko.GetLen())
	}
	// 2.2 按写入顺序相反的方向读取数据，反序列化为block对象
	readPos := len(b.data) - 4 // First read checksum length.
	b.chkLen = int(codec.BytesToU32(b.data[readPos : readPos+4]))

	if b.chkLen > len(b.data) {
		return nil, errors.New("invalid checksum length. Either the data is " +
			"corrupted or the table options are incorrectly set")
	}

	// 2.3 全部读取后检查 block 的 checksum 是否一致
	readPos -= b.chkLen
	b.checksum = b.data[readPos : readPos+b.chkLen]

	b.data = b.data[:readPos]

	if err = b.verifyCheckSum(); err != nil {
		return nil, err
	}

	readPos -= 4
	numEntries := int(codec.BytesToU32(b.data[readPos : readPos+4]))

	entriesIndexStart := readPos - (numEntries * 4)
	entriesIndexEnd := entriesIndexStart + numEntries*4

	b.entryOffsets = codec.BytesToU32Slice(b.data[entriesIndexStart:entriesIndexEnd])

	b.entriesIndexStart = entriesIndexStart

	t.lm.cache.blocks.Set(key, b)
	return nil, nil
}

// blockCacheKey is used to store blocks in the block cache.
func (t *table) blockCacheKey(idx int) []byte {
	utils.CondPanic(t.fid >= math.MaxUint32, fmt.Errorf("t.fid >= math.MaxUint32"))
	utils.CondPanic(uint32(idx) >= math.MaxUint32, fmt.Errorf("uint32(idx) >=  math.MaxUint32"))

	buf := make([]byte, 8)
	// Assume t.ID does not overflow uint32.
	binary.BigEndian.PutUint32(buf[:4], uint32(t.fid))
	binary.BigEndian.PutUint32(buf[4:], uint32(idx))
	return buf
}

func (t *table) read(off, sz int) ([]byte, error) {
	return t.ss.Bytes(off, sz)
}

// Serach 从table中查找key
// 这里面创建的迭代器只用一次，所以用完就把迭代器关了
func (t *table) Serach(key []byte, maxVs *uint64) (entry *codec.Entry, err error) {
	t.IncrRef()
	defer t.DecrRef()
	// 获取索引
	idx := t.ss.Indexs()
	// 检查key是否存在
	bloomFilter := utils.Filter(idx.BloomFilter)
	if t.ss.HasBloomFilter() && !bloomFilter.MayContainKey(key) {
		return nil, utils.ErrKeyNotFound
	}
	iter := t.NewIterator(&utils.Options{})
	defer iter.Close()

	iter.Seek(key)
	if !iter.Valid() {
		return nil, utils.ErrKeyNotFound
	}

	if codec.SameKey(key, iter.Item().Entry().Key) {
		if version := codec.ParseTs(iter.Item().Entry().Key); *maxVs < version {
			*maxVs = version
			return iter.Item().Entry(), nil
		}
	}
	return nil, utils.ErrKeyNotFound
}

func (t *table) Delete() error {
	return t.ss.Detele()
}

// 如果builder为0，就是加载文件中的数据到table结构体中
// 如果builder不为0，就使用builder来序列化数据到文件中
func openTable(lm *levelManager, tableName string, builder *tableBuilder) *table {
	// your code: lab_sst
	sstSize := int(lm.opt.SSTableMaxSz)
	if builder != nil {
		sstSize = builder.done().size
	}

	var (
		t   *table
		err error
	)
	fid := utils.FID(tableName)
	if builder != nil {
		// flush buildData to disk
		if t, err = builder.flush(lm, tableName); err != nil {
			utils.Err(err)
			return nil
		}
	} else {
		// 打开一个已经存在的 SST，初始化 table
		t = &table{
			lm:  lm,
			fid: fid,
		}
		t.ss = file.OpenSSTable(&file.Options{
			FileName: tableName,
			Dir:      lm.opt.WorkDir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    sstSize,
		})
	}
	t.IncrRef()
	// todo: 初始化 sst 文件

	//  初始化sst文件，把index加载进来（暂时不加载data block）
	if err := t.ss.Init(); err != nil {
		utils.Err(err)
		return nil
	}

	// 获取sst的最大key 需要使用迭代器
	itr := t.NewIterator(&utils.Options{})
	defer itr.Close()

	// 定位到初始位置就是最大的key
	itr.Rewind()
	utils.CondPanic(!itr.Valid(), errors.Errorf("failed to read index, form maxKey"))
	maxKey := itr.Item().Entry().Key
	t.ss.SetMaxKey(maxKey)

	return t
}
