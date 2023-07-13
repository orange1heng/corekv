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
	"bytes"
	"errors"
	"fmt"
	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec"
	"github.com/hardcore-os/corekv/utils/codec/pb"
	"io"
	"math"
	"os"
	"sort"
	"unsafe"
)

// TODO LAB 这里实现 序列化

type tableBuilder struct {
	opt        *Options
	sstSize    int64
	curBlock   *block
	blockList  []*block
	keyCount   uint32
	maxVersion uint64
	baseKey    []byte

	staleDataSize int
	estimateSz    int64
	keyHashes     []uint32
}

type buildData struct {
	blockList []*block
	index     []byte
	checksum  []byte
	size      int
}

type block struct {
	data         []byte
	offset       int // 当前block的offset首地址
	checksum     []byte
	chkLen       int
	entryOffsets []uint32
	baseKey      []byte

	entriesIndexStart int
	end               int // 当前已经存储的数据位置（从0开始）
	estimateSz        int64
}

func (b block) verifyCheckSum() error {
	return utils.VerifyChecksum(b.data, b.checksum)
}

type header struct {
	overlap uint16 // Overlap with base key.
	diff    uint16 // Length of the diff.
}

type blockIterator struct {
	data         []byte
	idx          int
	err          error
	baseKey      []byte
	key          []byte
	val          []byte
	entryOffsets []uint32
	block        *block

	tableID uint64
	blockID int

	prevOverlap uint16

	it utils.Item
}

const headerSize = uint16(unsafe.Sizeof(header{}))

func (h header) encode() []byte {
	var b [4]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}

// Decode decodes the header.
func (h *header) decode(buf []byte) {
	copy((*[headerSize]byte)(unsafe.Pointer(h))[:], buf[:headerSize])
}

func (tb *tableBuilder) add(e *codec.Entry, isStale bool) {
	key := e.Key
	val := codec.ValueStruct{
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
	}
	// 1.检查添加这个entry对象是否超出当前block的最大size
	// 2.超出最大size，序列化当前block，并新建一个block来保存这个entry
	if tb.tryFinishBlock(e) {
		if isStale {
			// This key will be added to tableIndex and it is stale.
			// todo：??什么意思
			tb.staleDataSize += len(key) + 4 /* len */ + 4 /* offset */
		}
		tb.finishBlock()
		tb.curBlock = &block{
			data: make([]byte, tb.opt.BlockSize), // TODO 加密block后块的大小会增加，需要预留一些填充位置
		}
	}
	// 3.没有超出最大size，加入到当前block

	// 用于之后生成布隆过滤器
	tb.keyHashes = append(tb.keyHashes, utils.Hash(codec.ParseKey(key)))

	if version := codec.ParseTs(key); version > tb.maxVersion {
		tb.maxVersion = version
	}

	var diffKey []byte
	if len(tb.curBlock.baseKey) == 0 {
		tb.curBlock.baseKey = append(tb.curBlock.baseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = tb.keyDiff(key)
	}
	// 如果 前缀 的长度 > 16位报错，因为给它设计的最长长度 就是uin16
	utils.CondPanic(!(len(key)-len(diffKey) <= math.MaxUint16), fmt.Errorf("tableBuilder.add: len(key)-len(diffKey) <= math.MaxUint16"))
	utils.CondPanic(!(len(diffKey) <= math.MaxUint16), fmt.Errorf("tableBuilder.add: len(diffKey) <= math.MaxUint16"))

	h := header{
		overlap: uint16(len(key) - len(diffKey)),
		diff:    uint16(len(diffKey)),
	}
	tb.curBlock.entryOffsets = append(tb.curBlock.entryOffsets, uint32(tb.curBlock.end))

	tb.append(h.encode())
	tb.append(diffKey)

	dst := tb.allocate(int(val.EncodedSize()))
	val.EncodeValue(dst)
}

func (tb *tableBuilder) done() buildData {
	tb.finishBlock()
	if len(tb.blockList) == 0 {
		return buildData{}
	}
	bd := buildData{
		blockList: tb.blockList,
	}
	var f utils.Filter

	if tb.opt.BloomFalsePositive > 0 {
		bitsPerKey := utils.BloomBitsPerKey(len(tb.keyHashes), tb.opt.BloomFalsePositive)
		f = utils.NewFilter(tb.keyHashes, bitsPerKey)
	}

	index, dataSize := tb.buildIndex(f)
	checksum := tb.calculateChecksum(index)
	bd.index = index
	bd.checksum = checksum
	bd.size = int(dataSize) + len(index) + len(checksum) +
		4 + // index_len
		4 // checksum_len
	return bd
}

func (tb *tableBuilder) buildIndex(bloom utils.Filter) ([]byte, uint32) {
	tableIndex := &pb.TableIndex{}
	if len(bloom) > 0 {
		tableIndex.BloomFilter = bloom
	}
	tableIndex.KeyCount = tb.keyCount
	tableIndex.MaxVersion = tb.maxVersion
	tableIndex.Offsets = tb.writeBlockOffsets(tableIndex)

	var dataSize uint32
	for i := range tb.blockList {
		dataSize += uint32(tb.blockList[i].end)
	}
	data, err := tableIndex.Marshal()
	utils.Panic(err)

	return data, dataSize
}

func (tb *tableBuilder) writeBlockOffsets(tableIndex *pb.TableIndex) []*pb.BlockOffset {
	var startOffset uint32
	var offsets []*pb.BlockOffset
	for _, bl := range tb.blockList {
		offset := tb.writeBlockOffset(bl, startOffset)
		offsets = append(offsets, offset)
		startOffset += uint32(bl.end)
	}
	return offsets
}

func (b *tableBuilder) writeBlockOffset(bl *block, startOffset uint32) *pb.BlockOffset {
	offset := &pb.BlockOffset{}
	offset.Key = bl.baseKey
	offset.Len = uint32(bl.end)
	offset.Offset = startOffset
	return offset
}

func (tb *tableBuilder) tryFinishBlock(e *codec.Entry) bool {
	if tb.curBlock == nil {
		return true
	}
	if len(tb.curBlock.entryOffsets) <= 0 {
		return false
	}

	// 需要这个判断吗？
	utils.CondPanic(!((uint32(len(tb.curBlock.entryOffsets))+1)*4+4+8+4 < math.MaxUint32), errors.New("Integer overflow"))
	// 字节数
	entriesOffsetsSize := int64((len(tb.curBlock.entryOffsets)+1)*4 + // len(offsets)
		4 + // size of list(offset_len)
		8 + // Sum64 in checksum proto
		4) // checksum length
	tb.curBlock.estimateSz = int64(tb.curBlock.end) +
		int64(6 /*header size for entry*/) +
		int64(len(e.Key)) + // key
		int64(e.EncodedSize()) + // len(value)
		entriesOffsetsSize // index part

	utils.CondPanic(!(uint64(tb.curBlock.end)+uint64(tb.curBlock.estimateSz) < math.MaxUint32), errors.New("Integer overflow"))

	return tb.curBlock.estimateSz > int64(tb.opt.BlockSize)
}

// 序列化 current block
func (tb *tableBuilder) finishBlock() {
	if tb.curBlock == nil || len(tb.curBlock.entryOffsets) == 0 {
		return
	}
	tb.append(codec.U32SliceToBytes(tb.curBlock.entryOffsets))
	tb.append(codec.U32ToBytes(uint32(len(tb.curBlock.entryOffsets))))

	checksum := tb.calculateChecksum(tb.curBlock.data[:tb.curBlock.end])
	tb.append(checksum)
	tb.append(codec.U32ToBytes(uint32(len(checksum))))

	tb.estimateSz += tb.curBlock.estimateSz
	tb.blockList = append(tb.blockList, tb.curBlock)
	// TODO: 预估整理builder写入磁盘后，sst文件的大小
	tb.keyCount += uint32(len(tb.curBlock.entryOffsets))
	tb.curBlock = nil
	return
}

// append appends to curBlock.data
func (tb *tableBuilder) append(data []byte) {
	dst := tb.allocate(len(data))
	utils.CondPanic(len(data) != copy(dst, data), errors.New("tableBuilder.append data"))
}

func (tb *tableBuilder) allocate(need int) []byte {
	bb := tb.curBlock
	if len(bb.data[bb.end:]) < need {
		// reallocate
		sz := 2 * len(bb.data)
		if bb.end+need > sz {
			sz = bb.end + need
		}
		tmp := make([]byte, sz) // todo 这里可以使用内存分配器来提升性能
		copy(tmp, bb.data)
		bb.data = tmp
	}
	bb.end += need
	return bb.data[bb.end-need : bb.end]
}

func (tb *tableBuilder) calculateChecksum(data []byte) []byte {
	checkSum := utils.CalculateChecksum(data)
	return codec.U64ToBytes(checkSum)
}

func (tb *tableBuilder) keyDiff(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(tb.curBlock.baseKey); i++ {
		if newKey[i] != tb.curBlock.baseKey[i] {
			break
		}
	}
	return newKey[i:]
}

func (tb *tableBuilder) flush(lm *levelManager, tableName string) (t *table, err error) {
	bd := tb.done()
	t = &table{lm: lm, fid: utils.FID(tableName)}
	t.ss = file.OpenSSTable(&file.Options{
		FileName: tableName,
		Dir:      lm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(bd.size),
	})
	buf := make([]byte, bd.size)

	written := bd.Copy(buf)
	utils.CondPanic(written != len(buf), fmt.Errorf("tableBuilder.flush written != len(buf)"))

	// 从mmap buffer中拿到一个足够大小的buf字节数组
	dst, err := t.ss.Bytes(0, bd.size)
	if err != nil {
		return nil, err
	}
	copy(dst, buf)
	return t, nil
}

func (bd *buildData) Copy(dst []byte) int {
	var written int
	for _, bl := range bd.blockList {
		written += copy(dst[written:], bl.data[:bl.end])
	}
	written += copy(dst[written:], bd.index)
	written += copy(dst[written:], codec.U32ToBytes(uint32(len(bd.index))))

	written += copy(dst[written:], bd.checksum)
	written += copy(dst[written:], codec.U32ToBytes(uint32(len(bd.checksum))))
	return written
}

func (itr *blockIterator) setBlock(b *block) {
	itr.block = b
	itr.err = nil
	itr.idx = 0
	itr.baseKey = itr.baseKey[:0]
	itr.prevOverlap = 0
	itr.key = itr.key[:0]
	itr.val = itr.val[:0]
	// Drop the index from the block. We don't need it anymore.
	itr.data = b.data[:b.entriesIndexStart]
	itr.entryOffsets = b.entryOffsets
}

// seekToFirst brings us to the first element.
func (itr *blockIterator) seekToFirst() {
	itr.setIdx(0)
}
func (itr *blockIterator) seekToLast() {
	itr.setIdx(len(itr.entryOffsets) - 1)
}
func (itr *blockIterator) seek(key []byte) {
	itr.err = nil
	startIndex := 0 // This tells from which index we should start binary search.

	foundEntryIdx := sort.Search(len(itr.entryOffsets), func(idx int) bool {
		// If idx is less than start index then just return false.
		if idx < startIndex {
			return false
		}
		itr.setIdx(idx)
		return utils.CompareKeys(itr.key, key) >= 0
	})
	itr.setIdx(foundEntryIdx)
}

// 将block的指针定位到idx的item
func (itr *blockIterator) setIdx(i int) {
	itr.idx = i
	if i >= len(itr.entryOffsets) || i < 0 {
		itr.err = io.EOF
		return
	}
	itr.err = nil
	startOffset := int(itr.entryOffsets[i])

	// Set base key.
	if len(itr.baseKey) == 0 {
		var baseHeader header
		baseHeader.decode(itr.data)
		itr.baseKey = itr.data[headerSize : headerSize+baseHeader.diff]
	}

	var endOffset int
	// idx points to the last entry in the block.
	if itr.idx+1 == len(itr.entryOffsets) {
		endOffset = len(itr.data)
	} else {
		// idx point to some entry other than the last one in the block.
		// EndOffset of the current entry is the start offset of the next entry.
		endOffset = int(itr.entryOffsets[itr.idx+1])
	}
	defer func() {
		if r := recover(); r != nil {
			var debugBuf bytes.Buffer
			fmt.Fprintf(&debugBuf, "==== Recovered====\n")
			fmt.Fprintf(&debugBuf, "Table ID: %d\nBlock ID: %d\nEntry Idx: %d\nData len: %d\n"+
				"StartOffset: %d\nEndOffset: %d\nEntryOffsets len: %d\nEntryOffsets: %v\n",
				itr.tableID, itr.blockID, itr.idx, len(itr.data), startOffset, endOffset,
				len(itr.entryOffsets), itr.entryOffsets)
			panic(debugBuf.String())
		}
	}()

	entryData := itr.data[startOffset:endOffset]
	var h header
	h.decode(entryData)
	if h.overlap > itr.prevOverlap {
		itr.key = append(itr.key[:itr.prevOverlap], itr.baseKey[itr.prevOverlap:h.overlap]...)
	}

	itr.prevOverlap = h.overlap
	valueOff := headerSize + h.diff
	diffKey := entryData[headerSize:valueOff]
	itr.key = append(itr.key[:h.overlap], diffKey...)
	e := &codec.Entry{Key: itr.key}
	val := &codec.ValueStruct{}
	val.DecodeValue(entryData[valueOff:])
	itr.val = val.Value
	e.Value = val.Value
	e.ExpiresAt = val.ExpiresAt
	//e.Meta = val.Meta
	itr.it = &Item{e: e}
}

func (itr *blockIterator) Error() error {
	return itr.err
}

func (itr *blockIterator) Next() {
	itr.setIdx(itr.idx + 1)
}

func (itr *blockIterator) Valid() bool {
	return itr.err != io.EOF // TODO 这里用err比较好
}

// 重置
func (itr *blockIterator) Rewind() bool {
	itr.setIdx(0)
	return true
}

func (itr *blockIterator) Item() utils.Item {
	return itr.it
}

func (itr *blockIterator) Close() error {
	return nil
}
