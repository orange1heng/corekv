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

package file

import (
	"github.com/golang/protobuf/proto"
	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec"
	"github.com/hardcore-os/corekv/utils/codec/pb"
	"github.com/pkg/errors"
	"io"
	"os"
	"sync"
	"syscall"
	"time"
)

// TODO LAB 在这里实现 sst 文件操作

type SSTable struct {
	lock           *sync.RWMutex
	f              *MmapFile
	maxKey         []byte
	minKey         []byte
	idxTables      *pb.TableIndex
	hasBloomFilter bool
	idxLen         int
	idxStart       int
	fid            uint64
	createdAt      time.Time
}

// Bytes returns data starting from offset off of size sz. If there's not enough data, it would
// return nil slice and io.EOF.
func (ss *SSTable) Bytes(off, sz int) ([]byte, error) {
	return ss.f.Bytes(off, sz)
}

func (ss *SSTable) MinKey() []byte {
	return ss.minKey
}

func (ss *SSTable) MaxKey() []byte {
	return ss.maxKey
}

// 反序列化SST文件 -> SSTable
func (ss *SSTable) Init() error {
	var ko *pb.BlockOffset
	var err error
	if ko, err = ss.initTable(); err != nil {
		return err
	}
	// 从文件中获取创建时间
	stat, _ := ss.f.Fd.Stat()
	statType := stat.Sys().(*syscall.Stat_t)
	ss.createdAt = time.Unix(statType.Atimespec.Sec, statType.Atimespec.Nsec)
	// init min key
	keyBytes := ko.GetKey()
	minKey := make([]byte, len(keyBytes))
	copy(minKey, keyBytes)
	ss.minKey = minKey
	ss.maxKey = minKey
	return nil
}

func (ss *SSTable) initTable() (bo *pb.BlockOffset, err error) {
	readPos := len(ss.f.Data)
	// Read checksum_len from the last 4 bytes.
	readPos -= 4
	buf := ss.readCheckError(readPos, 4)
	checksumLen := int(codec.BytesToU32(buf))
	if checksumLen < 0 {
		return nil, errors.New("checksum length less than zero. Data corrupted")
	}
	// Read checksum.
	readPos -= checksumLen
	expectedChk := ss.readCheckError(readPos, checksumLen)

	// Read index_len from the footer.
	readPos -= 4
	buf = ss.readCheckError(readPos, 4)
	ss.idxLen = int(codec.BytesToU32(buf))

	// Read index
	readPos -= ss.idxLen
	ss.idxStart = readPos
	data := ss.readCheckError(readPos, ss.idxLen)
	if err := utils.VerifyChecksum(data, expectedChk); err != nil {
		return nil, errors.Wrapf(err, "failed to verify checksum for table: %s", ss.f.Fd.Name())
	}

	indexTable := &pb.TableIndex{}
	if err := proto.Unmarshal(data, indexTable); err != nil {
		return nil, err
	}
	ss.idxTables = indexTable

	ss.hasBloomFilter = len(indexTable.BloomFilter) > 0
	if len(indexTable.GetOffsets()) > 0 {
		return indexTable.GetOffsets()[0], nil
	}
	return nil, errors.New("read index fail, offset is nil")
}

func (ss *SSTable) readCheckError(off, sz int) []byte {
	buf, err := ss.read(off, sz)
	utils.Panic(err)
	return buf
}

func (ss *SSTable) read(off, sz int) ([]byte, error) {
	if len(ss.f.Data) > 0 {
		if len(ss.f.Data[off:]) < sz {
			return nil, io.EOF
		}
		return ss.f.Data[off : off+sz], nil
	}
	// todo：mmap没有对应的内存，为什么会有这种情况？
	res := make([]byte, sz)
	_, err := ss.f.Fd.ReadAt(res, int64(off))
	return res, err
}

func (ss *SSTable) SetMaxKey(key []byte) {
	ss.maxKey = key
}

// Indexs _
func (ss *SSTable) Indexs() *pb.TableIndex {
	return ss.idxTables
}

// Size 返回底层文件的尺寸
func (ss *SSTable) Size() int64 {
	fileStats, err := ss.f.Fd.Stat()
	utils.Panic(err)
	return fileStats.Size()
}

func (ss *SSTable) FID() uint64 {
	return ss.fid
}

func (ss *SSTable) HasBloomFilter() bool {
	return ss.hasBloomFilter
}

// Detele _
func (ss *SSTable) Detele() error {
	return ss.f.Delete()
}

func OpenSSTable(opt *Options) *SSTable {
	omf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	utils.Err(err)
	return &SSTable{f: omf, fid: uint64(opt.FID), lock: &sync.RWMutex{}}
}
