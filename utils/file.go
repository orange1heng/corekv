// Copyright 2021 logicrec Project Authors
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

package utils

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/hardcore-os/corekv/utils/codec"
	"github.com/pkg/errors"
)

// FID 根据file name 获取其fid
func FID(name string) uint64 {
	name = path.Base(name)
	if !strings.HasSuffix(name, ".sst") {
		return 0
	}
	//	suffix := name[len(fileSuffix):]
	name = strings.TrimSuffix(name, ".sst")
	id, err := strconv.Atoi(name)
	if err != nil {
		Err(err)
		return 0
	}
	return uint64(id)
}

// FileNameSSTable  sst 文件名
func FileNameSSTable(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.sst", id))
}

// openDir opens a directory for syncing.
func openDir(path string) (*os.File, error) { return os.Open(path) }

// SyncDir When you create or delete a file, you have to ensure the directory entry for the file is synced
// in order to guarantee the file is visible (if the system crashes). (See the man page for fsync,
// or see https://github.com/coreos/etcd/issues/6368 for an example.)
func SyncDir(dir string) error {
	f, err := openDir(dir)
	if err != nil {
		return errors.Wrapf(err, "While opening directory: %s.", dir)
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return errors.Wrapf(err, "While syncing directory: %s.", dir)
	}
	return errors.Wrapf(closeErr, "While closing directory: %s.", dir)
}

// LoadIDMap Get the id of all sst files in the current folder
func LoadIDMap(dir string) map[uint64]struct{} {
	fileInfos, err := ioutil.ReadDir(dir)
	Err(err)
	idMap := make(map[uint64]struct{})
	for _, info := range fileInfos {
		if info.IsDir() {
			continue
		}
		fileID := FID(info.Name())
		if fileID != 0 {
			idMap[fileID] = struct{}{}
		}
	}
	return idMap
}

// CompareKeys checks the key without timestamp and checks the timestamp if keyNoTs
// is same.
// a<timestamp> would be sorted higher than aa<timestamp> if we use bytes.compare
// All keys should have timestamp.
func CompareKeys(key1, key2 []byte) int {
	CondPanic((len(key1) <=8 || len(key2) <= 8), fmt.Errorf("%s,%s < 8", key1, key2))
	if cmp := bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8]); cmp != 0 {
		return cmp
	}
	return bytes.Compare(key1[len(key1)-8:], key2[len(key2)-8:])
}

// VerifyChecksum crc32
func VerifyChecksum(data []byte, expected []byte) error {
	actual := uint64(crc32.Checksum(data, CastagnoliCrcTable))
	expectedU64 := codec.BytesToU64(expected)
	if actual != expectedU64 {
		return errors.Wrapf(ErrChecksumMismatch, "actual: %d, expected: %d", actual, expectedU64)
	}

	return nil
}

// CalculateChecksum _
func CalculateChecksum(data []byte) uint64 {
	return uint64(crc32.Checksum(data, CastagnoliCrcTable))
}