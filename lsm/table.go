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
	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/utils"
)

// TODO LAB 这里实现 table

type table struct {
	ss  *file.SSTable
	lm  *levelManager
	fid uint64
}

func openTable(lm *levelManager, tableName string, builder *SSTableBuilder) *table {
	sstSize := int(lm.opt.SSTableMaxSz)
	if builder != nil {
		sstSize = int(builder.done().size)
	}

}

func Search(key []byte, maxVs *uint64) (entry Entry, err error) {

	return nil, utils.ErrKeyNotFound
}
