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

// TODO LAB 这里实现 序列化

type SSTableBuilder struct {
	sstSize       int64
	curBlock      *block
	opt           *Options
	blockList     []*block
	keyCount      uint32
	keyHashes     []uint32
	maxVersion    uint64
	baseKey       []byte
	staleDataSize int
	estimateSz    int64
}

type buildData struct {
	blockList []*block
	index     []byte
	checksum  []byte
	size      int
}

type block struct {
	offset            int
	checksum          []byte
	chkLen            int
	entriesIndexStart int
	entryOffsets      []uint32
	data              []byte
	baseKey           []byte
	end               int
	estimateSz        int64
}

type header struct {
	overlap uint16 // base key
	diff    uint16 // diff key
}

func (sstb *SSTableBuilder) done() buildData {

}
