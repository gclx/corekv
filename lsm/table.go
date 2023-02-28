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
	"io"
	"os"
	"sort"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/hardcore-os/corekv/utils/codec/pb"

	"github.com/hardcore-os/corekv/iterator"

	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec"
)

// TODO LAB 这里实现 table

type table struct {
	ss  *file.SSTable
	lm  *levelManager
	fid uint64
	ref int32
}

type tableIterator struct {
	it       iterator.Item
	opt      *iterator.Options
	t        *table
	blockPos int
	bi       *blockIterator
	err      error
}

func openTable(lm *levelManager, tableName string, builder *SSTableBuilder) *table {
	sstSize := int(lm.opt.SSTableMaxSz)
	if builder != nil {
		sstSize = int(builder.done().size)
	}
	var (
		t   *table
		err error
	)
	fid := utils.FID(tableName)
	// 对builder存在的情况 把buf flush到磁盘
	if builder != nil {
		if t, err = builder.flush(lm, tableName); err != nil {
			utils.Err(err)
			return nil
		}
	} else {
		t = &table{lm: lm, fid: fid}
		// 如果没有builder 则创打开一个已经存在的sst文件
		t.ss = file.OpenSStable(&file.Options{
			FileName: tableName,
			Dir:      lm.opt.WorkDir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    int(sstSize)})
	}
	// 先要引用一下，否则后面使用迭代器会导致引用状态错误
	t.IncrRef()
	//  初始化sst文件，把index加载进来
	if err := t.ss.Init(); err != nil {
		utils.Err(err)
		return nil
	}

	// 获取sst的最大key 需要使用迭代器
	itr := t.NewIterator(&iterator.Options{}) // 默认是降序
	defer itr.Close()
	// 定位到初始位置就是最大的key
	itr.Rewind()
	utils.CondPanic(!itr.Valid(), errors.Errorf("failed to read index, form maxKey"))
	maxKey := itr.Item().Entry().Key
	t.ss.SetMaxKey(maxKey)

	return t
}

func (t *table) Search(key []byte, maxVs *uint64) (entry *codec.Entry, err error) {
	// 获取索引
	idx := t.ss.Indexs()

	// 检查key是否存在
	bloomFilter := utils.Filter(idx.BloomFilter)
	if t.ss.HasBloomFilter() && !bloomFilter.MayContainKey(key) {
		return nil, utils.ErrKeyNotFound
	}
	iter := t.NewIterator(&iterator.Options{})
	defer iter.Close()

	iter.Seek(key)
	if iter.Valid() {
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

func (t *table) NewIterator(options *iterator.Options) iterator.Iterator {
	return &tableIterator{
		opt: options,
		t:   t,
		bi:  &blockIterator{},
	}
}

// 加载对应的block
func (t *table) block(idx int) (*block, error) {
	if idx >= len(t.ss.Indexs().Offsets) {
		return nil, errors.New("block out of index")
	}
	var b *block
	// 先去cache找
	key := t.blockCacheKey(idx)
	blk, ok := t.lm.cache.blocks.Get(key)
	if ok && blk != nil {
		b, _ = blk.(*block)
		return b, nil
	}

	// 不在cache中
	var ko pb.BlockOffset
	utils.CondPanic(!t.offsets(&ko, idx), fmt.Errorf("block t.offset id = %d", idx))
	b = &block{
		offset: int(ko.GetOffset()),
	}

	var err error
	if b.data, err = t.read(b.offset, int(ko.GetLen())); err != nil {
		return nil, errors.Wrapf(err, "failed to read from sstable: %d at offset: %d, len: %d",
			t.ss.FID(), b.offset, ko.GetLen())
	}
	readPos := len(b.data) - 4
	b.chkLen = int(codec.BytesToU32(b.data[readPos : readPos+4]))

	if b.chkLen > len(b.data) {
		return nil, errors.New("invalid checksum length")
	}

	readPos -= b.chkLen
	b.checksum = b.data[readPos : readPos+b.chkLen]

	readPos -= 4
	numEntries := int(codec.BytesToU32(b.data[readPos : readPos+4]))
	entriesIndexStart := readPos - (numEntries * 4)
	entriesIndexEnd := readPos

	b.entryOffsets = codec.BytesToU32Slice(b.data[entriesIndexStart:entriesIndexEnd])
	b.entriesIndexStart = entriesIndexStart
	b.data = b.data[:readPos+4]

	if err = b.verifyChecksum(); err != nil {
		return nil, err
	}

	t.lm.cache.blocks.Set(key, b)

	return b, nil
}

func (t *table) blockCacheKey(idx int) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[:4], uint32(t.fid))
	binary.BigEndian.PutUint32(buf[4:], uint32(idx))
	return buf
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

func (t *table) read(off, sz int) ([]byte, error) {
	return t.ss.Bytes(off, sz)
}

func (it *tableIterator) Seek(key []byte) {
	var ko pb.BlockOffset
	idx := sort.Search(len(it.t.ss.Indexs().GetOffsets()), func(idx int) bool {
		utils.CondPanic(!it.t.offsets(&ko, idx), fmt.Errorf("table Seek idx < 0 || idx > len(index.GetOffsets())"))
		return utils.CompareKeys(ko.GetKey(), key) > 0
	})
	if idx == 0 {
		it.seekHelper(0, key)
		return
	}
	it.seekHelper(idx-1, key)
	if it.err == io.EOF {
		if idx == len(it.t.ss.Indexs().Offsets) {
			return
		}
		it.seekHelper(idx, key)
	}
}

func (it *tableIterator) seekHelper(blockIdx int, key []byte) {
	it.blockPos = blockIdx
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

func (it *tableIterator) Next() {

}

func (it *tableIterator) Valid() bool {
	return it == nil
}

func (it *tableIterator) Rewind() {

}

func (it *tableIterator) Item() iterator.Item {
	return it.it
}

func (it *tableIterator) Close() error {
	return nil
}

func (t *table) Size() int64 {
	return int64(t.ss.Size())
}

func (t *table) Delete() error {
	return t.ss.Delete()
}

func (t *table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

func (t *table) DecrRef() error {
	newRef := atomic.AddInt32(&t.ref, -1)
	if newRef == 0 {
		if err := t.Delete(); err != nil {
			return err
		}
	}
	return nil
}

func (t *table) decrRefs(tables []*table) error {
	for _, table := range tables {
		if err := table.DecrRef(); err != nil {
			return err
		}
	}
	return nil
}
