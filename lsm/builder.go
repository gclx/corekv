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
	"io"
	"math"
	"os"
	"sort"
	"unsafe"

	"github.com/hardcore-os/corekv/iterator"

	"github.com/hardcore-os/corekv/utils/codec/pb"

	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec"
)

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

const headerSize = uint16(unsafe.Sizeof(header{}))

func NewTableBuilder(opt *Options) *SSTableBuilder {
	return &SSTableBuilder{
		opt: opt,
	}
}

func (sstb *SSTableBuilder) flush(lm *levelManager, tableName string) (t *table, err error) {
	bd := sstb.done()
	t = &table{lm: lm, fid: utils.FID(tableName)}
	// 如果没有builder 则创打开一个已经存在的sst文件
	t.ss = file.OpenSStable(&file.Options{
		FileName: tableName,
		Dir:      lm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(bd.size)})
	buf := make([]byte, bd.size)
	written := bd.Copy(buf)
	utils.CondPanic(written != len(buf), fmt.Errorf("tableBuilder.flush written != len(buf)"))
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

func (sstb *SSTableBuilder) done() buildData {
	sstb.finishBlock()
	if len(sstb.blockList) == 0 {
		return buildData{}
	}
	bd := buildData{
		blockList: sstb.blockList,
	}

	// 构建BloomFilter
	var f utils.Filter
	if sstb.opt.BloomFalsePositive > 0 {
		bits := utils.BloomBitsPerKey(len(sstb.keyHashes), sstb.opt.BloomFalsePositive)
		f = utils.NewFilter(sstb.keyHashes, bits)
	}

	// 构建sst的索引
	index, dataSize := sstb.buildIndex(f)
	checksum := sstb.calculateChecksum(index)
	bd.index = index
	bd.checksum = checksum
	bd.size = int(dataSize) + len(index) + len(checksum) + 4 + 4
	return bd
}

func (sstb *SSTableBuilder) buildIndex(bloom []byte) ([]byte, uint32) {
	tableIndex := &pb.TableIndex{}
	if len(bloom) > 0 {
		tableIndex.BloomFilter = bloom
	}
	tableIndex.KeyCount = sstb.keyCount
	tableIndex.MaxVersion = sstb.maxVersion
	tableIndex.Offsets = sstb.writeBlockOffsets(tableIndex)

	var dataSize uint32
	for i := range sstb.blockList {
		dataSize += uint32(sstb.blockList[i].end)
	}
	data, err := tableIndex.Marshal()
	utils.Panic(err)
	return data, dataSize
}

func (sstb *SSTableBuilder) writeBlockOffsets(tableIndex *pb.TableIndex) []*pb.BlockOffset {
	var startOffset uint32
	var offsets []*pb.BlockOffset
	for _, bl := range sstb.blockList {
		offset := sstb.writeBlockOffset(bl, startOffset)
		offsets = append(offsets, offset)
		startOffset += uint32(bl.end)
	}
	return offsets
}

func (sstb *SSTableBuilder) writeBlockOffset(bl *block, startOffset uint32) *pb.BlockOffset {
	offset := &pb.BlockOffset{}
	offset.Key = bl.baseKey
	offset.Len = uint32(bl.end)
	offset.Offset = startOffset
	return offset
}

func (sstb *SSTableBuilder) add(e *codec.Entry) {
	key := e.Key
	// 检查是否需要分配新的block
	if sstb.tryFinishBlock(e) {
		sstb.finishBlock()
		sstb.curBlock = &block{
			data: make([]byte, sstb.opt.BlockSize),
		}
	}

	sstb.keyHashes = append(sstb.keyHashes, utils.Hash(codec.ParseKey(key)))

	// ParseTs用来解析时间戳，为了实现多版本，真实的Key都是带时间戳存储的
	if version := codec.ParseTs(key); version > sstb.maxVersion {
		sstb.maxVersion = version
	}

	var diffKey []byte
	if len(sstb.curBlock.baseKey) == 0 {
		sstb.curBlock.baseKey = append(sstb.curBlock.baseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = sstb.keyDiff(key)
	}

	h := header{
		overlap: uint16(len(key) - len(diffKey)),
		diff:    uint16(len(diffKey)),
	}

	sstb.curBlock.entryOffsets = append(sstb.curBlock.entryOffsets, uint32(sstb.curBlock.end))

	sstb.append(h.encode())
	sstb.append(diffKey)

	dst := sstb.allocate(int(e.EncodedSize()))
	e.EncodeEntry(dst)
}

// 判断是否需要分配新的block
func (sstb *SSTableBuilder) tryFinishBlock(e *codec.Entry) bool {
	if sstb.curBlock == nil {
		return true
	}
	if len(sstb.curBlock.entryOffsets) <= 0 {
		return false
	}
	utils.CondPanic(!((uint32(len(sstb.curBlock.entryOffsets)+1)*4 + 4 + 8 + 4) < math.MaxInt32), errors.New("Integer overflow"))
	entriesOffsetsSize := uint32((len(sstb.curBlock.entryOffsets)+1)*4 + 4 + 8 + 4)
	estimateSize := uint32(sstb.curBlock.end) + uint32(6+uint32(len(e.Key))+uint32(e.EncodedSize())) + entriesOffsetsSize
	utils.CondPanic(!(uint64(sstb.curBlock.end)+uint64(estimateSize) < math.MaxInt32), errors.New("Integer overflow"))

	return estimateSize > uint32(sstb.opt.BlockSize)
}

func (sstb *SSTableBuilder) finishBlock() {
	if sstb.curBlock == nil || len(sstb.curBlock.entryOffsets) == 0 {
		return
	}
	sstb.append(codec.U32SliceToBytes(sstb.curBlock.entryOffsets))
	sstb.append(codec.U32ToBytes(uint32(len(sstb.curBlock.entryOffsets))))

	checksum := sstb.calculateChecksum(sstb.curBlock.data[:sstb.curBlock.end])
	sstb.append(checksum)
	sstb.append(codec.U32ToBytes(uint32(len(checksum))))

	sstb.blockList = append(sstb.blockList, sstb.curBlock)

	sstb.keyCount += uint32(len(sstb.curBlock.entryOffsets))
	return
}

func (sstb *SSTableBuilder) calculateChecksum(data []byte) []byte {
	checkSum := utils.CalculateChecksum(data)
	return codec.U64ToBytes(checkSum)
}

func (sstb *SSTableBuilder) append(data []byte) {
	dst := sstb.allocate(len(data))
	utils.CondPanic(len(data) != copy(dst, data), errors.New("tableBuilder.append data"))
}

func (sstb *SSTableBuilder) allocate(need int) []byte {
	cb := sstb.curBlock
	if len(cb.data[cb.end:]) < need {
		sz := 2 * len(cb.data)
		if cb.end+need > sz {
			sz = cb.end + need
		}
		tmp := make([]byte, sz)
		copy(tmp, cb.data)
		cb.data = tmp
	}
	cb.end += need
	return cb.data[cb.end-need : cb.end]
}

func (sstb *SSTableBuilder) keyDiff(key []byte) []byte {
	var i int
	for i = 0; i < len(key) && i < len(sstb.curBlock.baseKey); i++ {
		if key[i] != sstb.curBlock.baseKey[i] {
			break
		}
	}
	return key[i:]
}

func (h header) encode() []byte {
	var b [4]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}

func (h *header) decode(buf []byte) {
	copy(((*[headerSize]byte)(unsafe.Pointer(h))[:]), buf[:headerSize])
}

func (b block) verifyChecksum() error {
	return utils.VerifyChecksum(b.data, b.checksum)
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

	tableID     uint64
	blockID     int
	prevOverlap uint16

	it iterator.Item
}

func (itr *blockIterator) setBlock(b *block) {
	itr.block = b
	itr.err = nil
	itr.idx = 0
	itr.baseKey = itr.baseKey[:0]
	itr.prevOverlap = 0
	itr.key = itr.key[:0]
	itr.val = itr.val[:0]

	itr.data = b.data[:b.entriesIndexStart]
	itr.entryOffsets = b.entryOffsets
}

func (itr *blockIterator) Error() error {
	return itr.err
}

func (itr *blockIterator) Item() iterator.Item {
	return itr.it
}

func (itr *blockIterator) seek(key []byte) {
	itr.err = nil
	startIndex := 0
	foundEntryIdx := sort.Search(len(itr.entryOffsets), func(idx int) bool {
		if idx < startIndex {
			return false
		}
		itr.setIdx(idx)
		return utils.CompareKeys(itr.key, key) >= 0
	})
	itr.setIdx(foundEntryIdx)
}

func (itr *blockIterator) setIdx(i int) {
	itr.idx = i
	if i >= len(itr.entryOffsets) || i < 0 {
		itr.err = io.EOF
		return
	}
	itr.err = nil
	startOffset := int(itr.entryOffsets[i])

	if len(itr.baseKey) == 0 {
		var baseHeader header
		baseHeader.decode(itr.data)
		itr.baseKey = itr.data[headerSize : headerSize+baseHeader.diff]
	}

	var endOffset int
	if itr.idx+1 == len(itr.entryOffsets) {
		endOffset = len(itr.data)
	} else {
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
	e := codec.NewEntry(itr.key, nil)
	e.DecodeEntry(entryData[valueOff:])
	itr.it = &Item{e: e}
}
