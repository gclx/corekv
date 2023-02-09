package utils

import (
	"bytes"
	"math/rand"
	"sync"

	"github.com/hardcore-os/corekv/utils/codec"
)

const (
	defaultMaxLevel = 48
)

type SkipList struct {
	header *Element

	rand *rand.Rand

	maxLevel int
	length   int
	lock     sync.RWMutex
	size     int64
}

func NewSkipList() *SkipList {
	//implement me here!!!

	header := &Element{
		levels: make([]*Element, defaultMaxLevel),
	}

	return &SkipList{
		header:   header,
		maxLevel: defaultMaxLevel - 1,
	}
}

type Element struct {
	levels []*Element
	entry  *codec.Entry
	score  float64
}

func newElement(score float64, entry *codec.Entry, level int) *Element {
	return &Element{
		levels: make([]*Element, level),
		entry:  entry,
		score:  score,
	}
}

func (elem *Element) Entry() *codec.Entry {
	return elem.entry
}

func (list *SkipList) Add(data *codec.Entry) error {
	//implement me here!!!
	list.lock.Lock()
	defer list.lock.Unlock()

	keyScore := list.calcScore(data.Key)
	prev := list.header
	prevElemHeaders := make([]*Element, defaultMaxLevel+1)
	for i := list.maxLevel; i >= 0; i-- {
		for node := prev.levels[i]; node != nil; node = prev.levels[i] {
			if comp := list.compare(keyScore, data.Key, node); comp <= 0 {
				if comp == 0 {
					node.entry = data
					return nil
				} else {
					prev = node
				}
			} else {
				break
			}
		}
		prevElemHeaders[i] = prev
	}
	newLevel := list.randLevel()
	newElement := newElement(keyScore, data, newLevel)
	for i := newLevel - 1; i >= 0; i-- {
		next := prevElemHeaders[i].levels[i]
		// newElement.levels[i] = prevElemHeaders[i].levels[i]
		prevElemHeaders[i].levels[i] = newElement
		newElement.levels[i] = next
	}
	return nil
}

func (list *SkipList) Search(key []byte) (e *codec.Entry) {
	//implement me here!!!
	list.lock.Lock()
	defer list.lock.Unlock()

	keyScore := list.calcScore(key)
	prev := list.header
	for i := list.maxLevel; i >= 0; i-- {
		for node := prev.levels[i]; node != nil; node = prev.levels[i] {
			if comp := list.compare(keyScore, key, node); comp <= 0 {
				if comp == 0 {
					return node.entry
				} else {
					prev = node
				}
			} else {
				break
			}
		}
	}
	return nil
}

func (list *SkipList) Close() error {
	return nil
}

func (list *SkipList) calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	//implement me here!!!
	if score == next.score {
		return bytes.Compare(key, next.entry.Key)
	}
	if score < next.score {
		return -1
	} else {
		return 1
	}
}

func (list *SkipList) randLevel() int {
	//implement me here!!!
	i := 1

	for ; i < list.maxLevel; i++ {
		if rand.Intn(2) == 0 {
			return i
		}
	}
	return list.maxLevel
}

func (list *SkipList) Size() int64 {
	//implement me here!!!
	return list.size
}
