package filer

import (
	"github.com/spf13/viper"
	"hash"
	"hash/fnv"
	"slices"
	"sync"
	"unsafe"
)

type storage struct {
	fileStorage       map[string]*data
	hashedIds         [][]int
	hash              *hasher
	mx                sync.Mutex
	hashBucketsAmount int
}

type data struct {
	id map[int]struct{}
}

func newStorage() *storage {
	hashBucketsAmount := viper.GetInt("storage.hash-buckets.amount")

	return &storage{
		hashedIds:         make([][]int, hashBucketsAmount),
		fileStorage:       make(map[string]*data),
		hash:              newHasher(),
		hashBucketsAmount: hashBucketsAmount,
	}
}

func (d *data) put(id int) {
	d.id[id] = struct{}{}
}

func (s *storage) add(id int, filename string, notUnique bool) bool {
	s.mx.Lock()
	defer s.mx.Unlock()
	bucketNumber := s.hash.getHash(id) % s.hashBucketsAmount
	i, find := slices.BinarySearch(s.hashedIds[bucketNumber], id)
	if find && !notUnique {
		return false
	}

	if !find {
		s.hashedIds[bucketNumber] = insertInBucket(s.hashedIds[bucketNumber], id, i)
	}

	s.fileStorage[filename].put(id)
	return true
}

type hasher struct {
	size uintptr
	hash hash.Hash32
}

func newHasher() *hasher {
	var tmp int
	return &hasher{
		size: unsafe.Sizeof(tmp),
		hash: fnv.New32(),
	}
}

func (h *hasher) getHash(v int) int {
	ptr := (*byte)(unsafe.Pointer(&v))
	dt := unsafe.Slice(ptr, h.size)
	defer h.hash.Reset()

	_, err := h.hash.Write(dt)
	if err != nil {
		panic(err)
	}

	return int(h.hash.Sum32())
}

func insertInBucket(bucket []int, id int, index int) []int {
	if len(bucket) == 0 {
		bucket = append(bucket, id)
		return bucket
	}

	if index == 0 {
		bucket = append(bucket[:1], bucket...)
		bucket[index] = 0
		return bucket
	}

	bucket = append(bucket[:index], bucket[index-1:]...)
	bucket[index] = id
	return bucket
}

func deleteFromBucket(bucket []int, index int) []int {
	bucket = append(bucket[:index], bucket[index+1:]...)
	return bucket[:len(bucket)-1]
}

func (s *storage) loadData(id int, filename string) {
	bucketNumber := s.hash.getHash(id) % s.hashBucketsAmount

	i, find := slices.BinarySearch(s.hashedIds[bucketNumber], id)
	if !find {
		s.hashedIds[bucketNumber] = insertInBucket(s.hashedIds[bucketNumber], id, i)
	}

	s.fileStorage[filename].put(id)
}

func (s *storage) getData(filename string) ([]int, bool) {
	_, ok := s.fileStorage[filename]
	if !ok {
		return nil, false
	}

	id := make([]int, 0, len(s.fileStorage[filename].id))
	for key := range s.fileStorage[filename].id {
		id = append(id, key)
	}

	return id, true
}

func (s *storage) deleteData(filename string, ids []int) {
	for _, id := range ids {
		bucketNumber := s.hash.getHash(id) % s.hashBucketsAmount

		i, ok := slices.BinarySearch(s.hashedIds[bucketNumber], id)
		if !ok {
			continue
		}

		s.hashedIds[bucketNumber] = deleteFromBucket(s.hashedIds[bucketNumber], i)
		delete(s.fileStorage[filename].id, id)
	}
}
