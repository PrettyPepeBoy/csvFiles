package filer

import (
	"github.com/spf13/viper"
	"hash"
	"hash/fnv"
	"slices"
	"sync"
	"unsafe"
)

type Storage struct {
	fileStorage       map[string]*Data
	hashedId          [][]int
	hash              *hasher
	mx                sync.Mutex
	hashBucketsAmount int
}

type Data struct {
	id map[int]struct{}
}

func NewStorage() *Storage {
	hashBucketsAmount := viper.GetInt("storage.hash-buckets.amount")

	return &Storage{
		hashedId:          make([][]int, hashBucketsAmount),
		fileStorage:       make(map[string]*Data),
		hash:              newHasher(),
		hashBucketsAmount: hashBucketsAmount,
	}
}

func (d *Data) put(id int) {
	d.id[id] = struct{}{}
}

func (s *Storage) add(id int, filename string, notUnique bool) bool {
	s.mx.Lock()
	defer s.mx.Unlock()
	bucketNumber := int(s.hash.getHash(id)) % s.hashBucketsAmount
	i, find := slices.BinarySearch(s.hashedId[bucketNumber], id)
	if find && !notUnique {
		return false
	}

	if !find {
		s.hashedId[bucketNumber] = insertInBucket(s.hashedId[bucketNumber], id, i)
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

func (h *hasher) getHash(v int) uint32 {
	ptr := (*byte)(unsafe.Pointer(&v))
	data := unsafe.Slice(ptr, h.size)
	defer h.hash.Reset()

	_, err := h.hash.Write(data)
	if err != nil {
		panic(err)
	}

	return h.hash.Sum32()
}

func insertInBucket(bucket []int, id int, index int) []int {
	if len(bucket) == 0 {
		bucket = append(bucket, id)
		return bucket
	}

	if index == 0 {
		bucket = append(bucket[:index+1], bucket...)
		bucket[index] = 0
		return bucket
	}

	bucket = append(bucket[:index], bucket[index-1:]...)
	bucket[index] = id
	return bucket
}

func (s *Storage) loadData(id int, filename string) {
	bucketNumber := int(s.hash.getHash(id)) % s.hashBucketsAmount

	i, find := slices.BinarySearch(s.hashedId[bucketNumber], id)
	if !find {
		s.hashedId[bucketNumber] = insertInBucket(s.hashedId[bucketNumber], id, i)
	}

	s.fileStorage[filename].put(id)
}

func (s *Storage) getData(filename string) []int {
	id := make([]int, 0, len(s.fileStorage[filename].id))
	for key := range s.fileStorage[filename].id {
		id = append(id, key)
	}

	return id
}
