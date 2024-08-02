package main

import (
	"fmt"
	"hash"
	"hash/fnv"
	"unsafe"
)

func main() {
	h := newHasher()
	fmt.Println(h.getHash(15))
	fmt.Println(h.getHash(15))
	fmt.Println(h.getHash(16))
	fmt.Println(h.getHash(15))
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
