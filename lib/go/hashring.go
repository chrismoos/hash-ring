/*

Golang client wrapper for the C consistent hash ring library http://chrismoos.com

Example:

	ring := hashring.New(128, hashring.MD5)
	defer ring.Free()

	ring.Add([]byte("node1"))
	ring.Add([]byte("node2"))

	node := string(ring.FindNode([]byte("key")))
*/
package hashring

/*
#cgo LDFLAGS: -lhashring

#include <stdlib.h>
#include <hash_ring.h>
*/
import "C"

import "unsafe"
import "sync"

const (
	MD5  = C.HASH_FUNCTION_MD5
	SHA1 = C.HASH_FUNCTION_SHA1
)

type Ring struct {
	ptr *C.hash_ring_t
	sync.RWMutex
}

// Construct a new hash ring. fn is MD5 or SHA1
func New(numReplicas int, fn C.HASH_FUNCTION) *Ring {
	return &Ring{ptr: C.hash_ring_create(C.uint32_t(numReplicas), fn)}
}

// Add a node to the ring
func (r *Ring) Add(node []byte) {
	r.Lock()
	defer r.Unlock()
	C.hash_ring_add_node(r.ptr, (*C.uint8_t)(&node[0]), C.uint32_t(len(node)))
}

// Remove a node from the ring
func (r *Ring) Remove(node []byte) {
	r.Lock()
	defer r.Unlock()
	C.hash_ring_remove_node(r.ptr, (*C.uint8_t)(&node[0]), C.uint32_t(len(node)))
}

// Print the members of the ring to stdout
func (r *Ring) Print() {
	r.RLock()
	defer r.RUnlock()
	C.hash_ring_print(r.ptr)
}

// Find the node for a given key
func (r *Ring) FindNode(key []byte) []byte {
	r.RLock()
	defer r.RUnlock()
	var s *C.hash_ring_node_t
	s = C.hash_ring_find_node(r.ptr, (*C.uint8_t)(&key[0]), C.uint32_t(len(key)))
	return C.GoBytes(unsafe.Pointer(s.name), C.int(s.nameLen))
}

func (r *Ring) FindNodes(key []byte, num int) [][]byte {
	r.RLock()
	defer r.RUnlock()

	nodes := make([]*C.hash_ring_node_t, num)
	got := C.hash_ring_find_nodes(
		r.ptr,
		(*C.uint8_t)(&key[0]),
		C.uint32_t(len(key)),
		&nodes[0],
		C.uint32_t(num))

	ret := make([][]byte, got)
	for i := 0; i < int(got); i++ {
		ret[i] = C.GoBytes(unsafe.Pointer(nodes[i].name), C.int(nodes[i].nameLen))
	}
	return ret
}

// Cleanly dispose of the ring
func (r *Ring) Free() {
	r.Lock()
	defer r.Unlock()
	C.hash_ring_free(r.ptr)
}
