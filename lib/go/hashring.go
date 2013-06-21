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
	MD5 = C.HASH_FUNCTION_MD5
)


type Ring struct {
	ptr *C.hash_ring_t
    sync.RWMutex
}


func New(numReplicas int, fn C.HASH_FUNCTION) *Ring {
	return &Ring{ptr:C.hash_ring_create(C.uint32_t(numReplicas), fn)}
}

func (r *Ring) Add(node []byte) {
	r.Lock()
	defer r.Unlock()
	C.hash_ring_add_node(r.ptr, (*C.uint8_t)(&node[0]), C.uint32_t(len(node)))
}

func (r *Ring) Remove(node []byte) {
	r.Lock()
	defer r.Unlock()
	C.hash_ring_remove_node(r.ptr, (*C.uint8_t)(&node[0]), C.uint32_t(len(node)))
}

func (r *Ring) Print() {
	r.RLock()
	defer r.RUnlock()
	C.hash_ring_print(r.ptr)
}

func (r *Ring) FindNode(key []byte) []byte {
	r.RLock()
	defer r.RUnlock()
	var s *C.hash_ring_node_t
	s = C.hash_ring_find_node(r.ptr, (*C.uint8_t)(&key[0]), C.uint32_t(len(key)))
	return C.GoBytes(unsafe.Pointer(s.name), C.int(s.nameLen))
}

func (r *Ring) Free() {
	r.Lock()
	defer r.Unlock()
	C.hash_ring_free(r.ptr)
}
