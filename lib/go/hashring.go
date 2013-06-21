package hashring

/*
#cgo LDFLAGS: -lhashring

#include <stdlib.h>
#include <hash_ring.h>
*/
import "C"

const (
	MD5 = C.HASH_FUNCTION_MD5
)

type Ring struct {
	ptr *C.hash_ring_t
}

func New(numReplicas int, fn C.HASH_FUNCTION) *Ring {
	return &Ring{C.hash_ring_create(C.uint32_t(numReplicas), fn)}
}

func (r *Ring) Add (node []byte) {
	C.hash_ring_add_node(r.ptr, (*C.uint8_t)(&node[0]), C.uint32_t(len(node)))
}

func (r *Ring) Print () {
	C.hash_ring_print(r.ptr)
}
