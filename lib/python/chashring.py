"""
 * Copyright 2011 Instagram
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
"""

import ctypes
from contextlib import contextmanager

class HashRingNode(ctypes.Structure):
    _fields_ = [("name", ctypes.c_char_p),
                 ("nameLen", ctypes.c_int)]

class HashRingException(Exception):
    pass

class HashFunction:
    SHA1 = 1
    MD5 = 2

NODE_NAME_ARGTYPES = (ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int)
HASH_RING_NODE_POINTER = ctypes.POINTER(HashRingNode)

hash_ring = None

def set_up_hash_ring_ctypes():
    global hash_ring
    try:
        hash_ring = ctypes.cdll.LoadLibrary('libhashring.so')
    except OSError:
        try:
            hash_ring = ctypes.cdll.LoadLibrary('libhashring.dylib')
        except OSError:
            raise HashRingException("Shared library could not be loaded")

    hash_ring.hash_ring_create.restype = ctypes.c_void_p

    hash_ring.hash_ring_add_node.argtypes = NODE_NAME_ARGTYPES

    hash_ring.hash_ring_remove_node.argtypes = NODE_NAME_ARGTYPES

    hash_ring.hash_ring_get_node.argtypes = NODE_NAME_ARGTYPES
    hash_ring.hash_ring_get_node.restype = HASH_RING_NODE_POINTER

    hash_ring.hash_ring_find_node.argtypes = NODE_NAME_ARGTYPES
    hash_ring.hash_ring_find_node.restype = HASH_RING_NODE_POINTER

    hash_ring.hash_ring_print.argtypes = (ctypes.c_void_p,)

    hash_ring.hash_ring_free.argtypes = (ctypes.c_void_p,)
    hash_ring.hash_ring_free.restype = ctypes.c_void_p

set_up_hash_ring_ctypes()

class HashRing(object):

    def __init__(self, nodes=None, num_replicas=128, hash_fn=HashFunction.MD5):
        if not hash_ring:
            raise HashRingException("Shared library not loaded successfully")
        if not nodes:
            nodes = []
        self._hash_ring_ptr = hash_ring.hash_ring_create(num_replicas, hash_fn)
        for node in nodes:
            hash_ring.hash_ring_add_node(self._hash_ring_ptr, node, len(node))

    def add_node(self, node):
        hash_ring.hash_ring_add_node(self._hash_ring_ptr, node, len(node))

    def remove_node(self, node):
        hash_ring.hash_ring_remove_node(self._hash_ring_ptr, node, len(node))

    def check_node_in_ring(self, node):
        get_val = hash_ring.hash_ring_get_node(self._hash_ring_ptr, node, len(node))
        return bool(get_val)

    def print_ring(self):
        hash_ring.hash_ring_print(self._hash_ring_ptr)

    def find_node(self, key):
        node_struct_ptr = hash_ring.hash_ring_find_node(self._hash_ring_ptr, key, len(key))
        if node_struct_ptr:
            node_struct = node_struct_ptr.contents
            return node_struct.name[:node_struct.nameLen]
        return None

    def free(self):
        hash_ring.hash_ring_free(self._hash_ring_ptr)

@contextmanager
def create_hash_ring(nodes):
    """ Use this to safely use a HashRing object without worrying about having to
        call 'free' afterwards.
    """
    h = HashRing(nodes)
    yield h
    h.free()