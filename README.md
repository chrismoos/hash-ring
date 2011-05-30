# hash_ring

This library provides a consistent hashing implementation that uses SHA-1. It is high performance and suitable for rings with a large number of items (replicas or nodes).

## License (See LICENSE file for full license)

Copyright 2011 LiveProfile

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Overview

**hash_ring** exposes a simple API to create/remove rings, add/remove nodes, and locate the node for a key on the ring.

A ring is created by using the *hash_ring_create()* function. There is one parameter, *numReplicas*, which determines how many times a node is placed on the ring. This parameter gives you flexibility on how consistent the ring is. Larger values will even out the node distribution on the ring. 128 is a sensible default for most users.

    hash_ring_t *ring = hash_ring_create(128);

Next you can add some nodes to the ring. For example, if you have a cluster of Redis servers, you might do the following:

    char *redis01 = "redis01", *redis02 = "redis02", *redis03 = "redis03";

    hash_ring_add_node(ring, (uint8_t*)redis01, strlen(redis01));
    hash_ring_add_node(ring, (uint8_t*)redis02, strlen(redis02));
    hash_ring_add_node(ring, (uint8_t*)redis03, strlen(redis01));
    
The ring will now have **384** items, **128** per node (with 3 nodes total).

## Node hashing

It is helpful to know how a node is hashed onto the ring, especially if you want to write a compatible library for another language or platform.

**hash_ring** hashes the node's name followed by the ASCII number of the replica. For example, with *2* replicas, the following pseudo code is used to hash the node:

    redis01_0_int = SHA1("redis01" + "0") & 0x000000000000000000000000ffffffffffffffff
    redis01_1_int = SHA1("redis01" + "1") & 0x000000000000000000000000ffffffffffffffff
  
The high **64-bits** of the digest are used to retrieve the number.

## Compiling 

Compile the library with:

    make
    
This will produce a shared library in **build/libhashring.so**.

## Running the tests

    make test
    
## Bindings

### Erlang

    make erl

## Example

The following code is from the tests, which hashes some known keys and ensures that they are located on the ring properly.

    hash_ring_t *ring = hash_ring_create(8);
    char *slotA = "slotA";
    char *slotB = "slotB";

    char *keyA = "keyA";
    char *keyB = "keyBBBB";
    char *keyC = "keyB_";

    hash_ring_node_t *node;

    assert(hash_ring_add_node(ring, (uint8_t*)slotA, strlen(slotA)) == HASH_RING_OK);
    assert(hash_ring_add_node(ring, (uint8_t*)slotB, strlen(slotB)) == HASH_RING_OK);

    node = hash_ring_find_node(ring, (uint8_t*)keyA, strlen(keyA));
    assert(node != NULL && node->nameLen == strlen(slotA) && memcmp(node->name, slotA, strlen(slotA)) == 0);

    node = hash_ring_find_node(ring, (uint8_t*)keyB, strlen(keyB));
    assert(node != NULL && node->nameLen == strlen(slotA) && memcmp(node->name, slotA, strlen(slotA)) == 0);

    node = hash_ring_find_node(ring, (uint8_t*)keyC, strlen(keyC));
    assert(node != NULL && node->nameLen == strlen(slotB) && memcmp(node->name, slotB, strlen(slotB)) == 0);

    hash_ring_free(ring);

## Benchmarks

The following benchmark was done on a MacBook Pro with the following specs:

* 2.8 Ghz Intel Core 2 Duo
* 8GB 1067 MHz DDR3
* Mac OS X 10.6.6

        ----------------------------------------------------
        bench: replicas = 1, nodes = 1, keys: 1000, ring size: 1
        ----------------------------------------------------
        generating and sorting 1 nodes...done
        generating keys...done
        stats: total = 0.04403s, avg/lookup: 0.44027us, min: 0.43374us, max: 0.73869us, ops/sec: 2272727
        ----------------------------------------------------
        bench: replicas = 1, nodes = 8, keys: 1000, ring size: 8
        ----------------------------------------------------
        generating and sorting 8 nodes...done
        generating keys...done
        stats: total = 0.04563s, avg/lookup: 0.45630us, min: 0.45339us, max: 0.50809us, ops/sec: 2192982
        ----------------------------------------------------
        bench: replicas = 1, nodes = 256, keys: 1000, ring size: 256
        ----------------------------------------------------
        generating and sorting 256 nodes...done
        generating keys...done
        stats: total = 0.04917s, avg/lookup: 0.49172us, min: 0.48684us, max: 0.54416us, ops/sec: 2036660
        ----------------------------------------------------
        bench: replicas = 8, nodes = 1, keys: 1000, ring size: 8
        ----------------------------------------------------
        generating and sorting 1 nodes...done
        generating keys...done
        stats: total = 0.04559s, avg/lookup: 0.45587us, min: 0.45276us, max: 0.46053us, ops/sec: 2197802
        ----------------------------------------------------
        bench: replicas = 8, nodes = 32, keys: 1000, ring size: 256
        ----------------------------------------------------
        generating and sorting 32 nodes...done
        generating keys...done
        stats: total = 0.04896s, avg/lookup: 0.48959us, min: 0.48601us, max: 0.54160us, ops/sec: 2044990
        ----------------------------------------------------
        bench: replicas = 8, nodes = 512, keys: 1000, ring size: 4096
        ----------------------------------------------------
        generating and sorting 512 nodes...done
        generating keys...done
        stats: total = 0.05401s, avg/lookup: 0.54006us, min: 0.53625us, max: 0.60199us, ops/sec: 1851852
        ----------------------------------------------------
        bench: replicas = 512, nodes = 8, keys: 1000, ring size: 4096
        ----------------------------------------------------
        generating and sorting 8 nodes...done
        generating keys...done
        stats: total = 0.05420s, avg/lookup: 0.54198us, min: 0.53301us, max: 1.03515us, ops/sec: 1848429
        ----------------------------------------------------
        bench: replicas = 512, nodes = 16, keys: 1000, ring size: 8192
        ----------------------------------------------------
        generating and sorting 16 nodes...done
        generating keys...done
        stats: total = 0.05576s, avg/lookup: 0.55763us, min: 0.55393us, max: 0.59292us, ops/sec: 1795332
        ----------------------------------------------------
        bench: replicas = 512, nodes = 32, keys: 100000, ring size: 16384
        ----------------------------------------------------
        generating and sorting 32 nodes...done
        generating keys...done
        stats: total = 5.73922s, avg/lookup: 0.57392us, min: 0.57195us, max: 0.58613us, ops/sec: 1745201
        ----------------------------------------------------
        bench: replicas = 512, nodes = 128, keys: 10000, ring size: 65536
        ----------------------------------------------------
        generating and sorting 128 nodes...done
        generating keys...done
        stats: total = 0.61953s, avg/lookup: 0.61953us, min: 0.61636us, max: 0.65255us, ops/sec: 1615509
        ----------------------------------------------------
        bench: replicas = 16, nodes = 1024, keys: 1000, ring size: 16384
        ----------------------------------------------------
        generating and sorting 1024 nodes...done
        generating keys...done
        stats: total = 0.05769s, avg/lookup: 0.57693us, min: 0.57307us, max: 0.63851us, ops/sec: 1736111

