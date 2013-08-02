/**
 * Copyright 2011 LiveProfile
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
 */

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>

#include "hash_ring.h"

#ifdef __APPLE__
#include <mach/mach_time.h>
#endif

static uint64_t opStartTime = 0;

void testAddMultipleTimes();
void testRemoveNode();
void testEmptyRingItemSearchReturnsNull();
void testEmptyRingSearchReturnsNull();
void testKnownNextHighestItemOnRing();
void testKnownSlotsOnRing();
void testKnownMultipleSlotsOnRing();
void testRingSorted();
void runBenchmark();
void testLibmemcachedCompat();

void startTiming();
uint64_t endTiming();

int main(int argc, char **argv) {
    testLibmemcachedCompat();
    testAddMultipleTimes();
    testRemoveNode();
    testRingSorted();
    testKnownNextHighestItemOnRing();
    testEmptyRingItemSearchReturnsNull();
    testEmptyRingSearchReturnsNull();
    testKnownSlotsOnRing();
    testKnownMultipleSlotsOnRing();
    
    runBenchmark();
    
    return 0;
}

void startTiming() {
#ifdef __APPLE__
    opStartTime = mach_absolute_time();
#endif
}

uint64_t endTiming() {
#ifdef __APPLE__
    uint64_t duration = mach_absolute_time() - opStartTime;
    
    /* Get the timebase info */
    mach_timebase_info_data_t info;
    mach_timebase_info(&info);
    
    /* Convert to nanoseconds */
    duration *= info.numer;
    duration /= info.denom;
    
    return duration;
#else
    return 0;
#endif
}

void fillRandomBytes(uint8_t *bytes, uint32_t num) {
    int x;
    for(x = 0; x < num; x++) {
        bytes[x] = (rand() % 25) + 97;
    }
}

void addNodes(hash_ring_t *ring, int numNodes) {
    int x;
    uint8_t nodeName[16];
    
    printf("generating and sorting %d nodes...", numNodes);
    fflush(stdout);
    for(x = 0; x < numNodes; x++) {
        fillRandomBytes(nodeName, sizeof(nodeName));
        hash_ring_add_node(ring, nodeName, sizeof(nodeName));
    }
    printf("done\n");
}

void generateKeys(uint8_t *keys, int numKeys, int keySize) {
    printf("generating keys...");
    fflush(stdout);
    
    int x;
    for(x = 0; x < numKeys; x++) {
        fillRandomBytes(&keys[x], keySize);
    }
    
    printf("done\n");
}

void runBench(HASH_FUNCTION hash_fn, int numReplicas, int numNodes, int numKeys, int keySize) {
    char *hash = NULL;
    if(hash_fn == HASH_FUNCTION_MD5) hash = "MD5";
    else if(hash_fn == HASH_FUNCTION_SHA1) hash = "SHA1";
    
    printf("----------------------------------------------------\n");
    printf("bench (%s): replicas = %d, nodes = %d, keys: %d, ring size: %d\n", hash, numReplicas, numNodes, numKeys, numReplicas * numNodes);
    printf("----------------------------------------------------\n");
    hash_ring_t *ring = hash_ring_create(numReplicas, hash_fn);
    
    addNodes(ring, numNodes);
    
    uint8_t *keys = (uint8_t*)malloc(keySize * numKeys);
    generateKeys(keys, numKeys, keySize);
    
    printf("running...\r");
    
    uint64_t min = 0;
    uint64_t max = 0;
    uint64_t total = 0;
    int times = 100;
    
    int x, y;
    for(y = 0; y < times; y++) {
        startTiming();
        for(x = 0; x < numKeys; x++) {
            assert(hash_ring_find_node(ring, keys + (keySize * x), keySize) != NULL);
        }
        uint64_t result = endTiming();
        if(result > max) max = result;
        if(min == 0 || result < min) min = result;
        total += result;
    }
    
    printf("stats: total = %.5fs, avg/lookup: %.5fus, min: %.5fus, max: %.5fus, ops/sec: %.0f\n", 
        (double)total / 1000000000,
        (((double)(total / numKeys)) / 1000) / times,
        (double)min / numKeys / 1000,
        (double)max / numKeys / 1000,
        1000000000 / ((double)(total / (numKeys * times))));
    
    free(keys);
    hash_ring_free(ring);
}

void runBenchmark() {
    printf("Starting benchmarks...\n");
    
    HASH_FUNCTION hash_fn = HASH_FUNCTION_SHA1;
    
    runBench(hash_fn, 1, 1, 1000, 16);
    runBench(hash_fn, 1, 8, 1000, 16);
    runBench(hash_fn, 1, 256, 1000, 16);
    runBench(hash_fn, 8, 1, 1000, 16);
    runBench(hash_fn, 8, 32, 1000, 16);
    runBench(hash_fn, 8, 512, 1000, 16);
    runBench(hash_fn, 512, 8, 1000, 16);
    runBench(hash_fn, 512, 16, 1000, 16);
    runBench(hash_fn, 512, 32, 100000, 16);
    runBench(hash_fn, 512, 128, 10000, 16);
    runBench(hash_fn, 16, 1024, 1000, 16);
    
    hash_fn = HASH_FUNCTION_MD5;
    
    runBench(hash_fn, 1, 1, 1000, 16);
    runBench(hash_fn, 1, 8, 1000, 16);
    runBench(hash_fn, 1, 256, 1000, 16);
    runBench(hash_fn, 8, 1, 1000, 16);
    runBench(hash_fn, 8, 32, 1000, 16);
    runBench(hash_fn, 8, 512, 1000, 16);
    runBench(hash_fn, 512, 8, 1000, 16);
    runBench(hash_fn, 512, 16, 1000, 16);
    runBench(hash_fn, 512, 32, 100000, 16);
    runBench(hash_fn, 512, 128, 10000, 16);
    runBench(hash_fn, 16, 1024, 1000, 16);
}

void testRingSorting(int num) {
    printf("Test that the ring is sorted [%d item(s)]...\n", num);
    hash_ring_t *ring = hash_ring_create(num, HASH_FUNCTION_SHA1);
    char *slotA = "slotA";
    
    assert(hash_ring_add_node(ring, (uint8_t*)slotA, strlen(slotA)) == HASH_RING_OK);
    int x;
    uint64_t cur = 0;
    for(x = 0; x < ring->numItems; x++) {
        assert(ring->items[x]->number > cur);
        cur = ring->items[x]->number;
    }
    
    hash_ring_free(ring);
}

void testRingSorted() {
    testRingSorting(1);
    testRingSorting(2);
    testRingSorting(3);
    testRingSorting(5);
    testRingSorting(64);
    testRingSorting(256);
    testRingSorting(1024);
    testRingSorting(10000);
    testRingSorting(100000);
    testRingSorting(1000000);
}

void testEmptyRingItemSearchReturnsNull() {
    printf("Test empty ring search returns null item...\n");
    hash_ring_t *ring = hash_ring_create(8, HASH_FUNCTION_SHA1);
    
    assert(hash_ring_find_next_highest_item(ring, 0) == NULL);
    
    hash_ring_free(ring);
}

void testEmptyRingSearchReturnsNull() {
    printf("Test empty ring search returns null node...\n");
    hash_ring_t *ring = hash_ring_create(8, HASH_FUNCTION_SHA1);
    char *key = "key";
    assert(hash_ring_find_node(ring, (uint8_t*)key, strlen(key)) == NULL);
    
    hash_ring_free(ring);
}

void testKnownSlotsOnRing() {
    printf("Test getting known nodes on ring...\n");
    hash_ring_t *ring = hash_ring_create(8, HASH_FUNCTION_SHA1);
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
}

void testKnownMultipleSlotsOnRing() {
    printf("\n\nTest getting multiple known nodes on ring...\n\n");

    hash_ring_t *ring = hash_ring_create(8, HASH_FUNCTION_SHA1);
    char *slotA = "slotA";
    char *slotB = "slotB";
    char *slotC = "slotC";

    // hashes to a low number
    char *keyA = "keyA";
    // hashes to high number
    char *keyB = "keyB*_*_*_";

    int x;
    hash_ring_node_t *nodes[3];

    assert(hash_ring_add_node(ring, (uint8_t*)slotA, strlen(slotA)) == HASH_RING_OK);
    assert(hash_ring_add_node(ring, (uint8_t*)slotB, strlen(slotB)) == HASH_RING_OK);

    x = hash_ring_find_nodes(ring, (uint8_t*)keyA, strlen(keyA), nodes, 3);
    assert(
        x == 2 &&
        nodes[0] != NULL &&
            nodes[0]->nameLen == strlen(slotA) &&
            memcmp(nodes[0]->name, slotA, strlen(slotA)) == 0 &&
        nodes[1] != NULL &&
            nodes[1]->nameLen == strlen(slotB) &&
            memcmp(nodes[1]->name, slotB, strlen(slotB)) == 0);

    x = hash_ring_find_nodes(ring, (uint8_t*)keyB, strlen(keyB), nodes, 3);
    assert(
        x == 2 &&
        nodes[0] != NULL &&
            nodes[0]->nameLen == strlen(slotB) &&
            memcmp(nodes[0]->name, slotB, strlen(slotB)) == 0 &&
        nodes[1] != NULL &&
            nodes[1]->nameLen == strlen(slotA) &&
            memcmp(nodes[1]->name, slotA, strlen(slotA)) == 0);

    assert(hash_ring_add_node(ring, (uint8_t*)slotC, strlen(slotC)) == HASH_RING_OK);

    x = hash_ring_find_nodes(ring, (uint8_t*)keyA, strlen(keyA), nodes, 3);
    assert(
        x == 3 &&
        nodes[0] != NULL &&
            nodes[0]->nameLen == strlen(slotC) &&
            memcmp(nodes[0]->name, slotC, strlen(slotC)) == 0 &&
        nodes[1] != NULL &&
            nodes[1]->nameLen == strlen(slotA) &&
            memcmp(nodes[1]->name, slotA, strlen(slotA)) == 0 &&
        nodes[2] != NULL &&
            nodes[2]->nameLen == strlen(slotB) &&
            memcmp(nodes[2]->name, slotB, strlen(slotB)) == 0);

    x = hash_ring_find_nodes(ring, (uint8_t*)keyB, strlen(keyB), nodes, 3);
    assert(
        x == 3 &&
        nodes[0] != NULL &&
            nodes[0]->nameLen == strlen(slotC) &&
            memcmp(nodes[0]->name, slotC, strlen(slotC)) == 0 &&
        nodes[1] != NULL &&
            nodes[1]->nameLen == strlen(slotB) &&
            memcmp(nodes[1]->name, slotB, strlen(slotB)) == 0 &&
        nodes[2] != NULL &&
            nodes[2]->nameLen == strlen(slotA) &&
            memcmp(nodes[2]->name, slotA, strlen(slotA)) == 0);

    hash_ring_free(ring);
}

void testKnownNextHighestItemOnRing() {
    printf("Test getting next highest item on ring...\n");
    hash_ring_t *ring = hash_ring_create(8, HASH_FUNCTION_SHA1);
    char *slotA = "slotA";
    char *slotB = "slotB";
    
    assert(hash_ring_add_node(ring, (uint8_t*)slotA, strlen(slotA)) == HASH_RING_OK);
    assert(hash_ring_add_node(ring, (uint8_t*)slotB, strlen(slotB)) == HASH_RING_OK);
    
    // next highest for first item should yield the second
    assert(hash_ring_find_next_highest_item(ring, 2351641940735260693u)->number == 2584980261350711786u);
    
    // number less than the first should yield the first
    assert(hash_ring_find_next_highest_item(ring, 2351641940735260692u)->number == 2351641940735260693u);
    
    // number in the middle should yield the next
    assert(hash_ring_find_next_highest_item(ring, 5908063426886290069u)->number == 6065789416862870789u);
    
    // number equal to the last should wrap around to the first
    assert(hash_ring_find_next_highest_item(ring, 17675051572751928939u)->number == 2351641940735260693u);
    
    hash_ring_free(ring);
}

void testRemoveNode() {
    printf("Test removing a node...\n");
    hash_ring_t *ring = hash_ring_create(1, HASH_FUNCTION_SHA1);
    hash_ring_node_t *node;
    char *mynode = "mynode";
    char *mynode1 = "mynode1";
    char *mynode2 = "mynode2";
    char *mykey = "mykey";
    
    
    assert(hash_ring_add_node(ring, (uint8_t*)mynode, strlen(mynode)) == HASH_RING_OK);
    assert(hash_ring_add_node(ring, (uint8_t*)mynode1, strlen(mynode2)) == HASH_RING_OK);
    assert(hash_ring_add_node(ring, (uint8_t*)mynode2, strlen(mynode2)) == HASH_RING_OK);
    assert(ring->numNodes == 3);
    
    node = hash_ring_find_node(ring, (uint8_t*)mykey, strlen(mykey));
    assert(node != NULL && node->nameLen == strlen(mynode) && memcmp(mynode, node->name, node->nameLen) == 0);
    
    assert(hash_ring_remove_node(ring, (uint8_t*)mynode, strlen(mynode)) == HASH_RING_OK);
    assert(ring->numNodes == 2);
    
    assert(hash_ring_get_node(ring, (uint8_t*)mynode, strlen(mynode)) == NULL);
    
    // remove node1, and try to search for a key that went to it before, and verify it goes to node2
    assert(hash_ring_remove_node(ring, (uint8_t*)mynode1, strlen(mynode1)) == HASH_RING_OK);
    assert(ring->numNodes == 1);
    
    node = hash_ring_find_node(ring, (uint8_t*)mykey, strlen(mykey));
    assert(node != NULL && node->nameLen == strlen(mynode2) && memcmp(mynode2, node->name, node->nameLen) == 0);
    
    hash_ring_free(ring);
}

void testAddMultipleTimes() {
    printf("Test adding a node multiple times...\n");
    hash_ring_t *ring = hash_ring_create(1, HASH_FUNCTION_SHA1);
    assert(ring != NULL);
    char *mynode = "mynode";
    hash_ring_add_node(ring, (uint8_t*)mynode, strlen(mynode));

    assert(ring->numNodes == 1);
    
    assert(hash_ring_add_node(ring, (uint8_t*)mynode, strlen(mynode)) == HASH_RING_ERR);
    assert(ring->numNodes == 1);
    
    hash_ring_free(ring);
}

void testLibmemcachedCompat() {
    printf("Testing libmemcached compatibility mode..\n");

    // make sure the mode can't be set to libmemcached if the function is not md5
    hash_ring_t *ring = hash_ring_create(1, HASH_FUNCTION_SHA1);
    assert(hash_ring_set_mode(ring, HASH_RING_MODE_LIBMEMCACHED_COMPAT) == HASH_RING_ERR);
    hash_ring_free(ring);

    ring = hash_ring_create(1, HASH_FUNCTION_MD5);
    assert(hash_ring_set_mode(ring, HASH_RING_MODE_LIBMEMCACHED_COMPAT) == HASH_RING_OK);

    /**
     * TODO - Add tests to make sure libmemcached compat hashes correctly.
     */

    hash_ring_free(ring);
}
