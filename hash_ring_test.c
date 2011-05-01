#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>

#include "hash_ring.h"

#include <mach/mach_time.h>

static uint64_t opStartTime = 0;

void testAddMultipleTimes();
void testRemoveNode();
void testEmptyRingItemSearchReturnsNull();
void testEmptyRingSearchReturnsNull();
void testKnownNextHighestItemOnRing();
void testKnownSlotsOnRing();
void testRingSorted();
void runBenchmark();

void startTiming();
uint64_t endTiming();

int main(int argc, char **argv) {
    /* Get the timebase info */
    mach_timebase_info_data_t info;
    mach_timebase_info(&info);

    uint64_t start = mach_absolute_time();
    
    //hash_ring_node_t *n = hash_ring_find_node(ring, (uint8_t*)keySearch, strlen(keySearch));
    
    
    uint64_t duration = mach_absolute_time() - start;

    /* Convert to nanoseconds */
    duration *= info.numer;
    duration /= info.denom;
    
    /* Log the time */
    /*printf("My amazing code took %lld microseconds!", duration / 1000);
    assert(n != NULL);
    
    printf("Cleaning up hash ring..\n");
    hash_ring_free(ring);
    
    printf("Running tests...\n");*/
    
    testAddMultipleTimes();
    testRemoveNode();
    testRingSorted();
    testKnownNextHighestItemOnRing();
    testEmptyRingItemSearchReturnsNull();
    testEmptyRingSearchReturnsNull();
    testKnownSlotsOnRing();
    
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

void runBench(int numReplicas, int numNodes, int numKeys, int keySize) {
    printf("----------------------------------------------------\n");
    printf("bench: replicas = %d, nodes = %d, keys: %d\n", numReplicas, numNodes, numKeys);
    printf("----------------------------------------------------\n");
    hash_ring_t *ring = hash_ring_create(numReplicas);
    
    addNodes(ring, numNodes);
    
    uint8_t *keys = (uint8_t*)malloc(keySize * numKeys);
    generateKeys(keys, numKeys, keySize);
    
    printf("running...\r");
    
    int x;
    startTiming();
    for(x = 0; x < numKeys; x++) {
        hash_ring_get_node(ring, &keys[x], keySize);
    }
    uint64_t result = endTiming();
    printf("stats: total = %.5fs, avg/lookup: %.5fus, ops/sec: %.0f\n", 
        (double)result / 1000000000,
        ((double)(result / numKeys)) / 1000,
        1000000000 / ((double)(result / numKeys)));
    
    
    //hash_ring_print(ring);
    
    hash_ring_free(ring);
}

void runBenchmark() {
    printf("Starting benchmarks...\n");
    
    runBench(1, 1, 1000, 16);
    runBench(1, 8, 1000, 16);
    runBench(1, 256, 1000, 16);
    runBench(8, 1, 1000, 16);
    runBench(8, 32, 1000, 16);
    runBench(8, 512, 1000, 16);
    runBench(512, 8, 1000, 16);
    runBench(512, 16, 1000, 16);
    runBench(512, 32, 1000, 16);
}

void testRingSorted() {
    printf("Test that the ring is sorted...\n");
    hash_ring_t *ring = hash_ring_create(64);
    char *slotA = "slotA";
    char *slotB = "slotB";
    
    assert(hash_ring_add_node(ring, (uint8_t*)slotA, strlen(slotA)) == HASH_RING_OK);
    assert(hash_ring_add_node(ring, (uint8_t*)slotB, strlen(slotB)) == HASH_RING_OK);
    
    int x;
    uint64_t cur = 0;
    for(x = 0; x < ring->numItems; x++) {
        assert(ring->items[x]->number > cur);
        cur = ring->items[x]->number;
    }
}

void testEmptyRingItemSearchReturnsNull() {
    printf("Test empty ring search returns null item...\n");
    hash_ring_t *ring = hash_ring_create(8);
    
    assert(hash_ring_find_next_highest_item(ring, 0) == NULL);
    
    hash_ring_free(ring);
}

void testEmptyRingSearchReturnsNull() {
    printf("Test empty ring search returns null node...\n");
    hash_ring_t *ring = hash_ring_create(8);
    char *key = "key";
    assert(hash_ring_find_node(ring, (uint8_t*)key, strlen(key)) == NULL);
    
    hash_ring_free(ring);
}

void testKnownSlotsOnRing() {
    printf("Test getting known nodes on ring...\n");
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
    assert(node != NULL && node->keyLen == strlen(slotA) && memcmp(node->key, slotA, strlen(slotA)) == 0);
    
    node = hash_ring_find_node(ring, (uint8_t*)keyB, strlen(keyB));
    assert(node != NULL && node->keyLen == strlen(slotA) && memcmp(node->key, slotA, strlen(slotA)) == 0);
    
    node = hash_ring_find_node(ring, (uint8_t*)keyC, strlen(keyC));
    assert(node != NULL && node->keyLen == strlen(slotB) && memcmp(node->key, slotB, strlen(slotB)) == 0);

    hash_ring_free(ring);
}

void testKnownNextHighestItemOnRing() {
    printf("Test getting next highest item on ring...\n");
    hash_ring_t *ring = hash_ring_create(8);
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
    hash_ring_t *ring = hash_ring_create(1);
    char *mynode = "mynode";
    char *mynode1 = "mynode1";
    char *mynode2 = "mynode2";
    
    assert(hash_ring_add_node(ring, (uint8_t*)mynode, strlen(mynode)) == HASH_RING_OK);
    assert(hash_ring_add_node(ring, (uint8_t*)mynode1, strlen(mynode2)) == HASH_RING_OK);
    assert(hash_ring_add_node(ring, (uint8_t*)mynode2, strlen(mynode2)) == HASH_RING_OK);
    assert(ring->numNodes == 3);
    
    assert(hash_ring_remove_node(ring, (uint8_t*)mynode, strlen(mynode)) == HASH_RING_OK);
    assert(ring->numNodes == 2);
    
    assert(hash_ring_get_node(ring, (uint8_t*)mynode, strlen(mynode)) == NULL);
    
    hash_ring_free(ring);
}

void testAddMultipleTimes() {
    printf("Test adding a node multiple times...\n");
    hash_ring_t *ring = hash_ring_create(1);
    char *mynode = "mynode";
    
    hash_ring_add_node(ring, (uint8_t*)mynode, strlen(mynode));
    assert(ring->numNodes == 1);
    
    assert(hash_ring_add_node(ring, (uint8_t*)mynode, strlen(mynode)) == HASH_RING_ERR);
    assert(ring->numNodes == 1);
    
    hash_ring_free(ring);
}