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

#ifndef HASH_RING_H
#define HASH_RING_H

#include <stdint.h>

#define HASH_RING_OK 0
#define HASH_RING_ERR 1

#define HASH_FUNCTION_SHA1 1
#define HASH_FUNCTION_MD5 2

#define HASH_RING_DEBUG 1

/**
 * This will do the hashing in the standard hash-ring way. See the README
 */
#define HASH_RING_MODE_NORMAL 1

/**
 * Hashing of nodes is the same as libmemcached.
 * The hash function must be HASH_FUNCTION_MD5.
 *
 * @see https://github.com/RJ/ketama/blob/master/libketama/ketama.c#L433
 */
#define HASH_RING_MODE_LIBMEMCACHED_COMPAT 2

typedef uint8_t HASH_MODE;

typedef struct ll_t {
    void *data;
    struct ll_t *next;
} ll_t;

typedef uint8_t HASH_FUNCTION;

/**
 * All nodes in the ring must have a unique name.
 *
 * The 'name' bytes below are hashed to place the node in the ring.
 * The name is typically human readable and should be consistent on all clients using
 * the hash ring.
 */
typedef struct hash_ring_node_t {
    uint8_t *name;
    uint32_t nameLen;
} hash_ring_node_t;

/**
 * Nodes have many items, each item has a number derived from the node's name.
 */
typedef struct hash_ring_item_t {
    hash_ring_node_t *node;
    
    /* number is dervied from a hash of the node's key */
    uint64_t number;
} hash_ring_item_t;

/**
 * This structure contains an array with the ring's items, as well as
 * a list of nodes. A node appears in the ring numReplicas times.
 */
typedef struct hash_ring_t {
    uint32_t numReplicas;
    
    /* List of nodes in the ring */
    ll_t *nodes;
    
    /* The number of nodes in the ring */
    uint32_t numNodes;
    
    /**
     * Array of items in the ring 
     * This array is sorted ascending
     */
    hash_ring_item_t **items;
    
    /* The number of items in the ring */
    uint32_t numItems;
    
    /* The hash function to use for this ring */
    HASH_FUNCTION hash_fn;

    /* The mode for hashing */
    HASH_MODE mode;
} hash_ring_t;

/**
 * Creates a new hash ring. 
 * The numReplicas parameter must be specified. It should be >= 1.
 * numReplicas is used to place a node on the ring multiple times to distribute it
 * more evenly. Increasing numReplicas improves distribution, but also increases memory by
 * (numReplicas * N).
 *
 * @param[in] numReplicas The number of replicas
 * @param[in] hash_fn The hash function to use. HASH_FUNCTION_SHA1 or HASH_FUNCTION_MD5
 *
 * @returns a new hash ring or NULL if it couldn't be created.
 */
hash_ring_t *hash_ring_create(uint32_t numReplicas, HASH_FUNCTION hash_fn);


/**
 * Frees the hash ring and all memory associated with it.
 */
void hash_ring_free(hash_ring_t *ring);


/**
 * Adds a node into the ring. The node is specified by passing an opaque
 * array of bytes in the name parameter. The name should be used consistently for clients using the ring.
 *
 * This function is idempotent, a node will only ever exist in the ring once, regardless
 * of how many times it is added.
 *
 * @returns HASH_RING_OK if the node was added, HASH_RING_ERR if an error occurred.
 */
int hash_ring_add_node(hash_ring_t *ring, uint8_t *name, uint32_t nameLen);


/**
 * Gets the node specified by name from the ring.
 *
 * @returns the node or NULL if it cannot be found.
 */
hash_ring_node_t *hash_ring_get_node(hash_ring_t *ring, uint8_t *name, uint32_t nameLen);


/**
 * Finds the node by hashing the given key and searching the ring.
 *
 * This is the function that is typically called the most from your clients. Once the ring is created, it is
 * usually not modified (unless you want to deal with re-hashing -- which isn't bad necessarily bad, i.e memcached).
 */
hash_ring_node_t *hash_ring_find_node(hash_ring_t *ring, uint8_t *key, uint32_t keyLen);

/**
 * Finds the set of num nodes by hashing the given key and searching the ring.
 * Returns the number of nodes found, or -1 if there is an error
 */
int hash_ring_find_nodes(hash_ring_t *ring, uint8_t *key, uint32_t keyLen, hash_ring_node_t *nodes[], uint32_t num);

/**
 * Find the next highest item for the given num.
 * This function is invoked by hash_ring_find_node to locate a key on the ring. If you want to do your own hashing on 
 * the keys, you might call this function, but probably not.
 */
hash_ring_item_t *hash_ring_find_next_highest_item(hash_ring_t *ring, uint64_t num);

/**
 * Removes a node from the ring. 
 * 
 * @returns HASH_RING_OK if the node was removed, or HASH_RING_ERR if it does not exist.
 */
int hash_ring_remove_node(hash_ring_t *ring, uint8_t *name, uint32_t nameLen);

/**
 * Print the hash ring to stdout.
 */
void hash_ring_print(hash_ring_t *ring);

/**
 * Sets the mode for hashing.
 *
 * This should be set after creating a ring and before adding nodes.
 *
 * If mode is set to HASH_RING_MODE_LIBMEMCACHED_COMPAT then the hash function must be HASH_FUNCTION_MD5 or this
 * call will fail.
 *
 * @param ring The ring to set the mode on.
 * @param mode The mode to set the hashing to.
 *
 * @returns HASH_RING_OK if the mode was set.
 */
int hash_ring_set_mode(hash_ring_t *ring, HASH_MODE mode);

#endif
