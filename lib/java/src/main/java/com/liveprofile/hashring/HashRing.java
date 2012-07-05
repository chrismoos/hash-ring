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

package com.liveprofile.hashring;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Structure;
import com.sun.jna.Pointer;
import com.sun.jna.NativeLong;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;

/**
 * This class provides access to the hash_ring C library.
 *
 * <p>
 * Make sure that the system
 * property <em>jna.library.path</em> points to the location of <strong>libhashring.so</strong> or
 * <strong>libhashring.dylib</strong>.
 * <br/>
 * <pre> {@code
 * -Djna.library.path=/usr/local/lib
 * }</pre>
 * </p>
 * 
 * @author Chris Moos
 */
public class HashRing {    
    private Pointer ringPointer;
    
    /**
     * Creates a hash ring.
     *
     * @param numReplicas The number of times each node should be placed on the node
     * @param hashFunction The hash function use on the ring
     * @see HashFunction
     */
    public HashRing(int numReplicas, HashFunction hashFunction) throws HashRingException {
        ringPointer = CLibrary.INSTANCE.hash_ring_create(numReplicas, hashFunction.type());
        if(ringPointer == null) {
            throw new HashRingException("Failed to create ring");
        }
    }
    
    /**
     * Adds a node to the ring.
     *
     * @return true if the node was added, or false if it wasn't added.
     */
    public boolean addNode(String node) throws HashRingException {
        if(CLibrary.INSTANCE.hash_ring_add_node(ringPointer, node, node.length()) != 0) {
            return false;
        }
        else {
            return true;
        }
    }
    
    /**
     * Removes a node from the ring.
     *
     * @return true if the node was removed, or false if it wasn't removed.
     */
    public boolean removeNode(String node) throws HashRingException {
        if(CLibrary.INSTANCE.hash_ring_remove_node(ringPointer, node, node.length()) != 0) {
            return false;
        }
        else {
            return true;
        }
    }
    
    /**
     * Finds the node on the ring for the given key.
     *
     * @param key The key to search the ring with
     * @return The node that comes after the key on the ring
     *
     * @throws HashRingException if the node search couldn't be completed
     */
    public String findNode(String key) throws HashRingException {
        NodeStructure node = CLibrary.INSTANCE.hash_ring_find_node(ringPointer, key, key.length());
        if(node == null) {
            throw new HashRingException("Failed to find node");
        }
        try {
            return new String(node.name.getByteArray(0, node.nameLength), "UTF-8");
        }
        catch(UnsupportedEncodingException uee) {
            throw new HashRingException("Unable to get UTF-8 representation of node", uee);
        }
    }
    
    /**
     * Finds the next highest item on the ring.
     *
     * This method is currently limited becuase it only supports positive numbers up to
     * 2 ^ 63 - 1. In C the unsigned int supports 2 ^ 64 - 1.
     *
     * @param number The number to start the search at.
     * @return The next highest item
     * @throws HashRingException If the next highest item couldn't be located
     *
     */
    public long findNextHighestItem(long number) throws HashRingException {
        ItemStructure item = CLibrary.INSTANCE.hash_ring_find_next_highest_item(ringPointer, number);
        if(item == null) {
            throw new HashRingException("Failed to find next highest item");
        }
        
        return item.number;
    }
    
    /**
     * Prints the current ring to stdout.
     */
    public void print() {
        CLibrary.INSTANCE.hash_ring_print(ringPointer);
    }
    
    /**
     * Cleans up the memory associated with the hash ring.
     */
    protected void finalize() {
        CLibrary.INSTANCE.hash_ring_free(ringPointer);
    }
    
    /**
     * The supported Hash functions.
     */
    public enum HashFunction {
        SHA1((char)1),
        MD5((char)2);
        
        private final char type;
        HashFunction(char type) {
            this.type = type;
        }
        
        char type() {
            return type;
        }
    };
    
    /**
     * This class defines the hash_ring_node_t structure.
     */
    public static class NodeStructure extends Structure {
        public Pointer name;
        public int nameLength;
    }
    
    /**
     * This class defines the hash_ring_item_t structure.
     */
    public static class ItemStructure extends Structure {
        public Pointer node;
        public long number;
    }
    
    private interface CLibrary extends Library {
        CLibrary INSTANCE = (CLibrary)Native.loadLibrary("hashring", CLibrary.class);
            
        Pointer hash_ring_create(long numReplicas, char hash_fn);
        void hash_ring_free(Pointer ring);
        int hash_ring_add_node(Pointer ring, String node, int nodeLength);
        int hash_ring_remove_node(Pointer ring, String node, int nodeLength);
        ItemStructure hash_ring_find_next_highest_item(Pointer ring, long number);
        NodeStructure hash_ring_find_node(Pointer ring, String key, int keyLength);
        void hash_ring_print(Pointer ring);
    }
}
