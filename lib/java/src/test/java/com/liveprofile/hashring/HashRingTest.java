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

import org.junit.*;
import static org.junit.Assert.*;
import java.math.BigInteger;

public class HashRingTest {
    @Test
    public void testCreateRing() throws HashRingException {
        HashRing ring = new HashRing(1, HashRing.HashFunction.MD5);
    }
    
    @Test
    public void testKnownSlotsOnRing() throws HashRingException {
        HashRing ring = new HashRing(8, HashRing.HashFunction.SHA1);
        assertTrue(ring.addNode("slotA"));
        assertTrue(ring.addNode("slotB"));
        
        assertEquals("slotA", ring.findNode("keyA"));
        assertEquals("slotA", ring.findNode("keyBBBB"));
        assertEquals("slotB", ring.findNode("keyB_"));
    }
    
    @Test
    public void testAddMultipleTimes() throws HashRingException {
        HashRing ring = new HashRing(8, HashRing.HashFunction.SHA1);
        assertTrue(ring.addNode("slotB"));
        assertFalse(ring.addNode("slotB"));
        assertTrue(ring.removeNode("slotB"));
        assertTrue(ring.addNode("slotA"));
    }
    
    @Test
    public void testNextHighestItem() throws HashRingException {
        HashRing ring = new HashRing(8, HashRing.HashFunction.SHA1);
        assertTrue(ring.addNode("slotA"));
        assertTrue(ring.addNode("slotB"));
        assertEquals(2584980261350711786L, ring.findNextHighestItem(2351641940735260693L));
    }
    
    @Test
    public void testAddRemoveNode() throws HashRingException {
        HashRing ring = new HashRing(8, HashRing.HashFunction.SHA1);
        ring.addNode("slotA");
        assertTrue(ring.removeNode("slotA"));
        assertFalse(ring.removeNode("slotA"));
    }
}