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