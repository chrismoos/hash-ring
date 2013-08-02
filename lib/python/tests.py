import unittest
import sys
from chashring import set_up_hash_ring_ctypes, HashRing, HashFunction

class CHashRingTests(unittest.TestCase):

    def test_create(self):
        new_hash = HashRing(["redis1", "redis2"])
        self.assert_(new_hash)
        new_hash.free()

    def test_find_node(self):
        """ Based on hash_ring_test's test """
        new_hash = HashRing(["slotA", "slotB"], num_replicas = 8, hash_fn = HashFunction.SHA1 )
        keyA = 'keyA'
        server = new_hash.find_node(keyA)
        self.assertEquals(server, 'slotA')
        keyBBBB = 'keyBBBB'
        node = new_hash.find_node(keyBBBB)
        self.assertEquals(node, 'slotA')
        key = 'keyB_'
        node = new_hash.find_node(key)
        self.assertEquals(node, 'slotB')
        new_hash.free()

    def test_find_nodes(self):
        """ Based on hash_ring_test's test """
        new_hash = HashRing(
            ["slotA", "slotB"],
            num_replicas = 8,
            hash_fn = HashFunction.SHA1)
        keyA = 'keyA'
        keyB = 'keyB*_*_*_'
        servers = new_hash.find_nodes(keyA, 3)
        self.assertEquals(servers, ['slotA', 'slotB'])
        servers = new_hash.find_nodes(keyB, 3)
        self.assertEquals(servers, ['slotB', 'slotA'])
        new_hash.add_node('slotC')
        servers = new_hash.find_nodes(keyA, 3)
        self.assertEquals(servers, ['slotC', 'slotA', 'slotB'])
        servers = new_hash.find_nodes(keyB, 3)
        self.assertEquals(servers, ['slotC', 'slotB', 'slotA'])
        servers = new_hash.find_nodes(keyA, 2)
        self.assertEquals(servers, ['slotC', 'slotA'])
        servers = new_hash.find_nodes(keyB, 2)
        self.assertEquals(servers, ['slotC', 'slotB'])
        new_hash.free()

    def test_add_node(self):
        hash = HashRing()
        self.assertFalse(hash.check_node_in_ring('redis1'))
        hash.add_node('redis1')
        self.assertTrue(hash.check_node_in_ring('redis1'))
        hash.free()

    def test_remove_node(self):
        hash = HashRing()
        hash.add_node('redis1')
        self.assertTrue(hash.check_node_in_ring('redis1'))
        hash.remove_node('redis1')
        self.assertFalse(hash.check_node_in_ring('redis1'))
        hash.free()



if __name__ == '__main__':
    unittest.main()
