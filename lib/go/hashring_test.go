package hashring

import (
	"testing"

	"bytes"
)

func TestFindNode(t *testing.T) {
	ring := New(8, SHA1)
	defer ring.Free()

	ring.Add([]byte("slotA"))
	ring.Add([]byte("slotB"))

	var node string
	node = string(ring.FindNode([]byte("keyA")))
	if node != "slotA" {
		t.Fatal(node)
	}
	node = string(ring.FindNode([]byte("keyBBBB")))
	if node != "slotA" {
		t.Fatal(node)
	}
	node = string(ring.FindNode([]byte("keyB_")))
	if node != "slotB" {
		t.Fatal(node)
	}
}

func TestFindNodes(t *testing.T) {
	ring := New(8, SHA1)
	defer ring.Free()

	ring.Add([]byte("slotA"))
	ring.Add([]byte("slotB"))

	var nodes [][]byte

	nodes = ring.FindNodes([]byte("keyA"), 3)
	if len(nodes) != 2 ||
		bytes.Compare(nodes[0], []byte("slotA")) != 0 ||
		bytes.Compare(nodes[1], []byte("slotB")) != 0 {
		t.Fatal(nodes)
	}

	nodes = ring.FindNodes([]byte("keyB*_*_*_"), 3)
	if len(nodes) != 2 ||
		bytes.Compare(nodes[0], []byte("slotB")) != 0 ||
		bytes.Compare(nodes[1], []byte("slotA")) != 0 {
		t.Fatal(nodes)
	}

	ring.Add([]byte("slotC"))

	nodes = ring.FindNodes([]byte("keyA"), 3)
	if len(nodes) != 3 ||
		bytes.Compare(nodes[0], []byte("slotC")) != 0 ||
		bytes.Compare(nodes[1], []byte("slotA")) != 0 ||
		bytes.Compare(nodes[2], []byte("slotB")) != 0 {
		t.Fatal(nodes)
	}

	nodes = ring.FindNodes([]byte("keyB*_*_*_"), 3)
	if len(nodes) != 3 ||
		bytes.Compare(nodes[0], []byte("slotC")) != 0 ||
		bytes.Compare(nodes[1], []byte("slotB")) != 0 ||
		bytes.Compare(nodes[2], []byte("slotA")) != 0 {
		t.Fatal(nodes)
	}

	nodes = ring.FindNodes([]byte("keyA"), 2)
	if len(nodes) != 2 ||
		bytes.Compare(nodes[0], []byte("slotC")) != 0 ||
		bytes.Compare(nodes[1], []byte("slotA")) != 0 {
		t.Fatal(nodes)
	}

	nodes = ring.FindNodes([]byte("keyB*_*_*_"), 2)
	if len(nodes) != 2 ||
		bytes.Compare(nodes[0], []byte("slotC")) != 0 ||
		bytes.Compare(nodes[1], []byte("slotB")) != 0 {
		t.Fatal(nodes)
	}
}
