// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

import (
	"bytes"
	"sort"
	"sync/atomic"
)

type Node8[T any] struct {
	id           uint64
	partialLen   uint32
	numChildren  uint8
	partial      []byte
	keys         [8]byte
	children     [8]Node[T]
	mutateCh     atomic.Pointer[chan struct{}]
	leaf         *NodeLeaf[T]
	refCount     int64
	lazyRefCount int64
}

func (n *Node8[T]) getId() uint64 {
	return n.id
}

func (n *Node8[T]) setId(id uint64) {
	n.id = id
}

func (n *Node8[T]) getPartialLen() uint32 {
	return n.partialLen
}

func (n *Node8[T]) setPartialLen(partialLen uint32) {
	n.partialLen = partialLen
}

func (n *Node8[T]) getArtNodeType() nodeType {
	return node8
}

func (n *Node8[T]) getNumChildren() uint8 {
	return n.numChildren
}

func (n *Node8[T]) setNumChildren(numChildren uint8) {
	n.numChildren = numChildren
}

func (n *Node8[T]) getPartial() []byte {
	return n.partial
}

func (n *Node8[T]) setPartial(partial []byte) {
	n.partial = partial
}

func (n *Node8[T]) isLeaf() bool {
	if n.numChildren == 0 && n.getNodeLeaf() != nil {
		return true
	}
	return false
}

// Iterator is used to return an Iterator at
// the given node to walk the tree
func (n *Node8[T]) Iterator() *Iterator[T] {
	return &Iterator[T]{
		node: n,
	}
}

func (n *Node8[T]) PathIterator(path []byte) *PathIterator[T] {
	nodeT := Node[T](n)
	return &PathIterator[T]{node: &nodeT,
		path:  getTreeKey(path),
		stack: []Node[T]{nodeT},
	}
}

func (n *Node8[T]) matchPrefix(prefix []byte) bool {
	return bytes.HasPrefix(n.partial, prefix)
}

func (n *Node8[T]) getChild(index int) Node[T] {
	return n.children[index]
}

func (n *Node8[T]) clone(keepWatch, deep bool) Node[T] {
	n.processRefCount()
	newNode := &Node8[T]{
		partialLen:  n.getPartialLen(),
		numChildren: n.getNumChildren(),
		refCount:    n.getRefCount(),
	}
	newNode.setId(n.getId())
	if keepWatch {
		newNode.setMutateCh(n.getMutateCh())
	}
	if deep {
		if n.getNodeLeaf() != nil {
			newNode.setNodeLeaf(n.getNodeLeaf().clone(true, true).(*NodeLeaf[T]))
		}
	} else {
		newNode.setNodeLeaf(n.getNodeLeaf())
	}
	newPartial := make([]byte, maxPrefixLen)
	copy(newPartial, n.partial)
	newNode.setPartial(newPartial)
	copy(newNode.keys[:], n.keys[:])
	if deep {
		cpy := make([]Node[T], len(n.children))
		copy(cpy, n.children[:])
		for i := 0; i < 8; i++ {
			if cpy[i] == nil {
				continue
			}
			newNode.setChild(i, cpy[i].clone(keepWatch, true))
		}
	} else {
		cpy := make([]Node[T], len(n.children))
		copy(cpy, n.children[:])
		for i := 0; i < 8; i++ {
			newNode.setChild(i, cpy[i])
		}
	}
	return newNode
}

func (n *Node8[T]) getKeyLen() uint32 {
	return 0
}

func (n *Node8[T]) setKeyLen(keyLen uint32) {

}

func (n *Node8[T]) setChild(index int, child Node[T]) {
	n.children[index] = child
}

func (n *Node8[T]) getKey() []byte {
	//no op
	return []byte{}
}

func (n *Node8[T]) getValue() T {
	//no op
	var zero T
	return zero
}

func (n *Node8[T]) getKeyAtIdx(idx int) byte {
	return n.keys[idx]
}

func (n *Node8[T]) setKeyAtIdx(idx int, key byte) {
	n.keys[idx] = key
}

func (n *Node8[T]) getChildren() []Node[T] {
	return n.children[:]
}

func (n *Node8[T]) getKeys() []byte {
	return n.keys[:]
}

func (n *Node8[T]) getMutateCh() chan struct{} {
	ch := n.mutateCh.Load()
	if ch != nil {
		return *ch
	}

	// No chan yet, create one
	newCh := make(chan struct{})

	swapped := n.mutateCh.CompareAndSwap(nil, &newCh)
	if swapped {
		return newCh
	}
	// We raced with another reader and they won so return the chan they created instead.
	return *n.mutateCh.Load()
}

func (n *Node8[T]) setValue(T) {

}

func (n *Node8[T]) setKey(key []byte) {
}

func (n *Node8[T]) getLowerBoundCh(c byte) int {
	nCh := int(n.getNumChildren())
	idx := sort.Search(nCh, func(i int) bool {
		return n.keys[i] >= c
	})
	// we want lower bound behavior so return even if it's not an exact match
	if idx < nCh {
		return idx
	}
	return -1
}

func (n *Node8[T]) ReverseIterator() *ReverseIterator[T] {
	nodeT := Node[T](n)
	return &ReverseIterator[T]{
		i: &Iterator[T]{
			node: nodeT,
		},
	}
}

func (n *Node8[T]) setMutateCh(ch chan struct{}) {
	n.mutateCh.Store(&ch)
}

func (n *Node8[T]) getNodeLeaf() *NodeLeaf[T] {
	return n.leaf
}

func (n *Node8[T]) setNodeLeaf(nl *NodeLeaf[T]) {
	n.leaf = nl
}
func (n *Node8[T]) LowerBoundIterator() *LowerBoundIterator[T] {
	return &LowerBoundIterator[T]{
		node: n,
	}
}

func (n *Node8[T]) incrementLazyRefCount(inc int64) {
	atomic.AddInt64(&n.lazyRefCount, inc)
}

func (n *Node8[T]) processRefCount() {
	if n.lazyRefCount == 0 {
		return
	}
	n.refCount += n.lazyRefCount
	if n.getNodeLeaf() != nil {
		n.getNodeLeaf().incrementLazyRefCount(n.lazyRefCount)
	}
	for _, child := range n.children {
		if child != nil {
			child.incrementLazyRefCount(n.lazyRefCount)
		}
	}
	atomic.StoreInt64(&n.lazyRefCount, 0)
}

func (n *Node8[T]) getRefCount() int64 {
	n.processRefCount()
	return n.refCount
}
