// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

import (
	"bytes"
	"sync/atomic"
)

type Node48[T any] struct {
	id           uint64
	partialLen   uint32
	numChildren  uint8
	partial      []byte
	keys         [256]byte
	children     [48]*Node[T]
	mutateCh     atomic.Pointer[chan struct{}]
	leaf         *Node[T]
	lazyRefCount int64
	refCount     int64
}

func (n *Node48[T]) getId() uint64 {
	return n.id
}

func (n *Node48[T]) setId(id uint64) {
	n.id = id
}

func (n *Node48[T]) getPartialLen() uint32 {
	return n.partialLen
}

func (n *Node48[T]) setPartialLen(partialLen uint32) {
	n.partialLen = partialLen
}

func (n *Node48[T]) getArtNodeType() nodeType {
	return node48
}

func (n *Node48[T]) getNumChildren() uint8 {
	return n.numChildren
}

func (n *Node48[T]) setNumChildren(numChildren uint8) {
	n.numChildren = numChildren
}

func (n *Node48[T]) getPartial() []byte {
	return n.partial
}

func (n *Node48[T]) setPartial(partial []byte) {
	n.partial = partial
}

func (n *Node48[T]) isLeaf() bool {
	if n.numChildren == 0 && n.getNodeLeaf() != nil {
		return true
	}
	return false
}

// Iterator is used to return an Iterator at
// the given node to walk the tree
func (n *Node48[T]) Iterator() *Iterator[T] {
	return &Iterator[T]{
		node: n,
	}
}

func (n *Node48[T]) PathIterator(path []byte) *PathIterator[T] {
	nodeT := Node[T](n)
	return &PathIterator[T]{
		node:  &nodeT,
		path:  getTreeKey(path),
		stack: []Node[T]{nodeT},
	}
}

func (n *Node48[T]) matchPrefix(prefix []byte) bool {
	for i := 0; i < 256; i++ {
		if n.keys[i] == 0 {
			continue
		}
		childPrefix := []byte{byte(i)}
		if bytes.HasPrefix(childPrefix, prefix) {
			return true
		}
	}
	return false
}

func (n *Node48[T]) getChild(index int) *Node[T] {
	if n.children[index] == nil {
		return nil
	}
	return n.children[index]
}

func (n *Node48[T]) clone(keepWatch, deep bool) *Node[T] {
	n.processRefCount()
	newNode := &Node48[T]{
		partialLen:  n.getPartialLen(),
		numChildren: n.getNumChildren(),
		refCount:    n.getRefCount(),
	}
	newNode.setId(n.getId())
	newPartial := make([]byte, maxPrefixLen)
	copy(newPartial, n.partial)
	newNode.setPartial(newPartial)
	if deep {
		if n.getNodeLeaf() != nil {
			newNode.setNodeLeaf((*n.getNodeLeaf()).clone(true, true))
		}
	} else {
		newNode.setNodeLeaf(n.getNodeLeaf())
	}
	if keepWatch {
		newNode.setMutateCh(n.getMutateCh())
	}
	copy(newNode.keys[:], n.keys[:])
	if deep {
		cpy := make([]*Node[T], len(n.children))
		copy(cpy, n.children[:])
		for i := 0; i < 48; i++ {
			if cpy[i] == nil || *cpy[i] == nil {
				continue
			}
			newNode.setChild(i, (*cpy[i]).clone(keepWatch, true))
		}
	} else {
		cpy := make([]*Node[T], len(n.children))
		copy(cpy, n.children[:])
		for i := 0; i < 48; i++ {
			if cpy[i] == nil {
				continue
			}
			newNode.setChild(i, cpy[i])
		}
	}
	nodeT := Node[T](newNode)
	return &nodeT
}

func (n *Node48[T]) getKeyLen() uint32 {
	return 0
}

func (n *Node48[T]) setKeyLen(keyLen uint32) {

}

func (n *Node48[T]) setChild(index int, child *Node[T]) {
	n.children[index] = child
}

func (n *Node48[T]) getKey() []byte {
	//no op
	return []byte{}
}

func (n *Node48[T]) getValue() T {
	//no op
	var zero T
	return zero
}

func (n *Node48[T]) getKeyAtIdx(idx int) byte {
	return n.keys[idx]
}

func (n *Node48[T]) setKeyAtIdx(idx int, key byte) {
	n.keys[idx] = key
}

func (n *Node48[T]) getChildren() []*Node[T] {
	return n.children[:]
}

func (n *Node48[T]) getKeys() []byte {
	return n.keys[:]
}

func (n *Node48[T]) getMutateCh() chan struct{} {
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

func (n *Node48[T]) setValue(T) {
}

func (n *Node48[T]) setKey(key []byte) {
}

func (n *Node48[T]) getLowerBoundCh(c byte) int {
	for i := int(c); i < 256; i++ {
		if n.getChild(int(n.keys[i])-1) != nil {
			return int(n.keys[i] - 1)
		}
	}
	return -1
}

func (n *Node48[T]) ReverseIterator() *ReverseIterator[T] {
	nodeT := Node[T](n)
	return &ReverseIterator[T]{
		i: &Iterator[T]{
			node: nodeT,
		},
	}
}
func (n *Node48[T]) setMutateCh(ch chan struct{}) {
	n.mutateCh.Store(&ch)
}

func (n *Node48[T]) getNodeLeaf() *Node[T] {
	return n.leaf
}

func (n *Node48[T]) setNodeLeaf(nl *Node[T]) {
	n.leaf = nl
}
func (n *Node48[T]) LowerBoundIterator() *LowerBoundIterator[T] {
	nodeT := Node[T](n)
	return &LowerBoundIterator[T]{
		node: nodeT,
	}
}

func (n *Node48[T]) incrementLazyRefCount(inc int64) {
	atomic.AddInt64(&n.lazyRefCount, inc)
}

func (n *Node48[T]) processRefCount() {
	if n.lazyRefCount == 0 {
		return
	}
	n.refCount += n.lazyRefCount
	if n.getNodeLeaf() != nil {
		(*n.getNodeLeaf()).incrementLazyRefCount(n.lazyRefCount)
	}
	for _, child := range n.children {
		if child != nil && *child != nil {
			(*child).incrementLazyRefCount(n.lazyRefCount)
		}
	}
	atomic.StoreInt64(&n.lazyRefCount, 0)
}

func (n *Node48[T]) getRefCount() int64 {
	n.processRefCount()
	return n.refCount
}
