// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
)

type Node4[T any] struct {
	id           uint64
	partialLen   uint32
	numChildren  uint8
	partial      []byte
	keys         [4]byte
	children     [4]Node[T]
	mutateCh     chan struct{}
	refCount     int32
	lazyRefCount int32
	oldRef       Node[T]
	mu           *sync.RWMutex
}

func (n *Node4[T]) getId() uint64 {
	return n.id
}

func (n *Node4[T]) setId(id uint64) {
	n.id = id
}

func (n *Node4[T]) decrementRefCount() int32 {
	n.processLazyRef()
	return atomic.AddInt32(&n.refCount, -1)
}

func (n *Node4[T]) incrementRefCount() int32 {
	n.processLazyRef()
	return atomic.AddInt32(&n.refCount, 1)
}

func (n *Node4[T]) getPartialLen() uint32 {
	return n.partialLen
}

func (n *Node4[T]) setPartialLen(partialLen uint32) {
	n.partialLen = partialLen
}

func (n *Node4[T]) getArtNodeType() nodeType {
	return node4
}

func (n *Node4[T]) getNumChildren() uint8 {
	return n.numChildren
}

func (n *Node4[T]) setNumChildren(numChildren uint8) {
	n.numChildren = numChildren
}

func (n *Node4[T]) getPartial() []byte {
	return n.partial
}

func (n *Node4[T]) setPartial(partial []byte) {
	n.partial = partial
}

func (n *Node4[T]) isLeaf() bool {
	return false
}

// Iterator is used to return an Iterator at
// the given node to walk the tree
func (n *Node4[T]) Iterator() *Iterator[T] {
	stack := make([]Node[T], 0)
	stack = append(stack, n)
	nodeT := Node[T](n)
	return &Iterator[T]{
		stack: stack,
		node:  nodeT,
	}
}

func (n *Node4[T]) PathIterator(path []byte) *PathIterator[T] {
	nodeT := Node[T](n)
	return &PathIterator[T]{node: &nodeT,
		path:  getTreeKey(path),
		stack: []Node[T]{nodeT},
	}
}

func (n *Node4[T]) matchPrefix(prefix []byte) bool {
	return bytes.HasPrefix(n.partial, prefix)
}

func (n *Node4[T]) getChild(index int) Node[T] {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.children[index]
}

func (n *Node4[T]) clone(keepWatch, deep bool) Node[T] {
	newNode := &Node4[T]{
		partialLen:  n.getPartialLen(),
		numChildren: n.getNumChildren(),
		mu:          &sync.RWMutex{},
	}
	newPartial := make([]byte, maxPrefixLen)
	copy(newPartial, n.partial)
	newNode.setPartial(newPartial)
	copy(newNode.keys[:], n.keys[:])
	if keepWatch {
		newNode.setMutateCh(n.getMutateCh())
	} else {
		newNode.setMutateCh(make(chan struct{}))
	}
	if deep {
		for i := 0; i < 4; i++ {
			if n.children[i] != nil {
				newNode.children[i] = n.children[i].clone(keepWatch, deep)
			}
		}
	} else {
		cpy := make([]Node[T], len(n.children))
		copy(cpy, n.children[:])
		for i := 0; i < 4; i++ {
			newNode.setChild(i, cpy[i])
		}

	}
	return newNode
}

func (n *Node4[T]) getKeyLen() uint32 {
	return 0
}

func (n *Node4[T]) setKeyLen(keyLen uint32) {

}

func (n *Node4[T]) setChild(index int, child Node[T]) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.children[index] = child
}

func (n *Node4[T]) getKey() []byte {
	//no op
	return []byte{}
}

func (n *Node4[T]) getValue() T {
	//no op
	var zero T
	return zero
}

func (n *Node4[T]) getKeyAtIdx(idx int) byte {
	return n.keys[idx]
}

func (n *Node4[T]) setKeyAtIdx(idx int, key byte) {
	n.keys[idx] = key
}

func (n *Node4[T]) getChildren() []Node[T] {
	return n.children[:]
}

func (n *Node4[T]) getKeys() []byte {
	return n.keys[:]
}

func (n *Node4[T]) getMutateCh() chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.mutateCh
}

func (n *Node4[T]) setMutateCh(ch chan struct{}) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if ch == nil {
		ch = make(chan struct{})
	}
	n.mutateCh = ch
}

func (n *Node4[T]) setValue(T) {

}

func (n *Node4[T]) setKey(key []byte) {
}

func (n *Node4[T]) getLowerBoundCh(c byte) int {
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

func (n *Node4[T]) ReverseIterator() *ReverseIterator[T] {
	nodeT := Node[T](n)
	return &ReverseIterator[T]{
		i: &Iterator[T]{
			stack: []Node[T]{nodeT},
			node:  nodeT,
		},
	}
}

func (n *Node4[T]) createNewMutateChn() chan struct{} {
	muCh := make(chan struct{})
	n.setMutateCh(muCh)
	return muCh
}

func (n *Node4[T]) incrementLazyRefCount(val int32) int32 {
	return atomic.AddInt32(&n.lazyRefCount, val)
}

func (n *Node4[T]) getRefCount() int32 {
	n.processLazyRef()
	return atomic.LoadInt32(&n.refCount)
}

func (n *Node4[T]) processLazyRef() {
	lazyRefCount := atomic.LoadInt32(&n.lazyRefCount)
	atomic.AddInt32(&n.refCount, lazyRefCount)
	for i := 0; i < 4; i++ {
		if n.children[i] != nil {
			n.children[i].incrementLazyRefCount(lazyRefCount)
		}
	}
	atomic.StoreInt32(&n.lazyRefCount, 0)
}

func (n *Node4[T]) setOldRef(or Node[T]) {
	n.oldRef = or
}

func (n *Node4[T]) getOldRef() Node[T] {
	return n.oldRef
}

func (n *Node4[T]) changeRefCount() int32 {
	atomic.AddInt32(&n.refCount, -1)
	return n.decrementRefCount()
}

func (n *Node4[T]) changeRefCountNoDecrement() int32 {
	return atomic.LoadInt32(&n.refCount)
}
