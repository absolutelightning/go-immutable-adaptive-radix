// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

import (
	"bytes"
	"sync"
	"sync/atomic"
)

type NodeLeaf[T any] struct {
	id           uint64
	value        T
	key          []byte
	mutateCh     chan struct{}
	refCount     int32
	lazyRefCount int32
	oldRef       Node[T]
	mu           *sync.RWMutex
}

func (n *NodeLeaf[T]) getId() uint64 {
	return n.id
}

func (n *NodeLeaf[T]) setId(id uint64) {
	n.id = id
}

func (n *NodeLeaf[T]) incrementRefCount() int32 {
	return atomic.AddInt32(&n.refCount, 1)
}

func (n *NodeLeaf[T]) decrementRefCount() int32 {
	return atomic.AddInt32(&n.refCount, -1)
}

func (n *NodeLeaf[T]) getPartialLen() uint32 {
	// no-op
	return 0
}

func (n *NodeLeaf[T]) setPartialLen(partialLen uint32) {
	// no-op
}

func (n *NodeLeaf[T]) getArtNodeType() nodeType {
	return leafType
}

func (n *NodeLeaf[T]) getNumChildren() uint8 {
	return 0
}

func (n *NodeLeaf[T]) setNumChildren(numChildren uint8) {
	// no-op
}

func (n *NodeLeaf[T]) isLeaf() bool {
	return true
}

func (n *NodeLeaf[T]) getValue() T {
	return n.value
}

func (n *NodeLeaf[T]) setValue(value T) {
	n.value = value
}

func (n *NodeLeaf[T]) getKeyLen() uint32 {
	return uint32(len(n.key))
}

func (n *NodeLeaf[T]) setKeyLen(keyLen uint32) {
	// no-op
}

func (n *NodeLeaf[T]) getKey() []byte {
	return n.key
}

func (n *NodeLeaf[T]) setKey(key []byte) {
	n.key = key
}

func (n *NodeLeaf[T]) getPartial() []byte {
	//no-op
	return []byte{}
}

func (n *NodeLeaf[T]) setPartial(partial []byte) {
	// no-op
}

func (l *NodeLeaf[T]) prefixContainsMatch(key []byte) bool {
	if len(key) == 0 || len(l.key) == 0 {
		return false
	}
	if key == nil {
		return false
	}

	return bytes.HasPrefix(getKey(key), getKey(l.key))
}

func (n *NodeLeaf[T]) Iterator() *Iterator[T] {
	stack := make([]Node[T], 0)
	stack = append(stack, n)
	nodeT := Node[T](n)
	return &Iterator[T]{
		stack: stack,
		node:  nodeT,
	}
}

func (n *NodeLeaf[T]) PathIterator(path []byte) *PathIterator[T] {
	nodeT := Node[T](n)
	return &PathIterator[T]{node: &nodeT,
		path:  getTreeKey(path),
		stack: []Node[T]{nodeT},
	}
}

func (n *NodeLeaf[T]) matchPrefix(prefix []byte) bool {
	if len(n.key) == 0 {
		return false
	}
	actualKey := getKey(n.key)
	actualPrefix := getKey(prefix)
	return bytes.HasPrefix(actualKey, actualPrefix)
}

func (n *NodeLeaf[T]) getChild(index int) Node[T] {
	return nil
}

func (n *NodeLeaf[T]) clone(keepWatch, deep bool) Node[T] {
	newNode := &NodeLeaf[T]{
		key:   make([]byte, len(n.getKey())),
		value: n.getValue(),
		mu:    &sync.RWMutex{},
	}
	if keepWatch {
		newNode.setMutateCh(n.getMutateCh())
	} else {
		newNode.setMutateCh(make(chan struct{}))
	}
	copy(newNode.key[:], n.key[:])
	nodeT := Node[T](newNode)
	return nodeT
}

func (n *NodeLeaf[T]) setChild(int, Node[T]) {
	return
}

func (n *NodeLeaf[T]) getKeyAtIdx(idx int) byte {
	// no op
	return 0
}

func (n *NodeLeaf[T]) setKeyAtIdx(idx int, key byte) {
}

func (n *NodeLeaf[T]) getChildren() []Node[T] {
	return nil
}

func (n *NodeLeaf[T]) getKeys() []byte {
	return nil
}

func (n *NodeLeaf[T]) getMutateCh() chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.mutateCh
}

func (n *NodeLeaf[T]) setMutateCh(ch chan struct{}) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if ch == nil {
		ch = make(chan struct{})
	}
	n.mutateCh = ch
}

func (n *NodeLeaf[T]) getLowerBoundCh(c byte) int {
	return -1
}

func (n *NodeLeaf[T]) ReverseIterator() *ReverseIterator[T] {
	nodeT := Node[T](n)
	return &ReverseIterator[T]{
		i: &Iterator[T]{
			stack: []Node[T]{nodeT},
			node:  nodeT,
		},
	}
}

func (n *NodeLeaf[T]) createNewMutateChn() chan struct{} {
	muCh := make(chan struct{})
	n.setMutateCh(muCh)
	return muCh
}

func (n *NodeLeaf[T]) getRefCount() int32 {
	n.processLazyRef()
	return atomic.LoadInt32(&n.refCount)
}

func (n *NodeLeaf[T]) incrementLazyRefCount(val int32) int32 {
	return atomic.AddInt32(&n.lazyRefCount, val)
}

func (n *NodeLeaf[T]) processLazyRef() {
	val := atomic.LoadInt32(&n.lazyRefCount)
	atomic.AddInt32(&n.refCount, val)
	atomic.StoreInt32(&n.lazyRefCount, 0)
}

func (n *NodeLeaf[T]) setOldRef(or Node[T]) {
	n.oldRef = or
}

func (n *NodeLeaf[T]) getOldRef() Node[T] {
	return n.oldRef
}

func (n *NodeLeaf[T]) changeRefCount() int32 {
	atomic.AddInt32(&n.refCount, -1)
	return n.decrementRefCount()
}

func (n *NodeLeaf[T]) changeRefCountNoDecrement() int32 {
	return atomic.LoadInt32(&n.refCount)
}
