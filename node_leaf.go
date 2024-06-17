// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

import (
	"bytes"
	"sync/atomic"
)

type NodeLeaf[T any] struct {
	id       uint64
	value    T
	key      []byte
	mutateCh atomic.Pointer[chan struct{}]
}

func (n *NodeLeaf[T]) getId() uint64 {
	return n.id
}

func (n *NodeLeaf[T]) setId(id uint64) {
	n.id = id
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
	nodeT := Node[T](n)
	return &Iterator[T]{
		node: nodeT,
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
	return bytes.HasPrefix(n.key, prefix)
}

func (n *NodeLeaf[T]) getChild(index int) Node[T] {
	return nil
}

func (n *NodeLeaf[T]) clone(keepWatch bool) Node[T] {
	newNode := &NodeLeaf[T]{
		key:   make([]byte, len(n.getKey())),
		value: n.getValue(),
	}
	if keepWatch {
		newNode.setMutateCh(n.getMutateCh())
	}
	newNode.setId(n.getId())
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
	// This must be lock free but we should ensure that concurrent callers will
	// end up with the same chan
	// Fast path if there is already a chan
	ch := n.mutateCh.Load()
	if ch != nil && !isClosed(*ch) {
		return *ch
	}

	// No chan yet, create one
	newCh := make(chan struct{})

	swapped := n.mutateCh.CompareAndSwap(ch, &newCh)
	if swapped {
		return newCh
	}
	// We raced with another reader and they won so return the chan they created instead.
	return *n.mutateCh.Load()
}

func (n *NodeLeaf[T]) getLowerBoundCh(c byte) int {
	return -1
}

func (n *NodeLeaf[T]) ReverseIterator() *ReverseIterator[T] {
	return &ReverseIterator[T]{
		i: &Iterator[T]{
			node: n,
		},
	}
}

func (n *NodeLeaf[T]) setMutateCh(ch chan struct{}) {
	n.mutateCh.Store(&ch)
}

func (n *NodeLeaf[T]) getNodeLeaf() *NodeLeaf[T] {
	return nil
}

func (n *NodeLeaf[T]) setNodeLeaf(nl *NodeLeaf[T]) {
	// no op
}

func (n *NodeLeaf[T]) LowerBoundIterator() *LowerBoundIterator[T] {
	return &LowerBoundIterator[T]{
		node: n,
	}
}
