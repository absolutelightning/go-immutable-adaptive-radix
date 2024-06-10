// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

import (
	"sync/atomic"
)

type Node256[T any] struct {
	id          uint64
	partialLen  uint32
	numChildren uint8
	partial     []byte
	children    [256]Node[T]
	mutateCh    atomic.Pointer[chan struct{}]
	leaf        *NodeLeaf[T]
}

func (n *Node256[T]) getId() uint64 {
	return n.id
}

func (n *Node256[T]) getPartialLen() uint32 {
	return n.partialLen
}

func (n *Node256[T]) setId(id uint64) {
	n.id = id
}

func (n *Node256[T]) setPartialLen(partialLen uint32) {
	n.partialLen = partialLen
}

func (n *Node256[T]) getKeyLen() uint32 {
	return 0
}

func (n *Node256[T]) setKeyLen(keyLen uint32) {

}

func (n *Node256[T]) getArtNodeType() nodeType {
	return node256
}

func (n *Node256[T]) getNumChildren() uint8 {
	return n.numChildren
}

func (n *Node256[T]) setNumChildren(numChildren uint8) {
	n.numChildren = numChildren
}

func (n *Node256[T]) getPartial() []byte {
	return n.partial
}

func (n *Node256[T]) setPartial(partial []byte) {
	n.partial = partial
}

func (n *Node256[T]) isLeaf() bool {
	if n.numChildren == 0 && n.getNodeLeaf() != nil {
		return true
	}
	return false
}

// Iterator is used to return an Iterator at
// the given node to walk the tree
func (n *Node256[T]) Iterator() *Iterator[T] {
	stack := make([]Node[T], 0)
	stack = append(stack, n)
	nodeT := Node[T](n)
	return &Iterator[T]{
		stack: stack,
		node:  nodeT,
	}
}

func (n *Node256[T]) PathIterator(path []byte) *PathIterator[T] {
	nodeT := Node[T](n)
	return &PathIterator[T]{node: &nodeT,
		path:  getTreeKey(path),
		stack: []Node[T]{nodeT},
	}
}

func (n *Node256[T]) matchPrefix(_ []byte) bool {
	// No partial keys in NODE256, always match
	return true
}

func (n *Node256[T]) getChild(index int) Node[T] {
	if index < 0 || index >= 256 {
		return nil
	}
	return n.children[index]
}

func (n *Node256[T]) clone(keepWatch bool) Node[T] {
	newNode := &Node256[T]{}
	newNode.setPartialLen(n.getPartialLen())
	newNode.setNumChildren(n.getNumChildren())
	if keepWatch {
		newNode.setMutateCh(n.getMutateCh())
	}
	newNode.setNodeLeaf(n.getNodeLeaf())
	newPartial := make([]byte, maxPrefixLen)
	newNode.setId(n.getId())
	copy(newPartial, n.partial)
	newNode.setPartial(newPartial)
	cpy := make([]Node[T], len(n.children))
	copy(cpy, n.children[:])
	for i := 0; i < 256; i++ {
		newNode.setChild(i, cpy[i])
	}
	return newNode
}

func (n *Node256[T]) setChild(index int, child Node[T]) {
	n.children[index] = child
}

func (n *Node256[T]) getKey() []byte {
	//no op
	return []byte{}
}

func (n *Node256[T]) getValue() T {
	//no op
	var zero T
	return zero
}

func (n *Node256[T]) getKeyAtIdx(idx int) byte {
	return 0
}

func (n *Node256[T]) setKeyAtIdx(idx int, key byte) {
}

func (n *Node256[T]) getChildren() []Node[T] {
	return n.children[:]
}

func (n *Node256[T]) getKeys() []byte {
	return nil
}

func (n *Node256[T]) getMutateCh() chan struct{} {
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

func (n *Node256[T]) setValue(T) {

}

func (n *Node256[T]) setKey(key []byte) {
}

func (n *Node256[T]) getLowerBoundCh(c byte) int {
	for i := int(c); i < 256; i++ {
		if n.getChild(i) != nil {
			return i
		}
	}
	return -1
}

func (n *Node256[T]) ReverseIterator() *ReverseIterator[T] {
	nodeT := Node[T](n)
	return &ReverseIterator[T]{
		i: &Iterator[T]{
			stack: []Node[T]{nodeT},
			node:  nodeT,
		},
	}
}

func (n *Node256[T]) setMutateCh(ch chan struct{}) {
	n.mutateCh.Store(&ch)
}

func (n *Node256[T]) getNodeLeaf() *NodeLeaf[T] {
	return n.leaf
}

func (n *Node256[T]) setNodeLeaf(nl *NodeLeaf[T]) {
	n.leaf = nl
}
