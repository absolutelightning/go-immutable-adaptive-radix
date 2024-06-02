// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

import (
	"bytes"
	"sort"
	"sync/atomic"
)

type Node4[T any] struct {
	id          uint64
	partialLen  uint32
	numChildren uint8
	partial     []byte
	keys        [4]byte
	children    [4]Node[T]
	mutateCh    atomic.Pointer[chan struct{}]
}

func (n *Node4[T]) getId() uint64 {
	return n.id
}

func (n *Node4[T]) setId(id uint64) {
	n.id = id
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
	return n.children[index]
}

func (n *Node4[T]) clone(keepWatch, deep bool) Node[T] {
	newNode := &Node4[T]{
		partialLen:  n.getPartialLen(),
		numChildren: n.getNumChildren(),
	}
	newNode.setId(n.getId())
	if keepWatch {
		newNode.setMutateCh(n.getMutateCh())
	}
	newPartial := make([]byte, maxPrefixLen)
	copy(newPartial, n.partial)
	newNode.setPartial(newPartial)
	copy(newNode.keys[:], n.keys[:])
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

func (n *Node4[T]) setMutateCh(ch chan struct{}) {
	n.mutateCh.Store(&ch)
}
