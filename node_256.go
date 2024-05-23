// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

import "sync"

type Node256[T any] struct {
	partialLen  uint32
	numChildren uint8
	partial     []byte
	children    [256]Node[T]
	mutateCh    chan struct{}
	mu          *sync.RWMutex
}

func (n *Node256[T]) getPartialLen() uint32 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.partialLen
}

func (n *Node256[T]) setPartialLen(partialLen uint32) {
	n.mu.Lock()
	defer n.mu.Unlock()
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
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.partial
}

func (n *Node256[T]) setPartial(partial []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partial = partial
}

func (n *Node256[T]) setMutex(mu *sync.RWMutex) {
	n.mu = mu
}

func (n *Node256[T]) isLeaf() bool {
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
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.children[index]
}

func (n *Node256[T]) clone(keepWatch bool, deep bool) Node[T] {
	newNode := &Node256[T]{
		partialLen:  n.getPartialLen(),
		numChildren: n.getNumChildren(),
		partial:     n.getPartial(),
		mu:          n.mu,
	}
	if keepWatch {
		newNode.mutateCh = n.getMutateCh()
	} else {
		newNode.mutateCh = make(chan struct{})
	}
	if deep {
		for i := 0; i < 256; i++ {
			if n.children[i] != nil {
				newNode.children[i] = n.children[i].clone(keepWatch, deep)
			}
		}
	} else {
		copy(newNode.children[:], n.children[:])
	}
	return newNode
}

func (n *Node256[T]) setChild(index int, child Node[T]) {
	n.mu.Lock()
	defer n.mu.Unlock()
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
	return n.mutateCh
}

func (n *Node256[T]) setMutateCh(ch chan struct{}) {
	n.mutateCh = ch
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
