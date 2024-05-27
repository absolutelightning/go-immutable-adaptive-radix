// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

import (
	"bytes"
	"sync/atomic"
)

const maxPrefixLen = 10

const (
	leafType nodeType = iota
	node4
	node16
	node48
	node256
)

type nodeType int

type IDGenerator struct {
	counter  uint64
	delChns  map[chan struct{}]struct{}
	trackIds map[uint64]struct{}
}

// NewIDGenerator initializes a new IDGenerator
func NewIDGenerator() *IDGenerator {
	return &IDGenerator{
		counter:  0,
		delChns:  make(map[chan struct{}]struct{}),
		trackIds: make(map[uint64]struct{})}
}

// GenerateID generates a new atomic ID
func (gen *IDGenerator) GenerateID() (uint64, chan struct{}) {
	ch := make(chan struct{})
	id := atomic.AddUint64(&gen.counter, 1)
	return id, ch
}

type RadixTree[T any] struct {
	root Node[T]
	size uint64
	idg  *IDGenerator
}

// WalkFn is used when walking the tree. Takes a
// key and value, returning if iteration should
// be terminated.
type WalkFn[T any] func(k []byte, v T) bool

func NewRadixTree[T any]() *RadixTree[T] {
	rt := &RadixTree[T]{size: 0}
	rt.root = &NodeLeaf[T]{}
	rt.idg = NewIDGenerator()
	id, ch := rt.idg.GenerateID()
	rt.root.setId(id)
	rt.root.setMutateCh(ch)
	return rt
}

// Len is used to return the number of elements in the tree
func (t *RadixTree[T]) Len() int {
	return int(t.size)
}

// Clone is used to return the clone of tree
func (t *RadixTree[T]) Clone(deep bool) *RadixTree[T] {
	t.root.processLazyRef()
	newRoot := t.root.clone(true, deep)
	newRoot.setId(t.root.getId())
	newRoot.incrementLazyRefCount(t.root.getRefCount() + 1)
	newRoot.processLazyRef()
	return &RadixTree[T]{root: newRoot, size: t.size, idg: t.idg}
}

func (t *RadixTree[T]) GetPathIterator(path []byte) *PathIterator[T] {
	nodeT := t.root
	return nodeT.PathIterator(path)
}

func (t *RadixTree[T]) Insert(key []byte, value T) (*RadixTree[T], T, bool) {
	txn := t.Txn()
	old, ok := txn.Insert(key, value)
	return txn.Commit(), old, ok
}

func (t *RadixTree[T]) Get(key []byte) (T, bool, <-chan struct{}) {
	return t.iterativeSearch(getTreeKey(key))
}

func (t *RadixTree[T]) LongestPrefix(k []byte) ([]byte, T, bool) {
	key := getTreeKey(k)
	var zero T
	if t.root == nil {
		return nil, zero, false
	}

	var child, last Node[T]
	depth := 0

	n := t.root
	for n != nil {

		// Bail if the prefix does not match
		if n.getPartialLen() > 0 {
			prefixLen := checkPrefix(n.getPartial(), int(n.getPartialLen()), key, depth)
			if prefixLen != min(maxPrefixLen, int(n.getPartialLen())) {
				break
			}
			depth += int(n.getPartialLen())
		}

		if depth >= len(key) {
			break
		}

		for _, ch := range n.getChildren() {
			if ch != nil {
				if isLeaf[T](ch) && bytes.HasPrefix(getKey(key), getKey(ch.getKey())) {
					last = ch
				}
			}
		}

		// Recursively search
		child, _ = t.findChild(n, key[depth])
		if child == nil {
			break
		}
		n = child
		depth++
	}

	if last != nil {
		return getKey(last.getKey()), last.getValue(), true
	}

	return nil, zero, false
}

func (t *RadixTree[T]) Minimum() *NodeLeaf[T] {
	return minimum[T](t.root)
}

func (t *RadixTree[T]) Maximum() *NodeLeaf[T] {
	return maximum[T](t.root)
}

func (t *RadixTree[T]) Delete(key []byte) (*RadixTree[T], T, bool) {
	txn := t.Txn()
	old, ok := txn.Delete(key)
	return txn.Commit(), old, ok
}

func (t *RadixTree[T]) iterativeSearch(key []byte) (T, bool, <-chan struct{}) {
	var zero T
	n := t.root
	n.processLazyRef()
	n.incrementLazyRefCount(1)
	watch := n.getMutateCh()
	if t.root == nil {
		n.incrementLazyRefCount(-1)
		return zero, false, watch
	}
	var child Node[T]
	depth := 0

	for {
		// Might be a leaf
		watch = n.getMutateCh()

		if isLeaf[T](n) {
			// Check if the expanded path matches
			if leafMatches(n.getKey(), key) == 0 {
				n.incrementLazyRefCount(-1)
				return n.getValue(), true, watch
			}
			break
		}

		// Bail if the prefix does not match
		if n.getPartialLen() > 0 {
			prefixLen := checkPrefix(n.getPartial(), int(n.getPartialLen()), key, depth)
			if prefixLen != min(maxPrefixLen, int(n.getPartialLen())) {
				n.incrementLazyRefCount(-1)
				return zero, false, watch
			}
			depth += int(n.getPartialLen())
		}

		if depth >= len(key) {
			n.incrementLazyRefCount(-1)
			return zero, false, watch
		}

		// Recursively search
		child, _ = t.findChild(n, key[depth])
		if child == nil {
			n.incrementLazyRefCount(-1)
			return zero, false, watch
		}
		n.incrementLazyRefCount(-1)
		n = child
		n.processLazyRef()
		n.incrementLazyRefCount(1)
		depth++
	}
	n.incrementLazyRefCount(-1)
	return zero, false, nil
}

func (t *RadixTree[T]) DeletePrefix(key []byte) (*RadixTree[T], bool) {
	txn := t.Txn()
	ok := txn.DeletePrefix(key)
	return txn.Commit(), ok
}

func (t *RadixTree[T]) deletePrefix(node Node[T], key []byte, depth int) (Node[T], int) {
	// Get terminated
	if node == nil {
		return nil, 0
	}
	// Handle hitting a leaf node
	if isLeaf[T](node) {
		if bytes.HasPrefix(getKey(node.getKey()), getKey(key)) {
			return nil, 1
		}
		return node, 0
	}

	// Bail if the prefix does not match
	if node.getPartialLen() > 0 {
		prefixLen := checkPrefix(node.getPartial(), int(node.getPartialLen()), key, depth)
		if prefixLen < min(maxPrefixLen, len(getKey(key))) {
			depth += prefixLen
		} else {
			return node, 0
		}
	}

	numDel := 0

	// Recurse on the children
	var newChIndxMap = make(map[int]Node[T])
	for idx, ch := range node.getChildren() {
		if ch != nil {
			newCh, del := t.deletePrefix(ch, key, depth+1)
			newChIndxMap[idx] = newCh
			numDel += del
		}
	}

	for idx, ch := range newChIndxMap {
		node.setChild(idx, ch)
	}

	return node, numDel
}

// findChild finds the child node pointer based on the given character in the ART tree node.
func (t *RadixTree[T]) findChild(n Node[T], c byte) (Node[T], int) {
	return findChild(n, c)
}

// Root returns the root node of the tree which can be used for richer
// query operations.
func (t *RadixTree[T]) Root() Node[T] {
	return t.root
}

// GetWatch is used to lookup a specific key, returning
// the watch channel, value and if it was found
func (t *RadixTree[T]) GetWatch(k []byte) (<-chan struct{}, T, bool) {
	res, found, watch := t.Get(k)
	return watch, res, found
}

// Walk is used to walk the tree
func (t *RadixTree[T]) Walk(fn WalkFn[T]) {
	recursiveWalk(t.root, fn)
}

func (t *RadixTree[T]) DFS(fn DfsFn[T]) {
	t.DFSNode(t.root, fn)
}

// recursiveWalk is used to do a pre-order walk of a node
// recursively. Returns true if the walk should be aborted
func recursiveWalk[T any](n Node[T], fn WalkFn[T]) bool {
	// Visit the leaf values if any
	if n.isLeaf() && fn(getKey(n.getKey()), n.getValue()) {
		return true
	}

	// Recurse on the children
	for _, e := range n.getChildren() {
		if e != nil {
			if recursiveWalk(e, fn) {
				return true
			}
		}
	}
	return false
}

type DfsFn[T any] func(n Node[T])

// recursiveWalk is used to do a pre-order walk of a node
// recursively. Returns true if the walk should be aborted
func (t *RadixTree[T]) DFSNode(n Node[T], fn DfsFn[T]) {
	// Visit the leaf values if any
	fn(n)

	// Recurse on the children
	for _, e := range n.getChildren() {
		if e != nil {
			t.DFSNode(e, fn)
		}
	}
}
