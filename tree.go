// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

import (
	"bytes"
	"fmt"
	"strconv"
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

type RadixTree[T any] struct {
	root      Node[T]
	size      uint64
	maxNodeId uint64
}

// WalkFn is used when walking the tree. Takes a
// key and value, returning if iteration should
// be terminated.
type WalkFn[T any] func(k []byte, v T) bool

func NewRadixTree[T any]() *RadixTree[T] {
	rt := &RadixTree[T]{size: 0, maxNodeId: 0}
	rt.root = &Node4[T]{
		leaf: &NodeLeaf[T]{},
	}
	rt.root.setId(rt.maxNodeId)
	rt.root.getNodeLeaf().setId(rt.maxNodeId + 1)
	rt.maxNodeId++
	return rt
}

// Len is used to return the number of elements in the tree
func (t *RadixTree[T]) Len() int {
	return int(t.size)
}

// Clone is used to return the clone of tree
//func (t *RadixTree[T]) Clone(deep bool) *RadixTree[T] {
//	newRoot := t.root.clone(true, deep)
//	if deep {
//		newRoot = t.root.clone(false, deep)
//	}
//	return &RadixTree[T]{root: newRoot, size: t.size, maxNodeId: t.maxNodeId}
//}

func (t *RadixTree[T]) GetPathIterator(path []byte) *PathIterator[T] {
	return t.root.PathIterator(path)
}

func (t *RadixTree[T]) Insert(key []byte, value T) (*RadixTree[T], T, bool) {
	txn := t.Txn()
	old, ok := txn.Insert(key, value)
	return txn.Commit(), old, ok
}

func (t *RadixTree[T]) Get(key []byte) (T, bool) {
	return t.iterativeSearch(getTreeKey(key))
}

func (t *RadixTree[T]) GetWatch(key []byte) (<-chan struct{}, T, bool) {
	val, found, watch := t.iterativeSearchWithWatch(getTreeKey(key))
	return watch, val, found
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
	last = nil
	if n.isLeaf() && n.getNodeLeaf() != nil {
		last = n.getNodeLeaf()
	}
	for {

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

		if len(key) >= 2 && n.getNodeLeaf() != nil && len(n.getNodeLeaf().getKey()) >= 2 && bytes.HasPrefix(getKey(key), getKey(n.getNodeLeaf().getKey())) {
			last = n.getNodeLeaf()
		}

		for _, ch := range n.getChildren() {
			if ch != nil {
				if ch.getNodeLeaf() != nil && bytes.HasPrefix(getKey(key), getKey(ch.getNodeLeaf().getKey())) {
					last = ch.getNodeLeaf()
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

	if last != nil && len(last.getKey()) >= 2 {
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

func (t *RadixTree[T]) iterativeSearch(key []byte) (T, bool) {
	var zero T
	n := t.root

	var child Node[T]
	depth := 0

	for {
		// Might be a leaf

		if isLeaf[T](n) {
			// Check if the expanded path matches
			nL := n.getNodeLeaf()
			if leafMatches(nL.getKey(), key) == 0 {
				return nL.getValue(), true
			}
		}

		// Bail if the prefix does not match
		if n.getPartialLen() > 0 {
			prefixLen := checkPrefix(n.getPartial(), int(n.getPartialLen()), key, depth)
			if prefixLen != min(maxPrefixLen, int(n.getPartialLen())) {
				if n.getNodeLeaf() != nil {
					if leafMatches(n.getNodeLeaf().getKey(), key) == 0 {
						return n.getNodeLeaf().getValue(), true
					}
				}
				return zero, false
			}
			depth += int(n.getPartialLen())
		}

		if depth >= len(key) {
			if n.getNodeLeaf() != nil {
				if leafMatches(n.getNodeLeaf().getKey(), key) == 0 {
					return n.getNodeLeaf().getValue(), true
				}
			}
			return zero, false
		}

		// Recursively search
		child, _ = t.findChild(n, key[depth])
		if child == nil {
			if n.getNodeLeaf() != nil {
				if leafMatches(n.getNodeLeaf().getKey(), key) == 0 {
					return n.getNodeLeaf().getValue(), true
				}
			}
			return zero, false
		}
		n = child
		depth++
	}
}

func (t *RadixTree[T]) iterativeSearchWithWatch(key []byte) (T, bool, <-chan struct{}) {
	var zero T
	n := t.root

	var child Node[T]
	depth := 0

	for {
		// Might be a leaf

		if isLeaf[T](n) {
			// Check if the expanded path matches
			nL := n.getNodeLeaf()
			if leafMatches(nL.getKey(), key) == 0 {
				return nL.getValue(), true, nL.getMutateCh()
			}
		}

		// Bail if the prefix does not match
		if n.getPartialLen() > 0 {
			prefixLen := checkPrefix(n.getPartial(), int(n.getPartialLen()), key, depth)
			if prefixLen != min(maxPrefixLen, int(n.getPartialLen())) {
				if n.getNodeLeaf() != nil {
					if leafMatches(n.getNodeLeaf().getKey(), key) == 0 {
						return n.getNodeLeaf().getValue(), true, n.getNodeLeaf().getMutateCh()
					}
				}
				return zero, false, n.getMutateCh()
			}
			depth += int(n.getPartialLen())
		}

		if depth >= len(key) {
			if n.getNodeLeaf() != nil {
				if leafMatches(n.getNodeLeaf().getKey(), key) == 0 {
					return n.getNodeLeaf().getValue(), true, n.getNodeLeaf().getMutateCh()
				}
			}
			return zero, false, n.getMutateCh()
		}

		// Recursively search
		child, _ = t.findChild(n, key[depth])
		if child == nil {
			if n.getNodeLeaf() != nil {
				if leafMatches(n.getNodeLeaf().getKey(), key) == 0 {
					return n.getNodeLeaf().getValue(), true, n.getNodeLeaf().getMutateCh()
				}
			}
			return zero, false, n.getMutateCh()
		}
		n = child
		depth++
	}
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

// Walk is used to walk the tree
func (t *RadixTree[T]) Walk(fn WalkFn[T]) {
	recursiveWalk(t.root, fn)
}

func (t *RadixTree[T]) DFS(fn DfsFn[T]) {
	t.DFSNode(t.root, fn)
}

func (t *RadixTree[T]) DFSPrintTreeUtil(node Node[T], depth int) {
	stPadding := " "
	for i := 0; i < depth*5; i++ {
		stPadding += " "
	}
	fmt.Println()
	fmt.Print(stPadding + "id -> " + strconv.Itoa(int(node.getId())) + " type -> " + strconv.Itoa(int(node.getArtNodeType())))
	fmt.Print(" key -> " + string(node.getKey()))
	fmt.Print(" partial -> " + string(node.getPartial()))
	fmt.Print(" num ch -> " + string(strconv.Itoa(int(node.getNumChildren()))))
	fmt.Print(" ch keys -> " + string(node.getKeys()))
	fmt.Print(" much -> ", node.getMutateCh())
	if node.getNodeLeaf() != nil {
		fmt.Print(" "+"optional leaf", string(node.getNodeLeaf().getKey()))
		fmt.Println(" "+"optional leaf much", node.getNodeLeaf().getMutateCh())
	}
	for _, ch := range node.getChildren() {
		if ch != nil {
			t.DFSPrintTreeUtil(ch, depth+1)
		}
	}
}

func (t *RadixTree[T]) DFSPrintTree() {
	t.DFSPrintTreeUtil(t.root, 0)
}

// recursiveWalk is used to do a pre-order walk of a node
// recursively. Returns true if the walk should be aborted
func recursiveWalk[T any](n Node[T], fn WalkFn[T]) bool {
	// Visit the leaf values if any
	if n.isLeaf() && n.getNodeLeaf() != nil && fn(getKey(n.getNodeLeaf().getKey()), n.getValue()) {
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
	for itr := 0; itr < int(n.getNumChildren()); itr++ {
		e := n.getChild(itr)
		if e != nil {
			t.DFSNode(e, fn)
		}
	}
}
