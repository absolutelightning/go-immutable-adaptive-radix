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

func (t *RadixTree[T]) Clone() *RadixTree[T] {
	nt := &RadixTree[T]{
		root:      t.root.clone(true),
		size:      t.size,
		maxNodeId: t.maxNodeId,
	}
	return nt
}

// Len is used to return the number of elements in the tree
func (t *RadixTree[T]) Len() int {
	return int(t.size)
}

func (t *RadixTree[T]) GetPathIterator(path []byte) *PathIterator[T] {
	return t.root.PathIterator(path)
}

func (t *RadixTree[T]) Insert(key []byte, value T) (*RadixTree[T], T, bool) {
	txn := t.Txn()
	old, ok := txn.Insert(key, value)
	return txn.Commit(), old, ok
}

func (t *RadixTree[T]) Get(key []byte) (T, bool) {
	// iterativeSearch applies the '$' sentinel virtually, so it takes the raw
	// key directly — no per-lookup allocation, no length-bounded fast path.
	return t.iterativeSearch(key)
}

func (t *RadixTree[T]) Delete(key []byte) (*RadixTree[T], T, bool) {
	txn := t.Txn()
	old, ok := txn.Delete(key)
	return txn.Commit(), old, ok
}

func (t *RadixTree[T]) GetWatch(key []byte) (<-chan struct{}, T, bool) {
	// iterativeSearchWithWatch applies the '$' sentinel virtually, so it takes
	// the raw key directly — no per-lookup allocation.
	val, found, watch := t.iterativeSearchWithWatch(key)
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
	if n.getNodeLeaf() != nil {
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

		if n.getNodeLeaf() != nil && bytes.HasPrefix(getKey(key), getKey(n.getNodeLeaf().getKey())) {
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

func (t *RadixTree[T]) iterativeSearch(key []byte) (T, bool) {
	var zero T
	n := t.root

	if n == nil {
		return zero, false
	}

	depth := 0

	// key is the caller's raw key; stored keys carry a trailing '$', which we
	// apply virtually via terminatedByteAt / *Terminated helpers so a lookup
	// never allocates a key+'$' copy. klen is the effective (terminated) length.
	klen := len(key) + 1

	// A single type switch per level handles the whole step — prefix match,
	// key-exhaustion, and child descent — with direct field access. Going
	// through the Node[T] interface accessors, or through a second type switch
	// in findChild, cost a virtual/dispatch hop per field on this hot path.
	for {
		switch m := n.(type) {
		case *NodeLeaf[T]:
			if leafMatchesTerminated(m.key, key) {
				return m.value, true
			}
			return zero, false

		case *Node4[T]:
			if m.partialLen > 0 {
				if checkPrefixTerminated(m.partial, int(m.partialLen), key, depth) != min(maxPrefixLen, int(m.partialLen)) {
					return searchNodeLeaves(n, key)
				}
				depth += int(m.partialLen)
			}
			if depth >= klen {
				return searchNodeLeaves(n, key)
			}
			c := terminatedByteAt(key, depth)
			var child Node[T]
			for i := 0; i < int(m.numChildren); i++ {
				if m.keys[i] == c {
					child = m.children[i]
					break
				}
			}
			if child == nil {
				return searchNodeLeaves(n, key)
			}
			n = child
			depth++

		case *Node16[T]:
			if m.partialLen > 0 {
				if checkPrefixTerminated(m.partial, int(m.partialLen), key, depth) != min(maxPrefixLen, int(m.partialLen)) {
					return searchNodeLeaves(n, key)
				}
				depth += int(m.partialLen)
			}
			if depth >= klen {
				return searchNodeLeaves(n, key)
			}
			i := node16FindIdx(&m.keys, m.numChildren, terminatedByteAt(key, depth))
			if i < 0 {
				return searchNodeLeaves(n, key)
			}
			n = m.children[i]
			depth++

		case *Node48[T]:
			if m.partialLen > 0 {
				if checkPrefixTerminated(m.partial, int(m.partialLen), key, depth) != min(maxPrefixLen, int(m.partialLen)) {
					return searchNodeLeaves(n, key)
				}
				depth += int(m.partialLen)
			}
			if depth >= klen {
				return searchNodeLeaves(n, key)
			}
			idx := m.keys[terminatedByteAt(key, depth)]
			if idx == 0 {
				return searchNodeLeaves(n, key)
			}
			n = m.children[idx-1]
			depth++

		case *Node256[T]:
			if m.partialLen > 0 {
				if checkPrefixTerminated(m.partial, int(m.partialLen), key, depth) != min(maxPrefixLen, int(m.partialLen)) {
					return searchNodeLeaves(n, key)
				}
				depth += int(m.partialLen)
			}
			if depth >= klen {
				return searchNodeLeaves(n, key)
			}
			child := m.children[terminatedByteAt(key, depth)]
			if child == nil {
				return searchNodeLeaves(n, key)
			}
			n = child
			depth++

		default:
			panic("Unknown node type")
		}
	}
}

// searchNodeLeaves handles the descent-failure fallback: the (virtually
// terminated) key may terminate exactly at this node or at one of its immediate
// children, both of which can carry a node-leaf. This runs only when normal
// descent stops, so the interface accessors here are off the hot path.
func searchNodeLeaves[T any](n Node[T], key []byte) (T, bool) {
	var zero T
	if nl := n.getNodeLeaf(); nl != nil && leafMatchesTerminated(nl.getKey(), key) {
		return nl.getValue(), true
	}
	for _, ch := range n.getChildren() {
		if ch == nil {
			continue
		}
		if cl := ch.getNodeLeaf(); cl != nil && leafMatchesTerminated(cl.getKey(), key) {
			return cl.getValue(), true
		}
	}
	return zero, false
}

func (t *RadixTree[T]) iterativeSearchWithWatch(key []byte) (T, bool, <-chan struct{}) {
	var zero T
	n := t.root

	if n == nil {
		return zero, false, nil
	}

	depth := 0
	klen := len(key) + 1 // '$' sentinel applied virtually; see iterativeSearch.

	// Same single-type-switch descent as iterativeSearch, but each terminal
	// returns the mutate channel to watch: the matched leaf's channel on a hit,
	// and the channel of the node where the search stopped on a miss.
	for {
		switch m := n.(type) {
		case *NodeLeaf[T]:
			if leafMatchesTerminated(m.key, key) {
				return m.value, true, m.getMutateCh()
			}
			return zero, false, m.getMutateCh()

		case *Node4[T]:
			if m.partialLen > 0 {
				if checkPrefixTerminated(m.partial, int(m.partialLen), key, depth) != min(maxPrefixLen, int(m.partialLen)) {
					return searchNodeLeavesWatch(n, key)
				}
				depth += int(m.partialLen)
			}
			if depth >= klen {
				return searchNodeLeavesWatch(n, key)
			}
			c := terminatedByteAt(key, depth)
			var child Node[T]
			for i := 0; i < int(m.numChildren); i++ {
				if m.keys[i] == c {
					child = m.children[i]
					break
				}
			}
			if child == nil {
				return searchNodeLeavesWatch(n, key)
			}
			n = child
			depth++

		case *Node16[T]:
			if m.partialLen > 0 {
				if checkPrefixTerminated(m.partial, int(m.partialLen), key, depth) != min(maxPrefixLen, int(m.partialLen)) {
					return searchNodeLeavesWatch(n, key)
				}
				depth += int(m.partialLen)
			}
			if depth >= klen {
				return searchNodeLeavesWatch(n, key)
			}
			i := node16FindIdx(&m.keys, m.numChildren, terminatedByteAt(key, depth))
			if i < 0 {
				return searchNodeLeavesWatch(n, key)
			}
			n = m.children[i]
			depth++

		case *Node48[T]:
			if m.partialLen > 0 {
				if checkPrefixTerminated(m.partial, int(m.partialLen), key, depth) != min(maxPrefixLen, int(m.partialLen)) {
					return searchNodeLeavesWatch(n, key)
				}
				depth += int(m.partialLen)
			}
			if depth >= klen {
				return searchNodeLeavesWatch(n, key)
			}
			idx := m.keys[terminatedByteAt(key, depth)]
			if idx == 0 {
				return searchNodeLeavesWatch(n, key)
			}
			n = m.children[idx-1]
			depth++

		case *Node256[T]:
			if m.partialLen > 0 {
				if checkPrefixTerminated(m.partial, int(m.partialLen), key, depth) != min(maxPrefixLen, int(m.partialLen)) {
					return searchNodeLeavesWatch(n, key)
				}
				depth += int(m.partialLen)
			}
			if depth >= klen {
				return searchNodeLeavesWatch(n, key)
			}
			child := m.children[terminatedByteAt(key, depth)]
			if child == nil {
				return searchNodeLeavesWatch(n, key)
			}
			n = child
			depth++

		default:
			panic("Unknown node type")
		}
	}
}

// searchNodeLeavesWatch is the watch-returning counterpart of searchNodeLeaves:
// on a miss it returns the mutate channel of the node where descent stopped, so
// a caller can watch for that key appearing.
func searchNodeLeavesWatch[T any](n Node[T], key []byte) (T, bool, <-chan struct{}) {
	var zero T
	if nl := n.getNodeLeaf(); nl != nil && leafMatchesTerminated(nl.getKey(), key) {
		return nl.getValue(), true, nl.getMutateCh()
	}
	for _, ch := range n.getChildren() {
		if ch == nil {
			continue
		}
		if cl := ch.getNodeLeaf(); cl != nil && leafMatchesTerminated(cl.getKey(), key) {
			return cl.getValue(), true, cl.getMutateCh()
		}
	}
	return zero, false, n.getMutateCh()
}

func (t *RadixTree[T]) DeletePrefix(key []byte) (*RadixTree[T], bool) {
	txn := t.Txn()
	ok := txn.DeletePrefix(key)
	return txn.Commit(), ok
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
