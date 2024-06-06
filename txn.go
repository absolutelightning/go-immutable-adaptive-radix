// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

import (
	"bytes"
)

const defaultModifiedCache = 8192

type Txn[T any] struct {
	tree *RadixTree[T]

	size uint64

	// snap is a snapshot of the node node for use if we have to run the
	// slow notify algorithm.
	snap *RadixTree[T]

	trackMutate bool

	trackChnMap map[chan struct{}]struct{}
}

func (t *Txn[T]) writeNode(n Node[T], trackCh bool) Node[T] {
	if n.getId() > t.snap.maxNodeId {
		return n
	}
	if trackCh {
		t.trackChannel(n)
		if n.getNodeLeaf() != nil {
			t.trackChannel(n.getNodeLeaf())
		}
	}
	nc := n.clone(!trackCh, false)
	t.tree.maxNodeId++
	nc.setId(t.tree.maxNodeId)
	return nc
}

// Txn starts a new transaction that can be used to mutate the tree
func (t *RadixTree[T]) Txn() *Txn[T] {
	txn := &Txn[T]{
		size: t.size,
		tree: &RadixTree[T]{
			t.root,
			t.size,
			t.maxNodeId,
		},
		snap: t,
	}
	return txn
}

// Clone makes an independent copy of the transaction. The new transaction
// does not track any nodes and has TrackMutate turned off. The cloned transaction will contain any uncommitted writes in the original transaction but further mutations to either will be independent and result in different radix trees on Commit. A cloned transaction may be passed to another goroutine and mutated there independently however each transaction may only be mutated in a single thread.
func (t *Txn[T]) Clone() *Txn[T] {
	// reset the writable node cache to avoid leaking future writes into the clone

	txn := &Txn[T]{
		tree: &RadixTree[T]{
			t.tree.root,
			t.size,
			t.tree.maxNodeId,
		},
		size: t.size,
		snap: t.tree,
	}
	return txn
}

// TrackMutate can be used to toggle if mutations are tracked. If this is enabled
// then notifications will be issued for affected internal nodes and leaves when
// the transaction is committed.
func (t *Txn[T]) TrackMutate(track bool) {
	t.trackMutate = track
}

// Get is used to look up a specific key, returning
// the value and if it was found
func (t *Txn[T]) Get(k []byte) (T, bool) {
	res, found := t.tree.Get(k)
	return res, found
}

func (t *Txn[T]) Insert(key []byte, value T) (T, bool) {
	var old int
	newRoot, oldVal, mutated := t.recursiveInsert(t.tree.root, getTreeKey(key), value, 0, &old)
	if old == 0 {
		t.size++
		t.tree.size++
	}
	if mutated {
		t.trackChannel(t.tree.root)
	}
	t.tree.root = newRoot
	return oldVal, old == 1
}

func (t *Txn[T]) recursiveInsert(node Node[T], key []byte, value T, depth int, old *int) (Node[T], T, bool) {
	var zero T

	if node.isLeaf() {
		nodeLeafStored := node.getNodeLeaf()
		if nodeLeafStored.getKeyLen() == 0 {
			t.trackChannel(nodeLeafStored)
			node = t.writeNode(node, true)
			newLeaf := t.allocNode(leafType)
			newLeaf.setKey(key)
			newLeaf.setValue(value)
			node.setNodeLeaf(newLeaf.(*NodeLeaf[T]))
			return node, zero, true
		}
	}

	// If we are at a leaf, we need to replace it with a node
	if node.isLeaf() {
		// Check if we are updating an existing value
		nodeLeafStored := node.getNodeLeaf()
		nodeKey := nodeLeafStored.getKey()
		if len(key) == len(nodeKey) && bytes.Equal(nodeKey, key) {
			*old = 1
			t.trackChannel(nodeLeafStored)
			oldVal := nodeLeafStored.getValue()
			node = t.writeNode(node, true)
			newLeaf := t.allocNode(leafType)
			newLeaf.setKey(key)
			newLeaf.setValue(value)
			node.setNodeLeaf(newLeaf.(*NodeLeaf[T]))
			return node, oldVal, true
		}

		// New value, we must split the leaf into a node4
		newLeaf2 := t.makeLeaf(key, value)
		newLeaf2L := newLeaf2.getNodeLeaf()

		nodeLeaf := node.getNodeLeaf()

		// Determine longest prefix
		longestPrefix := longestCommonPrefix[T](newLeaf2L, nodeLeaf, depth)
		newNode := t.allocNode(node4)
		newNode.setPartialLen(uint32(longestPrefix))
		copy(newNode.getPartial()[:], key[depth:depth+min(maxPrefixLen, longestPrefix)])

		if bytes.HasPrefix(getKey(nodeLeaf.getKey()), getKey(newLeaf2L.getKey())) {

			newNode.setNodeLeaf(newLeaf2L)
			newNode = t.addChild(newNode, nodeLeaf.getKey()[depth+longestPrefix], node)

		} else if bytes.HasPrefix(getKey(newLeaf2L.getKey()), getKey(nodeLeaf.getKey())) {

			newNode.setNodeLeaf(nodeLeaf)
			newNode = t.addChild(newNode, newLeaf2L.getKey()[depth+longestPrefix], newLeaf2)
			t.trackChannel(node)

		} else {
			if len(nodeLeaf.getKey()) > depth+longestPrefix {
				// Add the leafs to the new node4
				newNode = t.addChild(newNode, nodeLeaf.getKey()[depth+longestPrefix], node)
			}

			if len(newLeaf2L.getKey()) > depth+longestPrefix {
				newNode = t.addChild(newNode, newLeaf2L.getKey()[depth+longestPrefix], newLeaf2)
			}
		}

		t.trackChannel(node)
		return newNode, zero, true
	}

	if node.getNodeLeaf() != nil && leafMatches(node.getNodeLeaf().getKey(), key) == 0 {
		newLeaf := t.writeNode(node.getNodeLeaf(), true)
		newLeaf.setValue(value)
		node = t.writeNode(node, true)
		node.setNodeLeaf(newLeaf.(*NodeLeaf[T]))
		return node, zero, true
	}

	// Check if given node has a prefix
	if node.getPartialLen() > 0 {
		// Determine if the prefixes differ, since we need to split
		prefixDiff := prefixMismatch[T](node, key, len(key), depth)
		if prefixDiff >= int(node.getPartialLen()) {
			depth += int(node.getPartialLen())
			child, idx := t.findChild(node, key[depth])
			if child != nil {
				newChild, val, mutatedSubTree := t.recursiveInsert(child, key, value, depth+1, old)
				node = t.writeNode(node, true)
				node.setChild(idx, newChild)
				return node, val, mutatedSubTree
			}

			newLeaf := t.makeLeaf(key, value)
			newLeafL := newLeaf.getNodeLeaf()
			nL := node.getNodeLeaf()
			if nL != nil && nL.getKeyLen() != 0 {
				if bytes.HasPrefix(getKey(nL.getKey()), getKey(newLeafL.getKey())) {
					newNode := t.allocNode(node4)
					newNode.setNodeLeaf(newLeaf.(*NodeLeaf[T]))
					newNode = t.addChild(newNode, key[depth], node)
					return newNode, zero, true
				}
			}
			node = t.writeNode(node, true)
			// No child, node goes within us
			node = t.addChild(node, key[depth], newLeaf)
			// newNode was created
			return node, zero, true
		}

		// Create a new node
		newNode := t.allocNode(node4)
		newNode.setPartialLen(uint32(prefixDiff))
		copy(newNode.getPartial()[:], node.getPartial()[:min(maxPrefixLen, prefixDiff)])
		node = t.writeNode(node, true)

		// Adjust the prefix of the old node
		if node.getPartialLen() <= maxPrefixLen {
			newNode = t.addChild(newNode, node.getPartial()[prefixDiff], node)
			node.setPartialLen(node.getPartialLen() - uint32(prefixDiff+1))
			length := min(maxPrefixLen, int(node.getPartialLen()))
			copy(node.getPartial(), node.getPartial()[prefixDiff+1:prefixDiff+1+length])
		} else {
			node.setPartialLen(node.getPartialLen() - uint32(prefixDiff+1))
			l := minimum[T](node)
			newNode = t.addChild(newNode, l.key[depth+prefixDiff], node)
			length := min(maxPrefixLen, int(node.getPartialLen()))
			copy(node.getPartial(), l.key[depth+prefixDiff+1:depth+prefixDiff+1+length])
		}
		// Insert the new leaf
		newLeaf := t.makeLeaf(key, value)
		newNode = t.addChild(newNode, key[depth+prefixDiff], newLeaf)
		return newNode, zero, true
	}

	if depth < len(key) {
		// Find a child to recurse to
		child, idx := t.findChild(node, key[depth])
		if child != nil {
			newChild, val, mutatedSubtree := t.recursiveInsert(child, key, value, depth+1, old)
			node = t.writeNode(node, true)
			node.setChild(idx, newChild)
			return node, val, mutatedSubtree
		}
	}

	newLeaf := t.makeLeaf(key, value)
	if node.getArtNodeType() == 1 && node.getNodeLeaf() != nil && node.getNodeLeaf().getKeyLen() == 0 {
		return newLeaf, zero, false
	}
	if depth < len(key) {
		node = t.writeNode(node, true)
		return t.addChild(node, key[depth], newLeaf), zero, true
	}
	return node, zero, false
}

func (t *Txn[T]) Delete(key []byte) (T, bool) {
	var zero T
	newRoot, l, _ := t.recursiveDelete(t.tree.root, getTreeKey(key), 0)

	if newRoot == nil {
		newRoot = t.allocNode(node4)
		newRoot.setNodeLeaf(&NodeLeaf[T]{})
	}
	if l != nil {
		t.trackChannel(t.tree.root)
		t.size--
		t.tree.size--
		old := l.getValue()
		t.tree.root = newRoot
		return old, true
	}
	t.tree.root = newRoot
	return zero, false
}

func (t *Txn[T]) recursiveDelete(node Node[T], key []byte, depth int) (Node[T], Node[T], bool) {
	// Get terminated
	if node == nil {
		return nil, nil, false
	}

	// Handle hitting a leaf node
	if node.getNodeLeaf() != nil {
		nodeL := node.getNodeLeaf()
		if leafMatches(nodeL.getKey(), key) == 0 {
			t.trackChannel(nodeL)
			node = t.writeNode(node, true)
			node.setNodeLeaf(nil)
			if node.getNumChildren() > 0 {
				return node, nodeL, true
			} else {
				return nil, nodeL, false
			}
		}
	}

	// Bail if the prefix does not match
	if node.getPartialLen() > 0 {
		prefixLen := checkPrefix(node.getPartial(), int(node.getPartialLen()), key, depth)
		if prefixLen != min(maxPrefixLen, int(node.getPartialLen())) {
			return node, nil, false
		}
		depth += int(node.getPartialLen())
	}

	// Find child node
	child, idx := t.findChild(node, key[depth])
	if child == nil {
		return nil, nil, false
	}

	// Recurse
	newChild, val, mutate := t.recursiveDelete(child, key, depth+1)

	if newChild != child || val != nil {
		node = t.writeNode(node, mutate)
		if !mutate {
			t.trackChannel(node)
		}
		node.setChild(idx, newChild)
		if newChild == nil {
			node = t.removeChild(node, key[depth])
		}
	}

	return node, val, mutate
}

func (t *Txn[T]) Root() Node[T] {
	return t.tree.root
}

// GetWatch is used to lookup a specific key, returning
// the watch channel, value and if it was found
func (t *Txn[T]) GetWatch(k []byte) (<-chan struct{}, T, bool) {
	return t.tree.GetWatch(k)
}

// Notify is used along with TrackMutate to trigger notifications. This must
// only be done once a transaction is committed via CommitOnly, and it is called
// automatically by Commit.
func (t *Txn[T]) Notify() {
	if !t.trackMutate {
		return
	}

	t.slowNotify()
}

// Commit is used to finalize the transaction and return a new tree. If mutation
// tracking is turned on then notifications will also be issued.
func (t *Txn[T]) Commit() *RadixTree[T] {
	nt := t.CommitOnly()
	if t.trackMutate {
		t.Notify()
	}
	return nt
}

// CommitOnly is used to finalize the transaction and return a new tree, but
// does not issue any notifications until Notify is called.
func (t *Txn[T]) CommitOnly() *RadixTree[T] {
	if t.tree.root == nil {
		t.tree.root = &Node4[T]{
			leaf: &NodeLeaf[T]{},
			id:   0,
		}
	}
	nt := &RadixTree[T]{t.tree.root,
		t.size,
		t.tree.maxNodeId,
	}
	return nt

}

// slowNotify does a complete comparison of the before and after trees in order
// to trigger notifications. This doesn't require any additional state but it
// is very expensive to compute.
func (t *Txn[T]) slowNotify() {
	// isClosed returns true if the given channel is closed.
	for ch := range t.trackChnMap {
		if ch != nil && !isClosed(ch) {
			close(ch)
		}
	}
}

func (t *Txn[T]) LongestPrefix(prefix []byte) ([]byte, T, bool) {
	return t.tree.LongestPrefix(prefix)
}

// DeletePrefix is used to delete an entire subtree that matches the prefix
// This will delete all nodes under that prefix
func (t *Txn[T]) DeletePrefix(prefix []byte) bool {
	key := getTreeKey(prefix)
	newRoot, numDeletions := t.deletePrefix(t.tree.root, key, 0)
	if numDeletions != 0 {
		if t.trackMutate {
			t.trackChannel(t.tree.root)
		}
		t.tree.root = newRoot
		t.tree.size = t.tree.size - uint64(numDeletions)
		t.size = t.tree.size
		return true
	}
	return false
}

func (t *Txn[T]) deletePrefix(node Node[T], key []byte, depth int) (Node[T], int) {
	// Get terminated
	if node == nil {
		return nil, 0
	}
	// Handle hitting a leaf node
	if isLeaf[T](node) {
		nL := node.getNodeLeaf()
		if nL != nil && bytes.HasPrefix(getKey(nL.getKey()), getKey(key)) {
			t.trackChannel(node)
			t.trackChannel(nL)
			return nil, 1
		}
		return node, 0
	}

	// Bail if the prefix does not match
	if node.getPartialLen() > 0 {
		prefixLen := checkPrefix(node.getPartial(), int(node.getPartialLen()), key, depth)
		if prefixLen < min(maxPrefixLen, len(getKey(key))) {
			depth += prefixLen
		}
	}

	numDel := 0

	if node.getNodeLeaf() != nil {
		if bytes.HasPrefix(getKey(node.getNodeLeaf().getKey()), getKey(key)) {
			t.trackChannel(node.getNodeLeaf())
			numDel++
		}
	}

	// Recurse on the children
	var newChIndxMap = make(map[int]Node[T])
	for idx, ch := range node.getChildren() {
		if ch != nil {
			newCh, del := t.deletePrefix(ch, key, depth+1)
			newChIndxMap[idx] = newCh
			numDel += del
			if del > 0 && t.trackMutate {
				t.trackChannel(ch)
			}
		}
	}

	node = t.writeNode(node, true)

	for idx, ch := range newChIndxMap {
		node.setChild(idx, ch)
	}

	return node, numDel
}

func (t *Txn[T]) makeLeaf(key []byte, value T) Node[T] {
	// Allocate memory for the leaf node
	l := t.allocNode(leafType)

	if l == nil {
		return nil
	}

	// Set the value and key length
	l.setValue(value)
	l.setKeyLen(uint32(len(key)))
	l.setKey(key)

	n4 := t.allocNode(node4)
	n4.setNodeLeaf(l.(*NodeLeaf[T]))
	return n4
}

func (t *Txn[T]) allocNode(ntype nodeType) Node[T] {
	var n Node[T]
	switch ntype {
	case leafType:
		n = &NodeLeaf[T]{}
	case node4:
		n = &Node4[T]{}
	case node16:
		n = &Node16[T]{}
	case node48:
		n = &Node48[T]{}
	case node256:
		n = &Node256[T]{}
	default:
		panic("Unknown node type")
	}
	t.tree.maxNodeId++
	n.setId(t.tree.maxNodeId)
	if n.getArtNodeType() != leafType {
		n.setPartial(make([]byte, maxPrefixLen))
		n.setPartialLen(maxPrefixLen)
	}
	return n
}

// trackChannel safely attempts to track the given mutation channel, setting the
// overflow flag if we can no longer track any more. This limits the amount of
// state that will accumulate during a transaction and we have a slower algorithm
// to switch to if we overflow.
func (t *Txn[T]) trackChannel(node Node[T]) {
	// In overflow, make sure we don't store any more objects.
	// If this would overflow the state we reject it and set the flag (since

	if !t.trackMutate {
		return
	}

	// Create the map on the fly when we need it.
	if node == nil {
		return
	}

	ch := node.getMutateCh()
	if t.trackChnMap == nil {
		t.trackChnMap = make(map[chan struct{}]struct{})
	}
	t.trackChnMap[ch] = struct{}{}

	node.setMutateCh(make(chan struct{}))
}

// isClosed returns true if the given channel is closed.
func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// findChild finds the child node pointer based on the given character in the ART tree node.
func (t *Txn[T]) findChild(n Node[T], c byte) (Node[T], int) {
	return findChild(n, c)
}
