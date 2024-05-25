// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

import (
	"bytes"
	"github.com/hashicorp/golang-lru/v2/simplelru"
)

const defaultModifiedCache = 8192

type Txn[T any] struct {
	tree *RadixTree[T]

	size uint64

	// snap is a snapshot of the node node for use if we have to run the
	// slow notify algorithm.
	snap Node[T]

	// trackChannels is used to hold channels that need to be notified to
	// signal mutation of the tree. This will only hold up to
	// defaultModifiedCache number of entries, after which we will set the
	// trackOverflow flag, which will cause us to use a more expensive
	// algorithm to perform the notifications. Mutation tracking is only
	// performed if trackMutate is true.
	//trackIds      map[uint64]struct{}
	trackOverflow bool
	trackMutate   bool

	// writable is a cache of writable nodes that have been created during
	// the course of the transaction. This allows us to re-use the same
	// nodes for further writes and avoid unnecessary copies of nodes that
	// have never been exposed outside the transaction. This will only hold
	// up to defaultModifiedCache number of entries.
	writable *simplelru.LRU[Node[T], any]
}

// Txn starts a new transaction that can be used to mutate the tree
func (t *RadixTree[T]) Txn() *Txn[T] {
	treeClone := t.Clone(false)
	txn := &Txn[T]{
		size: t.size,
		tree: treeClone,
	}
	return txn
}

// Clone makes an independent copy of the transaction. The new transaction
// does not track any nodes and has TrackMutate turned off. The cloned transaction will contain any uncommitted writes in the original transaction but further mutations to either will be independent and result in different radix trees on Commit. A cloned transaction may be passed to another goroutine and mutated there independently however each transaction may only be mutated in a single thread.
func (t *Txn[T]) Clone() *Txn[T] {
	// reset the writable node cache to avoid leaking future writes into the clone

	txn := &Txn[T]{
		tree: t.tree.Clone(false),
		size: t.size,
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
	res, found, _ := t.tree.Get(k)
	return res, found
}

func (t *Txn[T]) Insert(key []byte, value T) (T, bool) {
	var old int
	oldRootCh := t.tree.root.getMutateCh()
	newRoot, oldVal := t.recursiveInsert(t.tree.root, getTreeKey(key), value, 0, &old)
	if old == 0 {
		t.size++
		t.tree.size++
	}
	if t.trackMutate {
		newRoot.setMutateCh(oldRootCh)
		t.trackId(t.tree.root)
	}
	t.tree.root = newRoot
	return oldVal, old == 1
}

func (t *Txn[T]) recursiveInsert(node Node[T], key []byte, value T, depth int, old *int) (Node[T], T) {
	var zero T

	if t.trackMutate {
		t.trackId(node)
	}

	// If we are at a nil node, inject a leaf
	if node == nil {
		return t.makeLeaf(key, value), zero
	}

	node.incrementRefCount()

	if node.isLeaf() {
		// This means node is nil
		if node.getKeyLen() == 0 {
			return t.makeLeaf(key, value), zero
		}
	}

	// If we are at a leaf, we need to replace it with a node
	if node.isLeaf() {
		// Check if we are updating an existing value
		nodeKey := node.getKey()
		if len(key) == len(nodeKey) && bytes.Equal(nodeKey, key) {
			*old = 1
			if node.decrementRefCount() > 0 {
				t.tree.idg.delChns[node.getMutateCh()] = struct{}{}
				return t.makeLeaf(key, value), node.getValue()
			}
			oldVal := node.getValue()
			node.setValue(value)
			return node, oldVal
		}

		// New value, we must split the leaf into a node4
		newLeaf2 := t.makeLeaf(key, value)

		if t.trackMutate {
			t.trackId(node)
		}

		// Determine longest prefix
		longestPrefix := longestCommonPrefix[T](node, newLeaf2, depth)
		newNode := t.allocNode(node4)
		newNode.setPartialLen(uint32(longestPrefix))
		copy(newNode.getPartial()[:], key[depth:depth+min(maxPrefixLen, longestPrefix)])

		if len(node.getKey()) > depth+longestPrefix {
			// Add the leafs to the new node4
			newNode = t.addChild(newNode, node.getKey()[depth+longestPrefix], node)
		}

		if len(newLeaf2.getKey()) > depth+longestPrefix {
			newNode = t.addChild(newNode, newLeaf2.getKey()[depth+longestPrefix], newLeaf2)
		}

		return newNode, zero
	}

	// Check if given node has a prefix
	if node.getPartialLen() > 0 {
		// Determine if the prefixes differ, since we need to split
		prefixDiff := prefixMismatch[T](node, key, len(key), depth)
		if prefixDiff >= int(node.getPartialLen()) {
			depth += int(node.getPartialLen())
			child, idx := t.findChild(node, key[depth])
			if child != nil {
				newChild, val := t.recursiveInsert(child, key, value, depth+1, old)
				if t.trackMutate {
					t.trackId(child)
				}
				if node.decrementRefCount() > 0 {
					if t.trackMutate {
						t.trackId(node)
					}
					node = node.clone(false, false)
				}
				node.setChild(idx, newChild)
				return node, val
			}

			// No child, node goes within us
			newLeaf := t.makeLeaf(key, value)
			if node.decrementRefCount() > 0 {
				if t.trackMutate {
					t.trackId(node)
				}
				node = node.clone(false, false)
			}
			node = t.addChild(node, key[depth], newLeaf)
			return node, zero
		}

		// Create a new node
		newNode := t.allocNode(node4)
		newNode.setPartialLen(uint32(prefixDiff))
		copy(newNode.getPartial()[:], node.getPartial()[:min(maxPrefixLen, prefixDiff)])

		// Adjust the prefix of the old node
		if node.getPartialLen() <= maxPrefixLen {
			newNode = t.addChild(newNode, node.getPartial()[prefixDiff], node)
			node.setPartialLen(node.getPartialLen() - uint32(prefixDiff+1))
			length := min(maxPrefixLen, int(node.getPartialLen()))
			copy(node.getPartial(), node.getPartial()[prefixDiff+1:prefixDiff+1+length])
		} else {
			if t.trackMutate {
				t.tree.idg.delChns[node.getMutateCh()] = struct{}{}
			}
			node.setPartialLen(node.getPartialLen() - uint32(prefixDiff+1))
			l := minimum[T](node)
			newNode = t.addChild(newNode, l.key[depth+prefixDiff], node)
			length := min(maxPrefixLen, int(node.getPartialLen()))
			copy(node.getPartial(), l.key[depth+prefixDiff+1:depth+prefixDiff+1+length])
		}

		// Insert the new leaf
		newLeaf := t.makeLeaf(key, value)
		newNode = t.addChild(newNode, key[depth+prefixDiff], newLeaf)
		return newNode, zero
	}

	if depth < len(key) {
		// Find a child to recurse to
		child, idx := t.findChild(node, key[depth])
		if child != nil {
			newChild, val := t.recursiveInsert(child, key, value, depth+1, old)
			t.tree.idg.delChns[node.getMutateCh()] = struct{}{}
			t.tree.idg.delChns[child.getMutateCh()] = struct{}{}
			if node.decrementRefCount() > 0 {
				if t.trackMutate {
					t.trackId(node)
				}
				node = node.clone(false, false)
			}
			node.setChild(idx, newChild)
			return node, val
		}
	}

	// No child, node goes within us
	newLeaf := t.makeLeaf(key, value)
	if depth < len(key) {
		return t.addChild(node, key[depth], newLeaf), zero
	}
	if node.decrementRefCount() > 0 {
		if t.trackMutate {
			t.trackId(node)
		}
		node = node.clone(false, false)
	}
	return node, zero
}

func (t *Txn[T]) Delete(key []byte) (T, bool) {
	var zero T
	oldRootCh := t.tree.root.getMutateCh()
	newRoot, l := t.recursiveDelete(t.tree.root, getTreeKey(key), 0)
	if newRoot == nil {
		newRoot = t.allocNode(leafType)
		newRoot.setMutateCh(make(chan struct{}))
	}
	if l != nil {
		if t.trackMutate {
			t.trackId(t.tree.root)
		}
		t.size--
		t.tree.size--
		old := l.getValue()
		newRoot.setMutateCh(oldRootCh)
		t.tree.root = newRoot
		return old, true
	}
	newRoot.setMutateCh(oldRootCh)
	t.tree.root = newRoot
	return zero, false
}

func (t *Txn[T]) recursiveDelete(node Node[T], key []byte, depth int) (Node[T], Node[T]) {
	// Get terminated
	if node == nil {
		return nil, nil
	}

	node.incrementRefCount()

	if t.trackMutate {
		t.trackId(node)
	}

	// Handle hitting a leaf node
	if isLeaf[T](node) {
		if leafMatches(node.getKey(), key) == 0 {
			t.tree.idg.delChns[node.getMutateCh()] = struct{}{}
			return nil, node
		}
		t.tree.idg.delChns[node.getMutateCh()] = struct{}{}
		node = node.clone(false, false)
		return node, nil
	}

	// Bail if the prefix does not match
	if node.getPartialLen() > 0 {
		prefixLen := checkPrefix(node.getPartial(), int(node.getPartialLen()), key, depth)
		if prefixLen != min(maxPrefixLen, int(node.getPartialLen())) {
			return node, nil
		}
		depth += int(node.getPartialLen())
	}

	// Find child node
	child, idx := t.findChild(node, key[depth])
	if child == nil {
		return nil, nil
	}

	// Recurse
	newChild, val := t.recursiveDelete(child, key, depth+1)
	if val != nil {
		if t.trackMutate {
			t.trackId(node)
		}
		t.tree.idg.delChns[node.getMutateCh()] = struct{}{}
		node = node.clone(false, false)
		node.setChild(idx, newChild)
		if newChild == nil {
			if t.trackMutate {
				t.trackId(node)
				t.tree.idg.delChns[node.getMutateCh()] = struct{}{}
			}
			t.tree.idg.delChns[child.getMutateCh()] = struct{}{}
			node = t.removeChild(node, key[depth])
		}
	}
	t.tree.idg.delChns[node.getMutateCh()] = struct{}{}
	node = node.clone(false, false)
	return node, val
}

func (t *Txn[T]) Root() Node[T] {
	return t.tree.root
}

// GetWatch is used to lookup a specific key, returning
// the watch channel, value and if it was found
func (t *Txn[T]) GetWatch(k []byte) (<-chan struct{}, T, bool) {
	res, found, watch := t.tree.Get(k)
	return watch, res, found
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
	nt := &RadixTree[T]{t.tree.root,
		t.size,
		t.tree.idg,
	}
	t.writable = nil
	return nt

}

// slowNotify does a complete comparison of the before and after trees in order
// to trigger notifications. This doesn't require any additional state but it
// is very expensive to compute.
func (t *Txn[T]) slowNotify() {
	// isClosed returns true if the given channel is closed.
	isClosed := func(ch <-chan struct{}) bool {
		select {
		case _, ok := <-ch:
			return !ok // If `ok` is false, the channel is closed.
		default:
			return false // The channel is not closed.
		}
	}

	for ch := range t.tree.idg.delChns {
		if ch != nil {
			if !isClosed(ch) {
				close(ch)
			}
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
			t.trackId(t.tree.root)
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
		if bytes.HasPrefix(getKey(node.getKey()), getKey(key)) {
			if t.trackMutate {
				t.tree.idg.delChns[node.getMutateCh()] = struct{}{}
			}
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

	if t.trackMutate {
		t.trackId(node)
	}

	numDel := 0

	// Recurse on the children
	var newChIndxMap = make(map[int]Node[T])
	for idx, ch := range node.getChildren() {
		if ch != nil {
			newCh, del := t.deletePrefix(ch, key, depth+1)
			newChIndxMap[idx] = newCh
			numDel += del
			if del > 0 && t.trackMutate {
				t.trackId(ch)
			}
		}
	}

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
	l.setMutateCh(make(chan struct{}))
	return l
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
	id, ch := t.tree.idg.GenerateID()
	n.setId(id)
	n.setMutateCh(ch)
	if !n.isLeaf() {
		n.setPartial(make([]byte, maxPrefixLen))
		n.setPartialLen(maxPrefixLen)
	}
	return n
}

// trackId safely attempts to track the given mutation channel, setting the
// overflow flag if we can no longer track any more. This limits the amount of
// state that will accumulate during a transaction and we have a slower algorithm
// to switch to if we overflow.
func (t *Txn[T]) trackId(node Node[T]) {
	// In overflow, make sure we don't store any more objects.
	// If this would overflow the state we reject it and set the flag (since

	// Create the map on the fly when we need it.
	//if t.trackIds == nil {
	//	t.trackIds = make(map[uint64]struct{})
	//}
	if t.trackMutate {
		if _, ok := t.tree.idg.trackIds[node.getId()]; !ok {
			t.tree.idg.delChns[node.getMutateCh()] = struct{}{}
			node.createNewMutateChn()
		}
	}
}

// findChild finds the child node pointer based on the given character in the ART tree node.
func (t *Txn[T]) findChild(n Node[T], c byte) (Node[T], int) {
	return findChild(n, c)
}
