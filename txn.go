// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

import (
	"bytes"
	"strings"

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
	trackChannels map[chan struct{}]struct{}
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
		snap: treeClone.root,
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
		snap: t.snap.clone(false, false),
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
	newRoot, oldVal := t.recursiveInsert(t.tree.root, getTreeKey(key), value, 0, &old)
	if t.trackMutate {
		t.trackChannel(t.tree.root.getMutateCh())
	}
	if old == 0 {
		t.size++
	}
	t.tree.root = newRoot
	return oldVal, old == 1
}

func (t *Txn[T]) recursiveInsert(node Node[T], key []byte, value T, depth int, old *int) (Node[T], T) {
	var zero T

	// If we are at a nil node, inject a leaf
	if node == nil {
		return t.makeLeaf(key, value), zero
	}

	if node.isLeaf() {
		// This means node is nil
		if node.getKeyLen() == 0 {
			if t.trackMutate {
				t.trackChannel(node.getMutateCh())
			}
			return t.makeLeaf(key, value), zero
		}
	}

	// If we are at a leaf, we need to replace it with a node
	if node.isLeaf() {
		// Check if we are updating an existing value
		nodeKey := node.getKey()
		if len(key) == len(nodeKey) && bytes.Equal(nodeKey, key) {
			*old = 1
			if t.trackMutate {
				t.trackChannel(node.getMutateCh())
			}
			return t.makeLeaf(key, value), node.getValue()
		}

		// New value, we must split the leaf into a node4
		newLeaf2 := t.makeLeaf(key, value)

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
				newChildChClone := t.writeNode(child)
				newChild, val := t.recursiveInsert(newChildChClone, key, value, depth+1, old)
				node.setChild(idx, newChild)
				if t.trackMutate {
					t.trackChannel(node.getMutateCh())
				}
				return node, val
			}

			// No child, node goes within us
			newLeaf := t.makeLeaf(key, value)
			node = t.addChild(node, key[depth], newLeaf)
			if t.trackMutate {
				t.trackChannel(node.getMutateCh())
			}
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
			copy(node.getPartial()[:], node.getPartial()[prefixDiff+1:+prefixDiff+1+length])
		} else {
			node.setPartialLen(node.getPartialLen() - uint32(prefixDiff+1))
			l := minimum[T](node)
			if l == nil {
				return node, zero
			}
			newNode = t.addChild(newNode, l.key[depth+prefixDiff], node)
			length := min(maxPrefixLen, int(node.getPartialLen()))
			copy(node.getPartial()[:], l.key[depth+prefixDiff+1:depth+prefixDiff+1+length])
		}
		if t.trackMutate {
			t.trackChannel(node.getMutateCh())
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
			node.setChild(idx, newChild)
			if t.trackMutate {
				t.trackChannel(node.getMutateCh())
			}
			return node, val
		}
	}

	// No child, node goes within us
	newLeaf := t.makeLeaf(key, value)
	if t.trackMutate {
		t.trackChannel(node.getMutateCh())
	}
	if depth < len(key) {
		return t.addChild(node, key[depth], newLeaf), zero
	}
	return node, zero
}

func (t *Txn[T]) Delete(key []byte) (T, bool) {
	var zero T
	newRoot, l := t.recursiveDelete(t.tree.root, getTreeKey(key), 0)
	if t.trackMutate {
		t.trackChannel(t.tree.root.getMutateCh())
	}
	if newRoot == nil {
		newRoot = t.allocNode(leafType)
	}
	t.tree.root = newRoot
	if l != nil {
		t.size--
		old := l.getValue()
		return old, true
	}
	return zero, false
}

func (t *Txn[T]) recursiveDelete(node Node[T], key []byte, depth int) (Node[T], Node[T]) {
	// Get terminated
	if node == nil {
		return nil, nil
	}
	// Handle hitting a leaf node
	if isLeaf[T](node) {
		if leafMatches(node.getKey(), key) == 0 {
			if t.trackMutate {
				t.trackChannel(node.getMutateCh())
			}
			return nil, node
		}
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

	// If the child is a leaf, delete from this node
	if isLeaf[T](child) {
		if leafMatches(child.getKey(), key) == 0 {
			if t.trackMutate {
				t.trackChannel(child.getMutateCh())
			}
			nc := t.writeNode(node)
			newNode := t.removeChild(nc, key[depth])
			return t.writeNode(newNode), child
		}
		return node, nil
	}

	// Recurse
	newChild, val := t.recursiveDelete(child, key, depth+1)
	nClone := t.writeNode(node)
	nClone.setChild(idx, newChild)
	return nClone, val
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

	// If we've overflowed the tracking state we can't use it in any way and
	// need to do a full tree compare.
	if t.trackOverflow {
		// TODO Discuss
		//t.slowNotify()
	} else {
		for ch := range t.trackChannels {
			if ch == nil {
				continue
			}
			select {
			case _, ok := <-ch:
				if ok {
					close(ch)
				}
			default:
				close(ch)
			}
		}
	}

	// Clean up the tracking state so that a re-notify is safe (will trigger
	// the else clause above which will be a no-op).
	t.trackChannels = nil
	t.trackOverflow = false
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
	}
	t.writable = nil
	return nt
}

// slowNotify does a complete comparison of the before and after trees in order
// to trigger notifications. This doesn't require any additional state but it
// is very expensive to compute.
func (t *Txn[T]) slowNotify() {
	snapIter := t.snap.Iterator()
	rootIter := t.Root().Iterator()
	snapIter.Next()
	rootIter.Next()
	for snapIter.Front() != nil || rootIter.Front() != nil {
		// If we've exhausted the nodes in the old snapshot, we know
		// there's nothing remaining to notify.
		if snapIter.Front() == nil {
			return
		}
		snapElem := snapIter.Front()

		// If we've exhausted the nodes in the new node, we know we need
		// to invalidate everything that remains in the old snapshot. We
		// know from the loop condition there's something in the old
		// snapshot.
		if rootIter.Front() == nil {
			select {
			case _, ok := <-snapElem.getMutateCh():
				if ok {
					close(snapElem.getMutateCh())
				}
			default:
				close(snapElem.getMutateCh())
			}
			snapIter.Next()
			continue
		}

		// Do one string compare so we can check the various conditions
		// below without repeating the compare.
		cmp := strings.Compare(string(getKey(snapIter.GetIterPath())), string(getKey(rootIter.GetIterPath())))

		// If the snapshot is behind the node, then we must have deleted
		// this node during the transaction.
		if cmp < 0 {
			close(snapElem.getMutateCh())
			snapIter.Next()
			continue
		}

		// If the snapshot is ahead of the node, then we must have added
		// this node during the transaction.
		if cmp > 0 {
			rootIter.Next()
			continue
		}

		// If we have the same path, then we need to see if we mutated a
		// node and possibly the leaf.
		rootElem := rootIter.Front()
		if snapElem != rootElem {
			close(snapElem.getMutateCh())
		}
		snapIter.Next()
		rootIter.Next()
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
			t.trackChannel(node.getMutateCh())
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

	t.trackChannel(node.getMutateCh())

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
	return l
}

func (t *Txn[T]) writeNode(n Node[T]) Node[T] {
	if n == nil {
		return n
	}

	if t.writable == nil {
		lru, err := simplelru.NewLRU[Node[T], any](defaultModifiedCache, nil)
		if err != nil {
			panic(err)
		}
		t.writable = lru
	}
	// If this node has already been modified, we can continue to use it
	// during this transaction. We know that we don't need to track it for
	// a node update since the node is writable, but if this is for a leaf
	// update we track it, in case the initial write to this node didn't
	// update the leaf.
	if _, ok := t.writable.Get(n); ok {
		if t.trackMutate {
			t.trackChannel(n.getMutateCh())
		}
		return n
	}
	// Mark this node as being mutated.
	if t.trackMutate {
		t.trackChannel(n.getMutateCh())
	}

	// Copy the existing node. If you have set forLeafUpdate it will be
	// safe to replace this leaf with another after you get your node for
	// writing. You MUST replace it, because the channel associated with
	// this leaf will be closed when this transaction is committed.
	nc := n.clone(false, false)

	// Mark this node as writable.
	t.writable.Add(nc, nil)
	return nc
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
	n.setMutateCh(make(chan struct{}))
	n.setPartial(make([]byte, maxPrefixLen))
	n.setPartialLen(maxPrefixLen)
	return n
}

// trackChannel safely attempts to track the given mutation channel, setting the
// overflow flag if we can no longer track any more. This limits the amount of
// state that will accumulate during a transaction and we have a slower algorithm
// to switch to if we overflow.
func (t *Txn[T]) trackChannel(ch chan struct{}) {
	// In overflow, make sure we don't store any more objects.
	if t.trackOverflow {
		return
	}

	// If this would overflow the state we reject it and set the flag (since
	// we aren't tracking everything that's required any longer).
	if len(t.trackChannels) >= defaultModifiedCache {
		// Mark that we are in the overflow state
		t.trackOverflow = true

		// Clear the map so that the channels can be garbage collected. It is
		// safe to do this since we have already overflowed and will be using
		// the slow notify algorithm.
		t.trackChannels = nil
		return
	}

	// Create the map on the fly when we need it.
	if t.trackChannels == nil {
		t.trackChannels = make(map[chan struct{}]struct{})
	}

	t.trackChannels[ch] = struct{}{}
}

// findChild finds the child node pointer based on the given character in the ART tree node.
func (t *Txn[T]) findChild(n Node[T], c byte) (Node[T], int) {
	return findChild(n, c)
}
