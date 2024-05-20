// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

import "strings"

const defaultModifiedCache = 8192

type Txn[T any] struct {
	tree *RadixTree[T]

	size uint64

	// snap is a snapshot of the node node for use if we have to run the
	// slow notify algorithm.
	snap Node[T]
}

// Txn starts a new transaction that can be used to mutate the tree
func (t *RadixTree[T]) Txn() *Txn[T] {
	txn := &Txn[T]{
		size: t.size,
		snap: t.root,
		tree: t,
	}
	return txn
}

// Clone makes an independent copy of the transaction. The new transaction
// does not track any nodes and has TrackMutate turned off. The cloned transaction will contain any uncommitted writes in the original transaction but further mutations to either will be independent and result in different radix trees on Commit. A cloned transaction may be passed to another goroutine and mutated there independently however each transaction may only be mutated in a single thread.
func (t *Txn[T]) Clone() *Txn[T] {
	// reset the writable node cache to avoid leaking future writes into the clone

	txn := &Txn[T]{
		tree: t.tree,
		snap: t.snap,
		size: t.size,
	}
	return txn
}

// TrackMutate can be used to toggle if mutations are tracked. If this is enabled
// then notifications will be issued for affected internal nodes and leaves when
// the transaction is committed.
func (t *Txn[T]) TrackMutate(track bool) {
	t.tree.trackMutate = track
}

// Get is used to look up a specific key, returning
// the value and if it was found
func (t *Txn[T]) Get(k []byte) (T, bool) {
	res, found, _ := t.tree.Get(k)
	return res, found
}

func (t *Txn[T]) Insert(key []byte, value T) T {
	oldVal := t.tree.Insert(key, value)
	t.size = t.tree.size
	return oldVal
}

func (t *Txn[T]) Delete(key []byte) T {
	oldVal := t.tree.Delete(key)
	t.size = t.tree.size
	return oldVal
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
	if !t.tree.trackMutate {
		return
	}

	// If we've overflowed the tracking state we can't use it in any way and
	// need to do a full tree compare.
	if t.tree.trackOverflow {
		t.slowNotify()
	} else {
		for ch := range t.tree.trackChannels {
			close(ch)
		}
	}

	// Clean up the tracking state so that a re-notify is safe (will trigger
	// the else clause above which will be a no-op).
	t.tree.trackChannels = nil
	t.tree.trackOverflow = false
}

// Commit is used to finalize the transaction and return a new tree. If mutation
// tracking is turned on then notifications will also be issued.
func (t *Txn[T]) Commit() *RadixTree[T] {
	nt := t.CommitOnly()
	if t.tree.trackMutate {
		t.Notify()
	}
	return nt
}

// CommitOnly is used to finalize the transaction and return a new tree, but
// does not issue any notifications until Notify is called.
func (t *Txn[T]) CommitOnly() *RadixTree[T] {
	nt := &RadixTree[T]{t.tree.root,
		t.size,
		t.tree.trackChannels,
		t.tree.trackOverflow,
		t.tree.trackMutate}
	return nt
}

// slowNotify does a complete comparison of the before and after trees in order
// to trigger notifications. This doesn't require any additional state but it
// is very expensive to compute.
func (t *Txn[T]) slowNotify() {
	snapIter := t.snap.Iterator()
	rootIter := t.Root().Iterator()
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
			close(snapElem.getMutateCh())
			snapIter.Next()
			continue
		}

		// Do one string compare so we can check the various conditions
		// below without repeating the compare.
		cmp := strings.Compare(snapIter.Path(), rootIter.Path())

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
	newRoot, ok := t.tree.DeletePrefix(prefix)
	if ok {
		t.tree.root = newRoot
		t.size = t.tree.size
		return true
	}
	return false
}
