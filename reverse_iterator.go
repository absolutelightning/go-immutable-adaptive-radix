package adaptive

import (
	"bytes"
)

// ReverseIterator is used to iterate over a set of nodes
// in reverse in-order
type ReverseIterator[T any] struct {
	i *Iterator[T]

	// expandedParents stores the set of parent nodes whose relevant children have
	// already been pushed into the stack. This can happen during seek or during
	// iteration.
	//
	// Unlike forward iteration we need to recurse into children before we can
	// output the value stored in an internal leaf since all children are greater.
	// We use this to track whether we have already ensured all the children are
	// in the stack.
	expandedParents map[Node[T]]struct{}
}

// SeekPrefixWatch is used to seek the iterator to a given prefix
// and returns the watch channel of the finest granularity
func (ri *ReverseIterator[T]) SeekPrefixWatch(prefix []byte) (watch <-chan struct{}) {
	return ri.i.SeekPrefixWatch(prefix)
}

// SeekPrefix is used to seek the iterator to a given prefix
func (ri *ReverseIterator[T]) SeekPrefix(prefix []byte) {
	ri.i.SeekPrefixWatch(prefix)
}

// SeekReverseLowerBound is used to seek the iterator to the largest key that is
// lower or equal to the given key. There is no watch variant as it's hard to
// predict based on the radix structure which node(s) changes might affect the
// result.
func (ri *ReverseIterator[T]) SeekReverseLowerBound(key []byte) {
	// ri.i.node starts off in the common case as pointing to the root node of the
	// tree. By the time we return we have either found a lower bound and setup
	// the stack to traverse all larger keys, or we have not and the stack and
	// node should both be nil to prevent the iterator from assuming it is just
	// iterating the whole tree from the root node. Either way this needs to end
	// up as nil so just set it here.
	ri.i.seenMismatch = false
	ri.i.stack = make([]Node[T], 0)
	ri.i.reverseLowerBound = true
	n := ri.i.node
	ri.i.node = nil
	prefix := getTreeKey(key)
	ri.i.path = prefix
	depth := 0

	if ri.expandedParents == nil {
		ri.expandedParents = make(map[Node[T]]struct{})
	}

	found := func(n Node[T]) {
		ri.i.stack = append(
			ri.i.stack,
			n,
		)
	}

	for {
		if n == nil {
			break
		}
		// Compare current prefix with the search key's same-length prefix.
		var prefixCmp int
		if int(n.getPartialLen()) < len(prefix) {
			prefixCmp = bytes.Compare(n.getPartial()[:n.getPartialLen()], prefix[depth:depth+int(n.getPartialLen())])
		} else {
			prefixCmp = bytes.Compare(n.getPartial()[:n.getPartialLen()], prefix[depth:])
		}

		if prefixCmp < 0 {
			// Prefix is smaller than search prefix, that means there is no exact
			// match for the search key. But we are looking in reverse, so the reverse
			// lower bound will be the largest leaf under this subtree, since it is
			// the value that would come right before the current search key if it
			// were in the tree. So we need to follow the maximum path in this subtree
			// to find it. Note that this is exactly what the iterator will already do
			// if it finds a node in the stack that has _not_ been marked as expanded
			// so in this one case we don't call `found` and instead let the iterator
			// do the expansion and recursion through all the children.
			ri.i.stack = append(ri.i.stack, n)
			return
		}

		if prefixCmp > 0 && !ri.i.seenMismatch {
			// Prefix is larger than search prefix, or there is no prefix but we've
			// also exhausted the search key. Either way, that means there is no
			// reverse lower bound since nothing comes before our current search
			// prefix.
			if n.getNodeLeaf() != nil {
				ri.i.stack = append(ri.i.stack, n.getNodeLeaf())
			}
			return
		}

		// If this is a leaf, something needs to happen! Note that if it's a leaf
		// and prefixCmp was zero (which it must be to get here) then the leaf value
		// is either an exact match for the search, or it's lower. It can't be
		// greater.
		if n.isLeaf() {

			nL := n.getNodeLeaf()
			// Firstly, if it's an exact match, we're done!
			if bytes.Equal(getKey(nL.getKey()), key) {
				found(n)
				return
			}

			// It's not so this node's leaf value must be lower and could still be a
			// valid contender for reverse lower bound.

			// If it has no children then we are also done.
			if bytes.Compare(getKey(nL.getKey()), key) <= 0 {
				// This leaf is the lower bound.
				found(n)
				return
			}
		}

		// Consume the search prefix. Note that this is safe because if n.prefix is
		// longer than the search slice prefixCmp would have been > 0 above and the
		// method would have already returned.
		// Determine the child index to proceed based on the next byte of the prefix
		if n.getPartialLen() > 0 {
			// If the node has a prefix, compare it with the prefix
			mismatchIdx := prefixMismatch[T](n, prefix, len(prefix), depth)
			if mismatchIdx < int(n.getPartialLen()) && !ri.i.seenMismatch {
				// If there's a mismatch, set the node to nil to break the loop
				if n.getNodeLeaf() != nil {
					ri.i.stack = append(ri.i.stack, n.getNodeLeaf())
				}
				n = nil
				return
			}
			if mismatchIdx > 0 {
				ri.i.seenMismatch = true
			}
			depth += int(n.getPartialLen())
		}

		if depth >= len(prefix) {
			ri.i.stack = append(ri.i.stack, n)
			return
		}

		if n.getNodeLeaf() != nil {
			ri.i.stack = append(ri.i.stack, n)
		}

		idx := n.getLowerBoundCh(prefix[depth])

		if idx == -1 || depth == len(prefix)-1 {
			idx = int(n.getNumChildren()) - 1
		}

		if idx >= 0 && n.getKeyAtIdx(idx) != prefix[depth] {
			ri.i.seenMismatch = true
		}

		if ri.i.seenMismatch {
			idx = int(n.getNumChildren()) - 1
		}

		for itr := 0; itr < idx; itr++ {
			if n.getChild(itr) != nil {
				ri.i.stack = append(ri.i.stack, n.getChild(itr))
			}
		}

		if idx == -1 {
			break
		}

		// Move to the next level in the tree
		ri.expandedParents[n] = struct{}{}
		n = n.getChild(idx)
		depth++
	}

}

// Previous returns the previous node in reverse order
func (ri *ReverseIterator[T]) Previous() ([]byte, T, bool) {
	var zero T

	if ri.expandedParents == nil {
		ri.expandedParents = make(map[Node[T]]struct{})
	}

	if ri.i.stack == nil && ri.i.node != nil {
		ri.i.stack = []Node[T]{ri.i.node}
	}

	// Iterate through the stack until it's empty
	for len(ri.i.stack) > 0 {
		node := ri.i.stack[len(ri.i.stack)-1]
		ri.i.stack = ri.i.stack[:len(ri.i.stack)-1]

		if node == nil {
			return nil, zero, false
		}

		switch node.getArtNodeType() {
		case leafType:
			leafCh := node.(*NodeLeaf[T])
			if ri.i.reverseLowerBound {
				if bytes.Compare(getKey(leafCh.key), getKey(ri.i.path)) <= 0 {
					return getKey(leafCh.key), leafCh.value, true
				}
				continue
			}
			if !leafCh.matchPrefix([]byte(ri.i.Path())) {
				continue
			}
			return getKey(leafCh.key), leafCh.value, true
		case node4:
			n4 := node.(*Node4[T])
			if !ri.i.seeKPrefixWatch && n4.leaf != nil {
				if bytes.Compare(n4.leaf.key, ri.i.path) <= 0 || len(ri.i.path) == 0 {
					ri.i.stack = append(ri.i.stack, n4.leaf)
				}
			}
			_, ok := ri.expandedParents[node]
			if ok {
				continue
			}
			for itr := 0; itr < int(n4.numChildren); itr++ {
				ri.i.stack = append(ri.i.stack, n4.children[itr])
			}
			if ri.i.lowerBound && n4.leaf != nil {
				if bytes.Compare(n4.leaf.key, ri.i.path) <= 0 {
					return getKey(n4.leaf.key), n4.leaf.value, true
				}
				continue
			}
			if n4.leaf != nil && ri.i.seeKPrefixWatch && hasPrefix(getKey(n4.leaf.key), ri.i.path) {
				return getKey(n4.leaf.key), n4.leaf.value, true
			}
		case node16:
			n16 := node.(*Node16[T])
			if !ri.i.seeKPrefixWatch && n16.leaf != nil {
				if bytes.Compare(n16.leaf.key, ri.i.path) <= 0 || len(ri.i.path) == 0 {
					ri.i.stack = append(ri.i.stack, n16.leaf)
				}
			}
			_, ok := ri.expandedParents[node]
			if ok {
				continue
			}
			for itr := 0; itr < int(n16.numChildren); itr++ {
				ri.i.stack = append(ri.i.stack, n16.children[itr])
			}
			if n16.leaf != nil && ri.i.seeKPrefixWatch && hasPrefix(getKey(n16.leaf.key), ri.i.path) {
				return getKey(n16.leaf.key), n16.leaf.value, true
			}
		case node48:
			n48 := node.(*Node48[T])
			if !ri.i.seeKPrefixWatch && n48.leaf != nil {
				if bytes.Compare(n48.leaf.key, ri.i.path) <= 0 || len(ri.i.path) == 0 {
					ri.i.stack = append(ri.i.stack, n48.leaf)
				}
			}
			_, ok := ri.expandedParents[node]
			if ok {
				continue
			}
			for itr := 0; itr < 256; itr++ {
				idx := n48.keys[itr]
				if idx == 0 {
					continue
				}
				nodeCh := n48.children[idx-1]
				if nodeCh == nil {
					continue
				}
				ri.i.stack = append(ri.i.stack, nodeCh)
			}
			if n48.leaf != nil && ri.i.seeKPrefixWatch && hasPrefix(getKey(n48.leaf.key), ri.i.path) {
				return getKey(n48.leaf.key), n48.leaf.value, true
			}
		case node256:
			n256 := node.(*Node256[T])
			if !ri.i.seeKPrefixWatch && n256.leaf != nil {
				if bytes.Compare(n256.leaf.key, ri.i.path) <= 0 || len(ri.i.path) == 0 {
					ri.i.stack = append(ri.i.stack, n256.leaf)
				}
			}
			_, ok := ri.expandedParents[node]
			if ok {
				continue
			}
			for itr := 0; itr < 256; itr++ {
				nodeCh := n256.children[itr]
				if nodeCh == nil {
					continue
				}
				ri.i.stack = append(ri.i.stack, nodeCh)
			}
			if n256.leaf != nil && ri.i.seeKPrefixWatch && hasPrefix(getKey(n256.leaf.key), ri.i.path) {
				return getKey(n256.leaf.key), n256.leaf.value, true
			}
		}
	}
	return nil, zero, false
}
