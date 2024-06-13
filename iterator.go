// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

import (
	"bytes"
)

// Iterator is used to iterate over a set of nodes from the node
// down to a specified path. This will iterate over the same values that
// the Node.WalkPath method will.
type Iterator[T any] struct {
	path              []byte
	node              Node[T]
	stack             []Node[T]
	stackIter         []NodeWrapper[T]
	depth             int
	pos               Node[T]
	lowerBound        bool
	reverseLowerBound bool
	seeKPrefixWatch   bool
	seenMismatch      bool
	iterPath          []byte
	stackItrSet       bool
}

type NodeWrapper[T any] struct {
	n Node[T]
	d int
}

func (i *Iterator[T]) GetIterPath() []byte {
	return i.iterPath
}

// Front returns the current node that has been iterated to.
func (i *Iterator[T]) Front() Node[T] {
	return i.pos
}

func (i *Iterator[T]) Path() string {
	return string(i.path)
}

func (i *Iterator[T]) Next() ([]byte, T, bool) {
	var zero T

	if !i.stackItrSet {
		i.stackItrSet = true
		stackIter := make([]NodeWrapper[T], 0)

		for _, ele := range i.stack {
			stackIter = append(stackIter, NodeWrapper[T]{n: ele, d: i.depth})
			i.stack = nil
			i.node = nil
		}
		if i.stack == nil && i.node != nil {
			stackIter = []NodeWrapper[T]{{i.node, i.depth}}
		}
		i.stackIter = stackIter
	}

	// Iterate through the stack until it's empty
	for len(i.stackIter) > 0 {
		nodeW := i.stackIter[len(i.stackIter)-1]
		i.stackIter = i.stackIter[:len(i.stackIter)-1]

		node := nodeW.n

		if node == nil {
			return nil, zero, false
		}

		switch node.getArtNodeType() {
		case leafType:
			leafCh := node.(*NodeLeaf[T])
			if i.lowerBound {
				if bytes.Compare(getKey(leafCh.key), getKey(i.path)) >= 0 {
					return getKey(leafCh.key), leafCh.value, true
				}
				continue
			}
			if i.seeKPrefixWatch {
				if bytes.HasPrefix(leafCh.key, i.path) {
					return leafCh.key, leafCh.value, true
				}
				continue
			}
			if !leafCh.matchPrefix([]byte(i.Path())) {
				continue
			}
			return leafCh.key, leafCh.value, true
		case node4:
			n4 := node.(*Node4[T])
			newDepth := nodeW.d
			for itr := int(n4.numChildren) - 1; itr >= 0; itr-- {
				nodeCh := n4.children[itr]
				if nodeCh == nil {
					continue
				}
				key := n4.keys[itr]
				if (newDepth < len(i.path) && i.path[newDepth] == key) || (newDepth >= len(i.path)) {
					i.stackIter = append(i.stackIter, NodeWrapper[T]{nodeCh, newDepth + int(n4.partialLen) + 1})
				}
			}
			if n4.leaf != nil && bytes.HasPrefix(n4.leaf.key, i.path) {
				return n4.leaf.key, n4.leaf.value, true
			}
		case node16:
			n16 := node.(*Node16[T])
			newDepth := nodeW.d
			for itr := int(n16.numChildren) - 1; itr >= 0; itr-- {
				nodeCh := n16.children[itr]
				if nodeCh == nil {
					continue
				}
				key := n16.keys[itr]
				if (newDepth < len(i.path) && i.path[newDepth] == key) || (newDepth >= len(i.path)) {
					i.stackIter = append(i.stackIter, NodeWrapper[T]{nodeCh, newDepth + int(n16.partialLen) + 1})
				}
			}
			if n16.leaf != nil && bytes.HasPrefix(n16.leaf.key, i.path) {
				return n16.leaf.key, n16.leaf.value, true
			}
		case node48:
			n48 := node.(*Node48[T])
			newDepth := nodeW.d
			for itr := 255; itr >= 0; itr-- {
				idx := n48.keys[itr]
				if idx == 0 {
					continue
				}
				nodeCh := n48.children[idx-1]
				if nodeCh == nil {
					continue
				}
				key := n48.keys[itr]
				if (newDepth < len(i.path) && i.path[newDepth] == key) || (newDepth >= len(i.path)) {
					i.stackIter = append(i.stackIter, NodeWrapper[T]{nodeCh, newDepth + int(n48.partialLen) + 1})
				}
			}
			if n48.leaf != nil && bytes.HasPrefix(n48.leaf.key, i.path) {
				return n48.leaf.key, n48.leaf.value, true
			}
		case node256:
			n256 := node.(*Node256[T])
			newDepth := nodeW.d + int(n256.partialLen)
			for itr := 255; itr >= 0; itr-- {
				nodeCh := n256.children[itr]
				if nodeCh == nil {
					continue
				}
				i.stackIter = append(i.stackIter, NodeWrapper[T]{nodeCh, newDepth + int(n256.partialLen) + 1})
			}
			if n256.leaf != nil && len(n256.leaf.key) >= 2 && bytes.HasPrefix(n256.leaf.key, i.path) {
				return n256.leaf.key, n256.leaf.value, true
			}
		}
	}
	return nil, zero, false
}

func (i *Iterator[T]) SeekPrefixWatch(prefixKey []byte) (watch <-chan struct{}) {
	// Start from the node
	i.seeKPrefixWatch = true

	node := i.node

	prefix := getTreeKey(prefixKey)

	i.path = prefix

	i.stack = nil
	depth := 0

	if len(prefixKey) == 0 {
		i.node = node
		i.stack = []Node[T]{node}
		return node.getMutateCh()
	}

	i.stack = []Node[T]{node}
	i.node = node

	for {
		// Check if the node matches the prefix

		// Determine the child index to proceed based on the next byte of the prefix
		if node.getPartialLen() > 0 {
			// If the node has a prefix, compare it with the prefix
			mismatchIdx := prefixMismatch[T](node, prefix, len(prefix), depth)
			if mismatchIdx < int(node.getPartialLen()) {
				// If there's a mismatch, set the node to nil to break the loop
				i.stack = nil
				i.node = node
				i.depth = depth
				return node.getMutateCh()
			}
			depth += int(node.getPartialLen())
		}

		if depth >= len(prefix) {
			// If the prefix is exhausted, break the loop
			i.stack = nil
			i.node = node
			i.depth = depth
			return node.getMutateCh()
		}

		// Get the next child node based on the prefix
		child, _ := findChild[T](node, prefix[depth])
		if child == nil {
			// If the child node doesn't exist, break the loop
			i.stack = nil
			i.node = node
			return node.getMutateCh()
		}

		i.stack = []Node[T]{node}
		i.node = node
		i.depth = depth

		node = child
		// Move to the next level in the tree
		depth++
	}
}

func (i *Iterator[T]) SeekPrefix(prefixKey []byte) {
	i.SeekPrefixWatch(prefixKey)
}

func (i *Iterator[T]) recurseMin(n Node[T]) Node[T] {
	// Traverse to the minimum child
	if n.isLeaf() {
		return n
	}
	nCh := n.getNumChildren()
	if nCh > 1 {
		// Add all the other edges to the stack (the min node will be added as
		// we recurse)
		var allCh []Node[T]
		for itr := nCh - 1; itr >= 1; itr-- {
			allCh = append(allCh, n.getChild(int(itr)))
		}
		i.stack = append(allCh, i.stack...)
	}
	if nCh > 0 {
		return i.recurseMin(n.getChild(0))
	}
	// Shouldn't be possible
	return nil
}

func (i *Iterator[T]) SeekLowerBound(prefixKey []byte) {
	node := i.node

	i.stack = []Node[T]{}
	i.lowerBound = true

	prefix := getTreeKey(prefixKey)

	found := func(n Node[T]) {
		i.stack = append(
			[]Node[T]{n},
			i.stack...,
		)
	}

	findMin := func(n Node[T]) {
		if n != nil {
			found(n)
			return
		}
		n = i.recurseMin(n)
	}

	i.path = prefix

	depth := 0
	i.node = node
	i.seenMismatch = false

	var parent Node[T]

	for {

		// Check if the node matches the prefix

		if node == nil {
			if parent != nil && parent.getNodeLeaf() != nil {
				i.stack = append([]Node[T]{parent.getNodeLeaf()}, i.stack...)
			}
			return
		}

		var prefixCmp int
		if int(node.getPartialLen()) < len(prefix) {
			prefixCmp = bytes.Compare(node.getPartial()[:node.getPartialLen()], prefix[depth:depth+int(node.getPartialLen())])
		} else {
			prefixCmp = bytes.Compare(node.getPartial()[:node.getPartialLen()], prefix[depth:])
		}

		if prefixCmp > 0 && !i.seenMismatch {
			// Prefix is larger, that means the lower bound is greater than the search
			// and from now on we need to follow the minimum path to the smallest
			// leaf under this subtree.
			findMin(node)
			if parent != nil && parent.getNodeLeaf() != nil {
				i.stack = append([]Node[T]{parent.getNodeLeaf()}, i.stack...)
			}
			return
		}

		if prefixCmp < 0 && !i.seenMismatch {
			// Prefix is smaller than search prefix, that means there is no lower
			// bound
			i.node = nil
			if parent != nil && parent.getNodeLeaf() != nil {
				i.stack = append([]Node[T]{parent.getNodeLeaf()}, i.stack...)
			}
			return
		}

		if node.isLeaf() && node.getNodeLeaf() != nil && bytes.Compare(node.getNodeLeaf().getKey(), prefix) >= 0 {
			found(node)
			if parent != nil && parent.getNodeLeaf() != nil {
				i.stack = append([]Node[T]{parent.getNodeLeaf()}, i.stack...)
			}
			return
		}

		// Determine the child index to proceed based on the next byte of the prefix
		if node.getPartialLen() > 0 {
			// If the node has a prefix, compare it with the prefix
			mismatchIdx := prefixMismatch[T](node, prefix, len(prefix), depth)
			if mismatchIdx < int(node.getPartialLen()) && !i.seenMismatch {
				// If there's a mismatch, set the node to nil to break the loop
				node = nil
				return
			}
			if mismatchIdx > 0 {
				i.seenMismatch = true
			}
			depth += int(node.getPartialLen())
		}

		if depth >= len(prefix) {
			i.stack = append([]Node[T]{node}, i.stack...)
			if parent != nil && parent.getNodeLeaf() != nil {
				i.stack = append([]Node[T]{parent.getNodeLeaf()}, i.stack...)
			}
			return
		}

		idx := node.getLowerBoundCh(prefix[depth])

		if i.seenMismatch {
			idx = 0
		}

		for itr := int(node.getNumChildren()) - 1; itr >= idx+1; itr-- {
			if node.getChild(itr) != nil {
				i.stack = append([]Node[T]{node.getChild(itr)}, i.stack...)
			}
		}

		if parent != nil && parent.getNodeLeaf() != nil {
			i.stack = append([]Node[T]{parent.getNodeLeaf()}, i.stack...)
		}

		if idx == -1 {
			node = nil
			return
		}

		parent = node
		// Move to the next level in the tree
		node = node.getChild(idx)

		depth++

	}

}
