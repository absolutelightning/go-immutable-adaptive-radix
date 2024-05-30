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
	depth             int
	pos               Node[T]
	lowerBound        bool
	reverseLowerBound bool
	seenMismatch      bool
	iterPath          []byte
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

	if i.stack == nil && i.node != nil {
		i.stack = []Node[T]{i.node}
	}

	// Iterate through the stack until it's empty
	for len(i.stack) > 0 {
		node := i.stack[0]
		i.stack = i.stack[1:]

		if node == nil {
			return nil, zero, false
		}

		currentNode := node.(Node[T])

		i.pos = currentNode
		switch currentNode.getArtNodeType() {
		case leafType:
			leafCh := currentNode.(*NodeLeaf[T])
			if i.lowerBound {
				if bytes.Compare(getKey(leafCh.key), getKey(i.path)) >= 0 {
					return getKey(leafCh.key), leafCh.value, true
				}
				continue
			}
			if len(i.Path()) >= 2 && !leafCh.matchPrefix([]byte(i.Path())) {
				continue
			}
			return getKey(leafCh.key), leafCh.value, true
		case node4:
			n4 := currentNode.(*Node4[T])
			for itr := int(n4.getNumChildren() - 1); itr >= 0; itr-- {
				nodeCh := n4.children[itr]
				if nodeCh == nil {
					continue
				}
				child := (n4.children[itr]).(Node[T])
				newStack := make([]Node[T], len(i.stack)+1)
				copy(newStack[1:], i.stack)
				newStack[0] = child
				i.stack = newStack
			}
		case node16:
			n16 := currentNode.(*Node16[T])
			for itr := int(n16.getNumChildren() - 1); itr >= 0; itr-- {
				nodeCh := n16.children[itr]
				if nodeCh == nil {
					continue
				}
				child := (nodeCh).(Node[T])
				newStack := make([]Node[T], len(i.stack)+1)
				copy(newStack[1:], i.stack)
				newStack[0] = child
				i.stack = newStack
			}
		case node48:
			n48 := currentNode.(*Node48[T])
			for itr := 0; itr < int(n48.getNumChildren()-1); itr++ {
				idx := n48.keys[itr]
				if idx == 0 {
					continue
				}
				nodeCh := n48.children[idx-1]
				if nodeCh == nil {
					continue
				}
				child := (nodeCh).(Node[T])
				newStack := make([]Node[T], len(i.stack)+1)
				copy(newStack[1:], i.stack)
				newStack[0] = child
				i.stack = newStack
			}
		case node256:
			n256 := currentNode.(*Node256[T])
			for itr := n256.getNumChildren() - 1; itr >= 0; itr-- {
				nodeCh := n256.children[itr]
				if nodeCh == nil {
					continue
				}
				child := (n256.children[itr]).(Node[T])
				newStack := make([]Node[T], len(i.stack)+1)
				copy(newStack[1:], i.stack)
				newStack[0] = child
				i.stack = newStack
			}
		}
	}
	i.pos = nil
	return nil, zero, false
}

func (i *Iterator[T]) SeekPrefixWatch(prefixKey []byte) (watch <-chan struct{}) {
	// Start from the node

	node := i.node
	watch = node.getMutateCh()

	prefix := getTreeKey(prefixKey)

	i.path = prefix

	i.stack = nil
	depth := 0

	if prefixKey == nil {
		i.node = node
		i.stack = []Node[T]{node}
		return watch
	}

	i.stack = []Node[T]{node}
	i.node = node

	for {
		// Check if the node matches the prefix

		if node.isLeaf() {
			return watch
		}

		// Determine the child index to proceed based on the next byte of the prefix
		if node.getPartialLen() > 0 {
			// If the node has a prefix, compare it with the prefix
			mismatchIdx := prefixMismatch[T](node, prefix, len(prefix), depth)
			if mismatchIdx < int(node.getPartialLen()) {
				// If there's a mismatch, set the node to nil to break the loop
				i.node = nil
				break
			}
			depth += int(node.getPartialLen())
		}

		// Get the next child node based on the prefix
		child, _ := findChild[T](node, prefix[depth])
		if child == nil {
			// If the child node doesn't exist, break the loop
			node = nil
			i.node = nil
			break
		}

		if depth == len(prefix) {
			// If the prefix is exhausted, break the loop
			i.node = nil
			break
		}

		i.stack = []Node[T]{node}
		i.node = node

		// Move to the next level in the tree
		watch = node.getMutateCh()
		node = child
		depth++
	}
	return watch
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

	for {
		// Check if the node matches the prefix

		if node == nil {
			break
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
			return
		}

		if prefixCmp < 0 && !i.seenMismatch {
			// Prefix is smaller than search prefix, that means there is no lower
			// bound
			i.node = nil
			return
		}

		if node.isLeaf() && bytes.Compare(node.getKey(), prefix) >= 0 {
			found(node)
			return
		}

		// Determine the child index to proceed based on the next byte of the prefix
		if node.getPartialLen() > 0 {
			// If the node has a prefix, compare it with the prefix
			mismatchIdx := prefixMismatch[T](node, prefix, len(prefix), depth)
			if mismatchIdx < int(node.getPartialLen()) && !i.seenMismatch {
				// If there's a mismatch, set the node to nil to break the loop
				node = nil
				break
			}
			if mismatchIdx > 0 {
				i.seenMismatch = true
			}
			depth += int(node.getPartialLen())
		}

		if depth >= len(prefix) {
			i.stack = append([]Node[T]{node}, i.stack...)
			break
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

		if idx == -1 {
			node = nil
			break
		}

		// Move to the next level in the tree
		node = node.getChild(idx)
		depth++
	}
}
