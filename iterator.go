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
	path       []byte
	node       Node[T]
	stack      []Node[T]
	depth      int
	pos        Node[T]
	lowerBound bool
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

	if len(i.stack) == 0 {
		return nil, zero, false
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
		i.path = append(i.path, currentNode.getPartial()...)
		switch currentNode.getArtNodeType() {
		case leafType:
			leafCh := currentNode.(*NodeLeaf[T])
			if i.lowerBound {
				i.pos = leafCh
				i.path = leafCh.key
				return getKey(leafCh.key), leafCh.value, true
			}
			if !leafCh.matchPrefix(i.path) {
				continue
			}
			i.pos = leafCh
			i.path = leafCh.key
			return getKey(leafCh.key), leafCh.value, true
		case node4:
			n4 := currentNode.(*Node4[T])
			for itr := 3; itr >= 0; itr-- {
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
			for itr := 15; itr >= 0; itr-- {
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
			for itr := 0; itr < 256; itr++ {
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
			for itr := 255; itr >= 0; itr-- {
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
	i.path = []byte{}
	return nil, zero, false
}

func (i *Iterator[T]) SeekPrefixWatch(prefixKey []byte) (watch <-chan struct{}) {
	// Start from the node node
	node := i.node
	watch = node.getMutateCh()

	prefix := getTreeKey(prefixKey)

	i.path = prefix

	i.stack = nil
	depth := 0

	for {
		// Check if the node matches the prefix
		i.stack = []Node[T]{node}
		i.node = node

		if node.isLeaf() {
			return
		}

		// Determine the child index to proceed based on the next byte of the prefix
		if node.getPartialLen() > 0 {
			// If the node has a prefix, compare it with the prefix
			mismatchIdx := prefixMismatch[T](node, prefix, len(prefix), depth)
			if mismatchIdx < int(node.getPartialLen()) {
				// If there's a mismatch, set the node to nil to break the loop
				node = nil
				break
			}
			depth += int(node.getPartialLen())
		}

		// Get the next child node based on the prefix
		child, _ := findChild[T](node, prefix[depth])
		if child == nil {
			// If the child node doesn't exist, break the loop
			node = nil
			break
		}

		if depth == len(prefix)-1 {
			// If the prefix is exhausted, break the loop
			break
		}

		// Move to the next level in the tree
		node = child
		watch = node.getMutateCh()
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
	nEdges := n.getNumChildren()
	if nEdges > 1 {
		// Add all the other edges to the stack (the min node will be added as
		// we recurse)
		i.stack = append(i.stack, n.getChildren()[1:]...)
	}
	if nEdges > 0 {
		return i.recurseMin(n.getChildren()[0])
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
		n = i.recurseMin(n)
		if n != nil {
			found(n)
			return
		}
	}

	i.path = prefix

	depth := 0
	i.node = node

	for {
		// Check if the node matches the prefix

		var prefixCmp int
		if int(node.getPartialLen()) < len(prefix) {
			prefixCmp = bytes.Compare(node.getPartial()[:node.getPartialLen()], prefix[depth:depth+int(node.getPartialLen())])
		} else {
			prefixCmp = bytes.Compare(node.getPartial()[:node.getPartialLen()], prefix)
		}

		if prefixCmp > 0 {
			// Prefix is larger, that means the lower bound is greater than the search
			// and from now on we need to follow the minimum path to the smallest
			// leaf under this subtree.
			findMin(node)
			return
		}

		if prefixCmp < 0 {
			// Prefix is smaller than search prefix, that means there is no lower
			// bound
			i.node = nil
			return
		}

		if node.isLeaf() {
			if bytes.Compare(node.getKey(), prefix) >= 0 {
				found(node)
			}
			return
		}

		// Determine the child index to proceed based on the next byte of the prefix
		if node.getPartialLen() > 0 {
			// If the node has a prefix, compare it with the prefix
			mismatchIdx := prefixMismatch[T](node, prefix, len(prefix), depth)
			if mismatchIdx < int(node.getPartialLen()) {
				// If there's a mismatch, set the node to nil to break the loop
				node = nil
				break
			}
			depth += int(node.getPartialLen())
		}

		idx := node.getLowerBoundCh(prefix[depth])
		if idx == -1 {
			// If the child node doesn't exist, break the loop
			node = nil
			break
		}

		if idx+1 < int(node.getNumChildren()) {
			for itr := int(node.getNumChildren()) - 1; itr >= idx+1; itr-- {
				if node.getChild(itr) != nil {
					i.stack = append([]Node[T]{node.getChild(itr)}, i.stack...)
				}
			}
		}

		// Move to the next level in the tree
		node = node.getChild(idx)
		depth++
	}
}
