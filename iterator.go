// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

// Iterator is used to iterate over a set of nodes from the node
// down to a specified path. This will iterate over the same values that
// the Node.WalkPath method will.
type Iterator[T any] struct {
	path         []byte
	node         Node[T]
	stack        []Node[T]
	depth        int
	pos          Node[T]
	seenMismatch bool
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

	// Iterate through the stack until it's empty
	for len(i.stack) > 0 {
		node := i.stack[len(i.stack)-1]
		i.stack = i.stack[:len(i.stack)-1]

		if node == nil {
			return nil, zero, false
		}

		// Bind the concrete node in the switch so each case skips a second type
		// assertion. Children are pushed in reverse so they pop in key order.
		switch n := node.(type) {
		case *Node4[T]:
			for itr := int(n.numChildren) - 1; itr >= 0; itr-- {
				i.stack = append(i.stack, n.children[itr])
			}
			if n.leaf != nil && hasPrefix(n.leaf.key, i.path) {
				return getKey(n.leaf.key), n.leaf.value, true
			}
		case *Node16[T]:
			for itr := int(n.numChildren) - 1; itr >= 0; itr-- {
				i.stack = append(i.stack, n.children[itr])
			}
			if n.leaf != nil && hasPrefix(n.leaf.key, i.path) {
				return getKey(n.leaf.key), n.leaf.value, true
			}
		case *Node48[T]:
			for itr := 255; itr >= 0; itr-- {
				idx := n.keys[itr]
				if idx == 0 {
					continue
				}
				if nodeCh := n.children[idx-1]; nodeCh != nil {
					i.stack = append(i.stack, nodeCh)
				}
			}
			if n.leaf != nil && hasPrefix(n.leaf.key, i.path) {
				return getKey(n.leaf.key), n.leaf.value, true
			}
		case *Node256[T]:
			for itr := 255; itr >= 0; itr-- {
				if nodeCh := n.children[itr]; nodeCh != nil {
					i.stack = append(i.stack, nodeCh)
				}
			}
			if n.leaf != nil && hasPrefix(n.leaf.key, i.path) {
				return getKey(n.leaf.key), n.leaf.value, true
			}
		case *NodeLeaf[T]:
			if hasPrefix(n.key, i.path) {
				return getKey(n.key), n.value, true
			}
		}
	}
	return nil, zero, false
}

func (i *Iterator[T]) SeekPrefixWatch(prefix []byte) (watch <-chan struct{}) {
	node := i.SeekPrefix(prefix)
	return node.getMutateCh()
}

func (i *Iterator[T]) SeekPrefix(prefix []byte) Node[T] {
	node := i.node

	i.path = prefix

	i.stack = nil
	depth := 0

	i.stack = []Node[T]{node}
	i.node = node

	for {
		// Check if the node matches the prefix

		// Determine the child index to proceed based on the next byte of the prefix
		if !node.isLeaf() && node.getPartialLen() > 0 {
			// If the node has a prefix, compare it with the prefix
			mismatchIdx := prefixMismatch[T](node, prefix, len(prefix), depth)
			if mismatchIdx < int(node.getPartialLen()) {
				// If there's a mismatch, set the node to nil to break the loop
				i.node = node
				i.stack = []Node[T]{node}
				return node
			}
			depth += int(node.getPartialLen())
		}

		if depth >= len(prefix) {
			// If the prefix is exhausted, break the loop
			i.node = node
			i.stack = []Node[T]{node}
			return node
		}

		// Get the next child node based on the prefix
		child, _ := findChild[T](node, prefix[depth])
		if child == nil {
			// If the child node doesn't exist, break the loop
			i.node = node
			i.stack = []Node[T]{node}
			return node
		}

		i.stack = []Node[T]{node}
		i.node = node
		i.depth = depth

		node = child
		// Move to the next level in the tree
		depth++
	}
}
