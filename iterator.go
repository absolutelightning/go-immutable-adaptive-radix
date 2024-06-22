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

		switch node.(type) {
		case *Node4[T]:
			n4 := node.(*Node4[T])
			for itr := int(n4.numChildren) - 1; itr >= 0; itr-- {
				i.stack = append(i.stack, *n4.children[itr])
			}
			nodeLeaf := n4.getNodeLeaf()
			if nodeLeaf != nil && hasPrefix(nodeLeaf.getKey(), i.path) {
				return getKey(nodeLeaf.getKey()), nodeLeaf.getValue(), true
			}
		case *Node16[T]:
			n16 := node.(*Node16[T])
			for itr := int(n16.numChildren) - 1; itr >= 0; itr-- {
				i.stack = append(i.stack, *n16.children[itr])
			}
			nodeLeaf := n16.getNodeLeaf()
			if nodeLeaf != nil && hasPrefix(nodeLeaf.getKey(), i.path) {
				return getKey(nodeLeaf.getKey()), nodeLeaf.getValue(), true
			}
		case *Node48[T]:
			n48 := node.(*Node48[T])
			for itr := 255; itr >= 0; itr-- {
				idx := n48.keys[itr]
				if idx == 0 {
					continue
				}
				nodeCh := n48.children[idx-1]
				if nodeCh == nil {
					continue
				}
				i.stack = append(i.stack, *nodeCh)
			}
			nodeLeaf := n48.getNodeLeaf()
			if nodeLeaf != nil && hasPrefix(nodeLeaf.getKey(), i.path) {
				return getKey(nodeLeaf.getKey()), nodeLeaf.getValue(), true
			}
		case *Node256[T]:
			n256 := node.(*Node256[T])
			for itr := 255; itr >= 0; itr-- {
				nodeCh := n256.children[itr]
				if nodeCh == nil {
					continue
				}
				i.stack = append(i.stack, *nodeCh)
			}
			nodeLeaf := n256.getNodeLeaf()
			if nodeLeaf != nil && hasPrefix(nodeLeaf.getKey(), i.path) {
				return getKey(nodeLeaf.getKey()), nodeLeaf.getValue(), true
			}
		case *NodeLeaf[T]:
			leafCh := node.(*NodeLeaf[T])
			if !leafCh.matchPrefix([]byte(i.Path())) {
				continue
			}
			if hasPrefix(leafCh.key, i.path) {
				return getKey(leafCh.key), leafCh.value, true
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

		depth += int(node.getPartialLen())

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

		node = *child
		// Move to the next level in the tree
		depth++
	}
}
