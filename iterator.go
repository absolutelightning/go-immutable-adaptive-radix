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
	path         []byte
	node         Node[T]
	stack        []Node[T]
	depth        int
	pos          Node[T]
	seenMismatch bool
	iterPath     []byte
	stackItrSet  bool
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
			n4L := n4.leaf
			for itr := int(n4.numChildren) - 1; itr >= 0; itr-- {
				i.stack = append(i.stack, n4.children[itr])
			}
			if n4L != nil {
				return getKey(n4L.key), n4L.value, true
			}
		case *Node16[T]:
			n16 := node.(*Node16[T])
			n16L := n16.leaf
			for itr := int(n16.numChildren) - 1; itr >= 0; itr-- {
				i.stack = append(i.stack, n16.children[itr])
			}
			if n16L != nil {
				return getKey(n16L.key), n16L.value, true
			}
		case *Node48[T]:
			n48 := node.(*Node48[T])
			n48L := n48.leaf
			for itr := 255; itr >= 0; itr-- {
				idx := n48.keys[itr]
				if idx == 0 {
					continue
				}
				nodeCh := n48.children[idx-1]
				if nodeCh == nil {
					continue
				}
				i.stack = append(i.stack, nodeCh)
			}
			if n48L != nil {
				return getKey(n48L.key), n48L.value, true
			}
		case *Node256[T]:
			n256 := node.(*Node256[T])
			n256L := n256.leaf
			for itr := 255; itr >= 0; itr-- {
				nodeCh := n256.children[itr]
				if nodeCh == nil {
					continue
				}
				i.stack = append(i.stack, nodeCh)
			}
			if n256L != nil {
				return getKey(n256L.key), n256L.value, true
			}
		case *NodeLeaf[T]:
			leafCh := node.(*NodeLeaf[T])
			if !leafCh.matchPrefix([]byte(i.Path())) {
				continue
			}
			return getKey(leafCh.key), leafCh.value, true
		}
	}
	return nil, zero, false
}

func (i *Iterator[T]) SeekPrefixWatch(prefix []byte) (watch <-chan struct{}) {
	// Start from the node

	node := i.node

	i.path = prefix

	i.stack = nil
	depth := 0

	if len(prefix) == 0 {
		i.node = node
		i.stack = append(i.stack, node)
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
				if node.getNodeLeaf() != nil {
					if hasPrefix(node.getNodeLeaf().getKey(), prefix) {
						i.stack = []Node[T]{node}
						i.node = node
					} else {
						i.stack = nil
						i.node = nil
					}
					return node.getMutateCh()
				}
				minNode := minimum(node)
				if minNode != nil {
					if hasPrefix(minNode.getKey(), prefix) {
						i.stack = []Node[T]{node}
						i.node = node
					} else {
						i.stack = nil
						i.node = nil
					}
				} else {
					i.stack = []Node[T]{node}
					i.node = node
				}
				return node.getMutateCh()
			}
			depth += int(node.getPartialLen())
		}

		if depth >= len(prefix) {
			// If the prefix is exhausted, break the loop
			if node.getNodeLeaf() != nil {
				if hasPrefix(node.getNodeLeaf().getKey(), prefix) {
					i.stack = []Node[T]{node}
					i.node = node
				} else {
					i.stack = nil
					i.node = nil
				}
				return node.getMutateCh()
			}
			minNode := minimum(node)
			if minNode != nil {
				if hasPrefix(minNode.getKey(), prefix) {
					i.stack = []Node[T]{node}
					i.node = node
				} else {
					i.stack = nil
					i.node = nil
				}
			} else {
				i.stack = []Node[T]{node}
				i.node = node
			}
			return node.getMutateCh()
		}

		// Get the next child node based on the prefix
		child, _ := findChild[T](node, prefix[depth])
		if child == nil {
			// If the child node doesn't exist, break the loop
			if node.getNodeLeaf() != nil {
				if hasPrefix(node.getNodeLeaf().getKey(), prefix) {
					i.stack = []Node[T]{node}
					i.node = node
				} else {
					i.stack = nil
					i.node = nil
				}
			} else {
				i.stack = []Node[T]{node}
				i.node = node
			}
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

	if len(prefixKey) == 0 {
		i.stack = []Node[T]{node}
		return
	}

	prefix := getTreeKey(prefixKey)

	found := func(n Node[T]) {
		i.stack = append(
			i.stack,
			n,
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
			if parent != nil {
				if bytes.Compare(parent.getNodeLeaf().getKey(), prefix) >= 0 {
					i.stack = append(i.stack, parent)
				}
			} else {
				i.stack = append(i.stack, parent.getNodeLeaf())
			}
			return
		}

		var prefixCmp int
		if !node.isLeaf() {
			if int(node.getPartialLen()) < len(prefix) {
				prefixCmp = bytes.Compare(node.getPartial()[:node.getPartialLen()], prefix[depth:depth+int(node.getPartialLen())])
			} else {
				prefixCmp = bytes.Compare(node.getPartial()[:node.getPartialLen()], prefix[depth:])
			}
		}

		if prefixCmp > 0 && !i.seenMismatch {
			// Prefix is larger, that means the lower bound is greater than the search
			// and from now on we need to follow the minimum path to the smallest
			// leaf under this subtree.
			findMin(node)
			if parent != nil && parent.getNodeLeaf() != nil {
				i.stack = append(i.stack, parent.getNodeLeaf())
				return
			}
		}

		if prefixCmp < 0 && !i.seenMismatch {
			// Prefix is smaller than search prefix, that means there is no lower
			// bound
			i.node = nil
			if parent != nil && parent.getNodeLeaf() != nil {
				i.stack = append(i.stack, parent.getNodeLeaf())
			}
			return
		}

		if node.isLeaf() && node.getNodeLeaf() != nil && bytes.Compare(node.getNodeLeaf().getKey(), prefix) >= 0 {
			found(node)
			if parent != nil && parent.getNodeLeaf() != nil {
				i.stack = append(i.stack, parent.getNodeLeaf())
			}
			return
		}

		// Determine the child index to proceed based on the next byte of the prefix
		// If the node has a prefix, compare it with the prefix
		if node.getArtNodeType() != leafType {
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
			i.stack = append(i.stack, node)
			if parent != nil && parent.getNodeLeaf() != nil {
				i.stack = append(i.stack, parent.getNodeLeaf())
				return
			}
			return
		}

		idx := node.getLowerBoundCh(prefix[depth])

		if idx >= 0 && node.getKeyAtIdx(idx) != prefix[depth] {
			i.seenMismatch = true
		}

		if i.seenMismatch {
			idx = 0
		}

		if i.seenMismatch {
			for itr := int(node.getNumChildren()) - 1; itr >= idx; itr-- {
				if node.getChild(itr) != nil {
					i.stack = append(i.stack, node.getChild(itr))
				}
			}
			if node.getNodeLeaf() != nil {
				i.stack = append(i.stack, node.getNodeLeaf())
			}
			return
		}

		for itr := int(node.getNumChildren()) - 1; itr >= idx+1; itr-- {
			if node.getChild(itr) != nil {
				i.stack = append(i.stack, node.getChild(itr))
			}
		}

		if parent != nil && parent.getNodeLeaf() != nil {
			i.stack = append(i.stack, parent)
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
