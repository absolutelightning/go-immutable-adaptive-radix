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
	stack             []NodeWrapper[T]
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

	if i.stack == nil && i.node != nil {
		i.stack = []NodeWrapper[T]{{i.node, i.depth}}
	}

	// Iterate through the stack until it's empty
	for len(i.stack) > 0 {
		nodeW := i.stack[len(i.stack)-1]
		i.stack = i.stack[:len(i.stack)-1]

		node := nodeW.n

		if node == nil {
			return nil, zero, false
		}

		switch node.(type) {
		case *NodeLeaf[T]:
			leafCh := node.(*NodeLeaf[T])
			if i.lowerBound {
				if bytes.Compare(getKey(leafCh.key), getKey(i.path)) >= 0 {
					return getKey(leafCh.key), leafCh.value, true
				}
				continue
			}
			if !leafCh.matchPrefix([]byte(i.Path())) {
				continue
			}
			return getKey(leafCh.key), leafCh.value, true
		case *Node4[T]:
			n4 := node.(*Node4[T])
			hasLeaf := n4.leaf != nil
			for itr := int(n4.numChildren) - 1; itr >= 0; itr-- {
				nodeCh := n4.children[itr]
				if nodeCh == nil {
					continue
				}
				key := n4.keys[itr]
				if i.seeKPrefixWatch {
					if (nodeW.d < len(i.path) && i.path[nodeW.d] == key) || (nodeW.d+int(n4.partialLen) >= len(i.path)) {
						i.stack = append(i.stack, NodeWrapper[T]{nodeCh, nodeW.d + int(n4.partialLen) + 1})
					} else if (nodeW.n.getNodeLeaf() != nil) && hasPrefix(getKey(nodeW.n.getNodeLeaf().getKey()), i.path) {
						i.stack = append(i.stack, NodeWrapper[T]{nodeW.n.getNodeLeaf(), nodeW.d + int(n4.partialLen) + 1})
					}
				} else {
					i.stack = append(i.stack, NodeWrapper[T]{nodeCh, nodeW.d + int(n4.partialLen) + 1})
				}
			}
			if hasLeaf && i.lowerBound {
				if bytes.Compare(n4.leaf.key, i.path) >= 0 {
					return getKey(n4.leaf.key), n4.leaf.value, true
				}
				continue
			}
			if hasLeaf && i.seeKPrefixWatch {
				return getKey(n4.leaf.key), n4.leaf.value, true
			}
		case *Node16[T]:
			n16 := node.(*Node16[T])
			hasLeaf := n16.leaf != nil
			for itr := int(n16.numChildren) - 1; itr >= 0; itr-- {
				nodeCh := n16.children[itr]
				if nodeCh == nil {
					continue
				}
				key := n16.keys[itr]
				if i.seeKPrefixWatch {
					if (nodeW.d < len(i.path) && i.path[nodeW.d] == key) || (nodeW.d+int(n16.partialLen) >= len(i.path)) {
						i.stack = append(i.stack, NodeWrapper[T]{nodeCh, nodeW.d + int(n16.partialLen) + 1})
					} else if (nodeW.n.getNodeLeaf() != nil) && hasPrefix(getKey(nodeW.n.getNodeLeaf().getKey()), i.path) {
						i.stack = append(i.stack, NodeWrapper[T]{nodeW.n.getNodeLeaf(), nodeW.d + int(n16.partialLen) + 1})
					}
				} else {
					i.stack = append(i.stack, NodeWrapper[T]{nodeCh, nodeW.d + int(n16.partialLen) + 1})
				}
			}
			if hasLeaf && i.lowerBound {
				if bytes.Compare(n16.leaf.key, i.path) >= 0 {
					return getKey(n16.leaf.key), n16.leaf.value, true
				}
				continue
			}
			if hasLeaf && i.seeKPrefixWatch {
				return getKey(n16.leaf.key), n16.leaf.value, true
			}
		case *Node48[T]:
			n48 := node.(*Node48[T])
			hasLeaf := n48.leaf != nil
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
				if i.seeKPrefixWatch {
					if (nodeW.d < len(i.path) && i.path[nodeW.d] == key) || (nodeW.d+int(n48.partialLen) >= len(i.path)) {
						i.stack = append(i.stack, NodeWrapper[T]{nodeCh, nodeW.d + int(n48.partialLen) + 1})
					} else if (nodeW.n.getNodeLeaf() != nil) && hasPrefix(getKey(nodeW.n.getNodeLeaf().getKey()), i.path) {
						i.stack = append(i.stack, NodeWrapper[T]{nodeW.n.getNodeLeaf(), nodeW.d + int(n48.partialLen) + 1})
					}
				} else {
					i.stack = append(i.stack, NodeWrapper[T]{nodeCh, nodeW.d + int(n48.partialLen) + 1})
				}
			}
			if hasLeaf && i.lowerBound {
				if bytes.Compare(n48.leaf.key, i.path) >= 0 {
					return getKey(n48.leaf.key), n48.leaf.value, true
				}
				continue
			}
			if hasLeaf && i.seeKPrefixWatch {
				return getKey(n48.leaf.key), n48.leaf.value, true
			}
		case *Node256[T]:
			n256 := node.(*Node256[T])
			hasLeaf := n256.leaf != nil
			for itr := 255; itr >= 0; itr-- {
				nodeCh := n256.children[itr]
				if nodeCh == nil {
					continue
				}
				i.stack = append(i.stack, NodeWrapper[T]{nodeCh, nodeW.d + int(n256.partialLen) + 1})
			}
			if hasLeaf && i.lowerBound {
				if bytes.Compare(n256.leaf.key, i.path) >= 0 {
					return getKey(n256.leaf.key), n256.leaf.value, true
				}
				continue
			}
			if hasLeaf && i.seeKPrefixWatch {
				return getKey(n256.leaf.key), n256.leaf.value, true
			}
		}
	}
	return nil, zero, false
}

func hasPrefix(key []byte, prefix []byte) bool {
	if len(prefix) == 0 {
		return true
	}
	return bytes.HasPrefix(key, prefix)
}

func (i *Iterator[T]) SeekPrefixWatch(prefix []byte) (watch <-chan struct{}) {
	// Start from the node
	i.seeKPrefixWatch = true

	node := i.node

	i.path = prefix

	i.stack = nil
	depth := 0

	if len(prefix) == 0 {
		i.node = node
		i.stack = []NodeWrapper[T]{{node, i.depth}}
		return node.getMutateCh()
	}

	i.stack = []NodeWrapper[T]{{node, i.depth}}
	i.node = node

	for {
		// Check if the node matches the prefix

		// Determine the child index to proceed based on the next byte of the prefix
		if node.getPartialLen() > 0 {
			// If the node has a prefix, compare it with the prefix
			mismatchIdx := prefixMismatch[T](node, prefix, len(prefix), depth)
			if mismatchIdx < int(node.getPartialLen()) || mismatchIdx >= len(prefix) {
				i.stack = nil
				i.node = node
				i.depth = depth
				if node.isLeaf() && !hasPrefix((getKey(node.getNodeLeaf().getKey())), prefix) {
					i.node = nil
				}
				return node.getMutateCh()
			}
			if mismatchIdx < int(node.getPartialLen()) {
				// If there's a mismatch, set the node to nil to break the loop
				i.stack = nil
				if node.getNodeLeaf() != nil && hasPrefix(getKey(node.getNodeLeaf().getKey()), prefix) {
					i.node = node
				} else {
					i.node = nil
				}
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
			if node.getNodeLeaf() != nil && hasPrefix(getKey(node.getNodeLeaf().getKey()), prefix) {
				i.node = node
			} else {
				i.node = nil
			}
			return node.getMutateCh()
		}

		i.stack = []NodeWrapper[T]{{node, i.depth}}
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
		var allCh []NodeWrapper[T]
		for itr := nCh - 1; itr >= 1; itr-- {
			allCh = append(allCh, NodeWrapper[T]{n.getChild(int(itr)), 0})
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

	i.stack = []NodeWrapper[T]{}
	i.lowerBound = true

	if len(prefixKey) == 0 {
		i.stack = []NodeWrapper[T]{{node, 0}}
		return
	}

	prefix := getTreeKey(prefixKey)

	found := func(n Node[T]) {
		i.stack = append(
			i.stack,
			NodeWrapper[T]{n, 0},
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
				i.stack = append(i.stack, NodeWrapper[T]{parent.getNodeLeaf(), 0})
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
				i.stack = append(i.stack, NodeWrapper[T]{parent.getNodeLeaf(), 0})
				return
			}
		}

		if prefixCmp < 0 && !i.seenMismatch {
			// Prefix is smaller than search prefix, that means there is no lower
			// bound
			i.node = nil
			if parent != nil && parent.getNodeLeaf() != nil {
				i.stack = append(i.stack, NodeWrapper[T]{parent.getNodeLeaf(), 0})
			}
			return
		}

		if node.isLeaf() && node.getNodeLeaf() != nil && bytes.Compare(node.getNodeLeaf().getKey(), prefix) >= 0 {
			found(node)
			if parent != nil && parent.getNodeLeaf() != nil {
				i.stack = append(i.stack, NodeWrapper[T]{parent.getNodeLeaf(), 0})
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
			i.stack = append(i.stack, NodeWrapper[T]{node, 0})
			if parent != nil && parent.getNodeLeaf() != nil {
				i.stack = append(i.stack, NodeWrapper[T]{parent.getNodeLeaf(), 0})
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
					i.stack = append(i.stack, NodeWrapper[T]{node.getChild(itr), 0})
				}
			}
			if node.getNodeLeaf() != nil {
				i.stack = append(i.stack, NodeWrapper[T]{node.getNodeLeaf(), 0})
			}
			return
		}

		for itr := int(node.getNumChildren()) - 1; itr >= idx+1; itr-- {
			if node.getChild(itr) != nil {
				i.stack = append(i.stack, NodeWrapper[T]{node.getChild(itr), 0})
			}
		}

		if parent != nil && parent.getNodeLeaf() != nil {
			i.stack = append(i.stack, NodeWrapper[T]{parent.getNodeLeaf(), 0})
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
