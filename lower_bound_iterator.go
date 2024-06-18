// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

import (
	"bytes"
)

// Iterator is used to iterate over a set of nodes from the node
// down to a specified path. This will iterate over the same values that
// the Node.WalkPath method will.
type LowerBoundIterator[T any] struct {
	path         []byte
	node         Node[T]
	stack        []Node[T]
	depth        int
	pos          Node[T]
	seenMismatch bool
}

// Front returns the current node that has been iterated to.
func (i *LowerBoundIterator[T]) Front() Node[T] {
	return i.pos
}

func (i *LowerBoundIterator[T]) Path() string {
	return string(i.path)
}

func (i *LowerBoundIterator[T]) Next() ([]byte, T, bool) {
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
				return getKey(n16.leaf.key), n16.leaf.value, true
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
				nodeCh := n256.children[byte(itr)]
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
			return getKey(leafCh.key), leafCh.value, true
		}
	}
	return nil, zero, false
}

func (i *LowerBoundIterator[T]) recurseMin(n Node[T]) Node[T] {
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

func (i *LowerBoundIterator[T]) SeekLowerBound(prefixKey []byte) {
	node := i.node

	i.stack = []Node[T]{}

	if len(prefixKey) == 0 {
		i.stack = []Node[T]{node}
		return
	}

	prefix := getTreeKey(prefixKey)

	found := func(n Node[T]) {
		nL := n.getNodeLeaf()
		if nL == nil {
			i.stack = append(
				i.stack,
				n,
			)
			return
		}
		if bytes.Compare(nL.key, i.path) >= 0 {
			i.stack = append(
				i.stack,
				n,
			)
			return
		}
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
				i.stack = append(i.stack, parent.getNodeLeaf())
			}
			return
		}

		var prefixCmp int
		if !node.isLeaf() {
			if int(node.getPartialLen()) < len(prefix) {
				prefixCmp = bytes.Compare(node.getPartial()[:node.getPartialLen()], prefix[depth:min(len(prefix), depth+int(node.getPartialLen()))])
			} else {
				prefixCmp = bytes.Compare(node.getPartial()[:node.getPartialLen()], prefix[depth:])
			}
		}

		if prefixCmp > 0 && !i.seenMismatch {
			// Prefix is larger, that means the lower bound is greater than the search
			// and from now on we need to follow the minimum path to the smallest
			// leaf under this subtree.
			nL := node.getNodeLeaf()
			if nL != nil {
				if bytes.Compare(nL.key, i.path) >= 0 {
					findMin(node)
				}
			} else {
				findMin(node)
			}
			if parent != nil && parent.getNodeLeaf() != nil {
				if bytes.Compare(parent.getNodeLeaf().getKey(), i.path) >= 0 {
					i.stack = append(i.stack, parent.getNodeLeaf())
				}
				return
			}
			return
		}

		if prefixCmp < 0 && !i.seenMismatch {
			// Prefix is smaller than search prefix, that means there is no lower
			// bound
			i.node = nil
			if parent != nil && parent.getNodeLeaf() != nil {
				if bytes.Compare(parent.getNodeLeaf().getKey(), i.path) >= 0 {
					i.stack = append(i.stack, parent.getNodeLeaf())
				}
			}
			return
		}

		if node.isLeaf() && node.getNodeLeaf() != nil && bytes.Compare(node.getNodeLeaf().getKey(), prefix) >= 0 {
			found(node)
			if parent != nil && parent.getNodeLeaf() != nil {
				if bytes.Compare(parent.getNodeLeaf().getKey(), i.path) >= 0 {
					i.stack = append(i.stack, parent.getNodeLeaf())
				}
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
			nL := node.getNodeLeaf()
			if nL != nil {
				if bytes.Compare(nL.getKey(), i.path) >= 0 {
					i.stack = append(i.stack, node)
				}
			} else {
				i.stack = append(i.stack, node)
			}
			if parent != nil && parent.getNodeLeaf() != nil {
				if bytes.Compare(parent.getNodeLeaf().getKey(), i.path) >= 0 {
					i.stack = append(i.stack, parent.getNodeLeaf())
				}
			}
			return
		}

		idx := node.getLowerBoundCh(prefix[depth])

		if idx == -1 && !i.seenMismatch {
			return
		}

		if i.seenMismatch && idx != -1 {
			idx = 0
		}

		if idx == -1 {
			if node.getNodeLeaf() != nil {
				if bytes.Compare(node.getNodeLeaf().getKey(), i.path) >= 0 {
					i.stack = append(i.stack, node)
				}
			} else {
				for itr := int(node.getNumChildren()) - 1; itr >= idx+1; itr-- {
					nCh := node.getChild(itr)
					nChL := nCh.getNodeLeaf()
					if nChL == nil {
						i.stack = append(i.stack, node.getChild(itr))
					} else {
						if bytes.Compare(nChL.key, i.path) >= 0 {
							i.stack = append(i.stack, node.getChild(itr))
						}
					}
				}
			}
			if parent.getNodeLeaf() != nil && bytes.Compare(parent.getNodeLeaf().getKey(), i.path) >= 0 {
				i.stack = append(i.stack, parent.getNodeLeaf())
			}
			node = nil
			return
		}

		if i.seenMismatch && parent != nil && parent.getNodeLeaf() != nil {
			nL := node.getNodeLeaf()
			addedNode := false
			if nL != nil {
				if bytes.Compare(nL.getKey(), i.path) >= 0 {
					i.stack = append(i.stack, node)
					addedNode = true
				}
			}
			if !addedNode {
				for itr := int(node.getNumChildren()) - 1; itr >= 0; itr-- {
					if node.getChild(itr) != nil {
						nCh := node.getChild(itr)
						nChL := nCh.getNodeLeaf()
						if nChL == nil {
							i.stack = append(i.stack, node.getChild(itr))
						} else {
							if bytes.Compare(nChL.key, i.path) >= 0 {
								i.stack = append(i.stack, node.getChild(itr))
							}
						}
					}
				}
			}
			if bytes.Compare(parent.getNodeLeaf().getKey(), i.path) >= 0 {
				i.stack = append(i.stack, parent.getNodeLeaf())
			}
			return
		}

		for itr := int(node.getNumChildren()) - 1; itr >= idx+1; itr-- {
			if node.getChild(itr) != nil {
				nCh := node.getChild(itr)
				nChL := nCh.getNodeLeaf()
				if nChL == nil {
					i.stack = append(i.stack, node.getChild(itr))
				} else {
					if bytes.Compare(nChL.key, i.path) >= 0 {
						i.stack = append(i.stack, node.getChild(itr))
					}
				}
			}
		}

		if idx >= 0 && node.getKeyAtIdx(idx) != prefix[depth] {
			i.seenMismatch = true
		}

		if idx == -1 {
			return
		}

		parent = node
		// Move to the next level in the tree
		node = node.getChild(idx)

		depth++

	}

}
