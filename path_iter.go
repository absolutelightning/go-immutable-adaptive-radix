// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

// PathIterator is used to iterate over a set of nodes from the node
// down to a specified path. This will iterate over the same values that
// the Node.WalkPath method will.
type PathIterator[T any] struct {
	path  []byte
	depth int
	node  *Node[T]
	stack []Node[T]
}

func (i *PathIterator[T]) Next() ([]byte, T, bool) {

	var zero T

	if len(i.stack) == 0 {
		return nil, zero, false
	}

	// Iterate through the stack until it's empty
	for len(i.stack) > 0 {
		nodeCur := i.stack[0]
		i.stack = i.stack[1:]
		currentNode := nodeCur.(Node[T])

		switch currentNode.getArtNodeType() {
		case leafType:
			leafCh := currentNode.(*NodeLeaf[T])
			if leafCh.prefixContainsMatch(i.path) {
				return getKey(leafCh.key), leafCh.value, true
			}
			continue
		case node4, node16, node32, node64:
			for itr := int(currentNode.getNumChildren()) - 1; itr >= 0; itr-- {
				nodeCh := currentNode.getChild(itr)
				if nodeCh == nil {
					continue
				}
				child := (currentNode.getChild(itr)).(Node[T])
				newStack := make([]Node[T], len(i.stack)+1)
				copy(newStack[1:], i.stack)
				newStack[0] = child
				i.stack = newStack
			}
			if currentNode.getNodeLeaf() != nil {
				i.stack = append([]Node[T]{currentNode.getNodeLeaf()}, i.stack...)
			}
		case node128:
			n48 := currentNode.(*Node128[T])
			for itr := 255; itr >= 0; itr-- {
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
			if n48.getNodeLeaf() != nil {
				i.stack = append([]Node[T]{n48.getNodeLeaf()}, i.stack...)
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
			if n256.getNodeLeaf() != nil {
				i.stack = append([]Node[T]{n256.getNodeLeaf()}, i.stack...)
			}
		}
	}
	return nil, zero, false
}
