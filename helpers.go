// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

import (
	"bytes"
)

func checkPrefix(partial []byte, partialLen int, key []byte, depth int) int {
	maxCmp := min(min(partialLen, maxPrefixLen), len(key)-depth)
	var idx int
	for idx = 0; idx < maxCmp; idx++ {
		if partial[idx] != key[depth+idx] {
			return idx
		}
	}
	return idx
}

func leafMatches(nodeKey []byte, key []byte) int {
	// Fail if the key lengths are different
	if len(nodeKey) != len(key) {
		return 1
	}
	// Compare the keys
	return bytes.Compare(nodeKey, key)
}

// longestCommonPrefix finds the length of the longest common prefix between two leaf nodes.
func longestCommonPrefix[T any](l1, l2 Node[T], depth int) int {
	maxCmp := len(l2.getKey()) - depth
	if len(l1.getKey()) < len(l2.getKey()) {
		maxCmp = int(l1.getKeyLen()) - depth
	}
	var idx int
	for idx = 0; idx < maxCmp; idx++ {
		if l1.getKey()[depth+idx] != l2.getKey()[depth+idx] {
			return idx
		}
	}
	return idx
}

// addChild adds a child node to the parent node.
func (t *Txn[T]) addChild(n Node[T], c byte, child Node[T]) Node[T] {
	switch n.getArtNodeType() {
	case node256:
		return t.addChild256(n, c, child)
	default:
		panic("Unknown node type")
	}
}

// addChild256 adds a child node to a node256.
func (t *Txn[T]) addChild256(n Node[T], c byte, child Node[T]) Node[T] {
	n.incrementMemory()
	idx := 0
	for i := 0; i < int(n.getNumChildren()); i++ {
		if n.getKeyAtIdx(i) <= c {
			idx = i + 1
		} else {
			break
		}
	}
	// Shift to make room
	length := int(n.getNumChildren()) - idx
	copy(n.getKeys()[idx+1:], n.getKeys()[idx:idx+length])
	copy(n.getChildren()[idx+1:], n.getChildren()[idx:idx+length])

	// Insert element
	n.setKeyAtIdx(idx, c)
	n.setChild(idx, child)
	n.setNumChildren(n.getNumChildren() + 1)
	return n
}

// copyHeader copies header information from src to dest node.
func (t *Txn[T]) copyHeader(dest, src Node[T]) {
	dest.setNumChildren(src.getNumChildren())
	length := min(maxPrefixLen, int(src.getPartialLen()))
	dest.setPartialLen(src.getPartialLen())
	copy(dest.getPartial()[:length], src.getPartial()[:length])
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// prefixMismatch calculates the index at which the prefixes mismatch.
func prefixMismatch[T any](n Node[T], key []byte, keyLen, depth int) int {
	maxCmp := min(min(maxPrefixLen, int(n.getPartialLen())), keyLen-depth)
	var idx int
	for idx = 0; idx < maxCmp; idx++ {
		if n.getPartial()[idx] != key[depth+idx] {
			return idx
		}
	}

	// If the prefix is short we can avoid finding a leaf
	if n.getPartialLen() > maxPrefixLen {
		// Prefix is longer than what we've checked, find a leaf
		l := minimum(n)
		if l == nil {
			return idx
		}
		maxCmp = min(len(l.key), keyLen) - depth
		for ; idx < maxCmp; idx++ {
			if l.key[idx+depth] != key[depth+idx] {
				return idx
			}
		}
	}
	return idx
}

// minimum finds the minimum leaf under a node.
func minimum[T any](node Node[T]) *NodeLeaf[T] {
	// Handle base cases
	if node == nil {
		return nil
	}
	if isLeaf[T](node) {
		if node.getArtNodeType() == leafType {
			return node.(*NodeLeaf[T])
		}
		return node.getNodeLeaf()
	}

	switch node.getArtNodeType() {
	case node256:
		if node.getNodeLeaf() != nil {
			return node.getNodeLeaf()
		}
		return minimum[T](node.getChild(0))
	default:
		panic("Unknown node type")
	}
	return nil
}

// maximum finds the maximum leaf under a node.
func maximum[T any](node Node[T]) *NodeLeaf[T] {
	// Handle base cases
	if node == nil {
		return nil
	}

	if isLeaf[T](node) {
		if node.getArtNodeType() == leafType {
			return node.(*NodeLeaf[T])
		}
		return node.getNodeLeaf()
	}

	switch node.getArtNodeType() {
	case node256:
		return maximum[T](node.getChild(int(node.getNumChildren() - 1)))
	default:
		panic("Unknown node type")
	}
	return nil
}

// IS_LEAF checks whether the least significant bit of the pointer x is set.
func isLeaf[T any](node Node[T]) bool {
	if node == nil {
		return false
	}
	return node.isLeaf()
}

func findChild[T any](n Node[T], c byte) (Node[T], int) {
	switch n.getArtNodeType() {
	case node256:
		keys := n.getKeys()
		nCh := int(n.getNumChildren())
		idx := 0
		for i := 0; i < nCh; i++ {
			if keys[i] <= c {
				idx = i
			} else {
				break
			}
		}
		if idx >= 0 && idx < len(keys) && keys[idx] == c {
			return n.getChild(idx), idx
		}
	case leafType:
		// no-op
		return nil, 0
	default:
		panic("Unknown node type")
	}
	return nil, 0
}

func getTreeKey(key []byte) []byte {
	return append(key, '$')
}

func getKey(key []byte) []byte {
	keyLen := len(key)
	if keyLen == 0 {
		return key
	}
	return key[:keyLen-1]
}

func (t *Txn[T]) removeChild(n Node[T], c byte) Node[T] {
	switch n.getArtNodeType() {
	case node256:
		return t.removeChild256(n, c)
	default:
		panic("invalid node type")
	}
}

func (t *Txn[T]) removeChild256(n Node[T], c byte) Node[T] {
	pos := 0
	for i := 0; i < int(n.getNumChildren()); i++ {
		if n.getKeyAtIdx(i) < c {
			pos = i + 1
		} else {
			break
		}
	}
	copy(n.getKeys()[pos:], n.getKeys()[pos+1:])
	slow := 0
	children := n.getChildren()
	itr := 0
	for ; itr < int(n.getNumChildren()); itr++ {
		if itr == pos {
			continue
		}
		n.setChild(slow, children[itr])
		slow += 1
	}
	for ; itr < len(n.getChildren()); itr++ {
		if n.getChild(itr) != nil {
			t.trackChannel(n.getChild(itr))
		}
		n.setChild(itr, nil)
	}

	n.setNumChildren(n.getNumChildren() - 1)

	if n.getNumChildren() == 1 && n.getNodeLeaf() == nil {
		nodeToReturn := t.writeNode(n.getChild(0), false)
		// Is not leaf
		if n.getArtNodeType() != leafType {
			// Concatenate the prefixes
			prefix := int(n.getPartialLen())
			if prefix < maxPrefixLen {
				n.getPartial()[prefix] = n.getKeyAtIdx(0)
				prefix++
			}
			if prefix < maxPrefixLen {
				subPrefix := min(int(nodeToReturn.getPartialLen()), maxPrefixLen-prefix)
				copy(n.getPartial()[prefix:], nodeToReturn.getPartial()[:subPrefix])
				prefix += subPrefix
			}

			// Store the prefix in the child
			copy(nodeToReturn.getPartial(), n.getPartial()[:min(prefix, maxPrefixLen)])
			nodeToReturn.setPartialLen(nodeToReturn.getPartialLen() + n.getPartialLen() + 1)
		}
		t.trackChannel(n)
		return nodeToReturn
	}
	return n
}

func hasPrefix(key []byte, prefix []byte) bool {
	if len(prefix) == 0 {
		return true
	}
	return bytes.HasPrefix(key, prefix)
}
