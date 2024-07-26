// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

import (
	"bytes"
	"sort"
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
	case node4:
		return t.addChild4(n, c, child)
	case node8:
		return t.addChild8(n, c, child)
	case node16:
		return t.addChild16(n, c, child)
	case node32:
		return t.addChild32(n, c, child)
	case node64:
		return t.addChild64(n, c, child)
	case node128:
		return t.addChild128(n, c, child)
	case node256:
		return t.addChild256(n, c, child)
	default:
		panic("Unknown node type")
	}
}

// addChild4 adds a child node to a node4.
func (t *Txn[T]) addChild4(n Node[T], c byte, child Node[T]) Node[T] {
	if n.getNumChildren() < 4 {
		idx := sort.Search(int(n.getNumChildren()), func(i int) bool {
			return n.getKeyAtIdx(i) > c
		})
		// Shift to make room
		length := int(n.getNumChildren()) - idx
		copy(n.getKeys()[idx+1:], n.getKeys()[idx:idx+length])
		copy(n.getChildren()[idx+1:], n.getChildren()[idx:idx+length])

		// Insert element
		n.setKeyAtIdx(idx, c)
		n.setChild(idx, child)
		n.setNumChildren(n.getNumChildren() + 1)
		return n
	} else {
		newNode := t.allocNode(node8)
		// Copy the child pointers and the key map
		if n.getNodeLeaf() != nil {
			nL := t.writeNode(n.getNodeLeaf(), true)
			newNode.setNodeLeaf(nL.(*NodeLeaf[T]))
		}
		copy(newNode.getChildren()[:], n.getChildren()[:n.getNumChildren()])
		copy(newNode.getKeys()[:], n.getKeys()[:n.getNumChildren()])
		t.copyHeader(newNode, n)
		return t.addChild8(newNode, c, child)
	}
}

// addChild4 adds a child node to a node4.
func (t *Txn[T]) addChild8(n Node[T], c byte, child Node[T]) Node[T] {
	if n.getNumChildren() < 8 {
		idx := sort.Search(int(n.getNumChildren()), func(i int) bool {
			return n.getKeyAtIdx(i) > c
		})
		// Shift to make room
		length := int(n.getNumChildren()) - idx
		copy(n.getKeys()[idx+1:], n.getKeys()[idx:idx+length])
		copy(n.getChildren()[idx+1:], n.getChildren()[idx:idx+length])

		// Insert element
		n.setKeyAtIdx(idx, c)
		n.setChild(idx, child)
		n.setNumChildren(n.getNumChildren() + 1)
		return n
	} else {
		newNode := t.allocNode(node16)
		// Copy the child pointers and the key map
		if n.getNodeLeaf() != nil {
			nL := t.writeNode(n.getNodeLeaf(), true)
			newNode.setNodeLeaf(nL.(*NodeLeaf[T]))
		}
		copy(newNode.getChildren()[:], n.getChildren()[:n.getNumChildren()])
		copy(newNode.getKeys()[:], n.getKeys()[:n.getNumChildren()])
		t.copyHeader(newNode, n)
		return t.addChild16(newNode, c, child)
	}
}

// addChild16 adds a child node to a node16.
func (t *Txn[T]) addChild16(n Node[T], c byte, child Node[T]) Node[T] {
	if n.getNumChildren() < 16 {
		idx := sort.Search(int(n.getNumChildren()), func(i int) bool {
			return n.getKeyAtIdx(i) > c
		})
		// Set the child
		length := int(n.getNumChildren()) - idx
		copy(n.getKeys()[idx+1:], n.getKeys()[idx:idx+length])
		copy(n.getChildren()[idx+1:], n.getChildren()[idx:idx+length])

		// Insert element
		n.setKeyAtIdx(idx, c)
		n.setChild(idx, child)
		n.setNumChildren(n.getNumChildren() + 1)
		return n
	} else {
		newNode := t.allocNode(node32)
		// Copy the child pointers and the key map
		if n.getNodeLeaf() != nil {
			nL := t.writeNode(n.getNodeLeaf(), true)
			newNode.setNodeLeaf(nL.(*NodeLeaf[T]))
		}
		copy(newNode.getChildren()[:], n.getChildren()[:n.getNumChildren()])
		copy(newNode.getKeys()[:], n.getKeys()[:n.getNumChildren()])
		t.copyHeader(newNode, n)
		return t.addChild32(newNode, c, child)
	}
}

// addChild16 adds a child node to a node16.
func (t *Txn[T]) addChild32(n Node[T], c byte, child Node[T]) Node[T] {
	if n.getNumChildren() < 32 {
		idx := sort.Search(int(n.getNumChildren()), func(i int) bool {
			return n.getKeyAtIdx(i) > c
		})
		// Set the child
		length := int(n.getNumChildren()) - idx
		copy(n.getKeys()[idx+1:], n.getKeys()[idx:idx+length])
		copy(n.getChildren()[idx+1:], n.getChildren()[idx:idx+length])

		// Insert element
		n.setKeyAtIdx(idx, c)
		n.setChild(idx, child)
		n.setNumChildren(n.getNumChildren() + 1)
		return n
	} else {
		newNode := t.allocNode(node64)
		// Copy the child pointers and the key map
		if n.getNodeLeaf() != nil {
			nL := t.writeNode(n.getNodeLeaf(), true)
			newNode.setNodeLeaf(nL.(*NodeLeaf[T]))
		}
		copy(newNode.getChildren()[:], n.getChildren()[:n.getNumChildren()])
		copy(newNode.getKeys()[:], n.getKeys()[:n.getNumChildren()])
		t.copyHeader(newNode, n)
		return t.addChild64(newNode, c, child)
	}
}

// addChild16 adds a child node to a node16.
func (t *Txn[T]) addChild64(n Node[T], c byte, child Node[T]) Node[T] {
	if n.getNumChildren() < 64 {
		idx := sort.Search(int(n.getNumChildren()), func(i int) bool {
			return n.getKeyAtIdx(i) > c
		})
		// Set the child
		length := int(n.getNumChildren()) - idx
		copy(n.getKeys()[idx+1:], n.getKeys()[idx:idx+length])
		copy(n.getChildren()[idx+1:], n.getChildren()[idx:idx+length])

		// Insert element
		n.setKeyAtIdx(idx, c)
		n.setChild(idx, child)
		n.setNumChildren(n.getNumChildren() + 1)
		return n
	} else {
		newNode := t.allocNode(node128)
		if n.getNodeLeaf() != nil {
			nL := t.writeNode(n.getNodeLeaf(), true)
			newNode.setNodeLeaf(nL.(*NodeLeaf[T]))
		}
		// Copy the child pointers and populate the key map
		copy(newNode.getChildren()[:], n.getChildren()[:n.getNumChildren()])
		for i := 0; i < int(n.getNumChildren()); i++ {
			newNode.setKeyAtIdx(int(n.getKeyAtIdx(i)), byte(i+1))
		}
		t.copyHeader(newNode, n)
		return t.addChild128(newNode, c, child)
	}
}

// addChild48 adds a child node to a Node128.
func (t *Txn[T]) addChild128(n Node[T], c byte, child Node[T]) Node[T] {
	if n.getNumChildren() < 128 {
		pos := 0
		for n.getChild(pos) != nil {
			pos++
		}
		n.setChild(pos, child)
		n.setKeyAtIdx(int(c), byte(pos+1))
		n.setNumChildren(n.getNumChildren() + 1)
		return n
	} else {
		newNode := t.allocNode(node256)
		if n.getNodeLeaf() != nil {
			nL := t.writeNode(n.getNodeLeaf(), true)
			newNode.setNodeLeaf(nL.(*NodeLeaf[T]))
		}
		for i := 0; i < 256; i++ {
			if n.getKeyAtIdx(i) != 0 {
				newNode.setChild(i, n.getChild(int(n.getKeyAtIdx(i))-1))
			}
		}
		t.copyHeader(newNode, n)
		return t.addChild256(newNode, c, child)
	}
}

// addChild256 adds a child node to a node256.
func (t *Txn[T]) addChild256(n Node[T], c byte, child Node[T]) Node[T] {
	n.setNumChildren(n.getNumChildren() + 1)
	n.setChild(int(c), child)
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

	var idx int
	switch node.getArtNodeType() {
	case node4, node8, node16, node32, node64:
		if node.getNodeLeaf() != nil {
			return node.getNodeLeaf()
		}
		return minimum[T](node.getChild(0))
	case node128:
		if node.getNodeLeaf() != nil {
			return node.getNodeLeaf()
		}
		idx = 0
		for idx < 256 && node.getKeyAtIdx(idx) == 0 {
			idx++
		}
		idx = int(node.getKeyAtIdx(idx) - 1)
		if idx < 48 {
			return minimum[T](node.getChild(idx))
		}
	case node256:
		if node.getNodeLeaf() != nil {
			return node.getNodeLeaf()
		}
		idx = 0
		for idx < 256 && node.getChild(idx) == nil {
			idx++
		}
		if idx < 256 {
			return minimum[T](node.getChild(idx))
		}
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

	var idx int
	switch node.getArtNodeType() {
	case node4, node8, node16, node32, node64:
		return maximum[T](node.getChild(int(node.getNumChildren() - 1)))
	case node128:
		idx = 255
		for idx >= 0 && node.getKeyAtIdx(idx) == 0 {
			idx--
		}
		idx = int(node.getKeyAtIdx(idx) - 1)
		if idx < 48 {
			return maximum[T](node.getChild(idx))
		}
	case node256:
		idx = 255
		for idx >= 0 && node.getChild(idx) == nil {
			idx--
		}
		if idx >= 0 {
			return maximum[T](node.getChild(idx))
		}
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
	case node4, node8, node16, node32, node64:
		keys := n.getKeys()
		nCh := int(n.getNumChildren())
		idx := sort.Search(nCh, func(i int) bool {
			return keys[i] > c
		})
		if idx >= 1 && keys[idx-1] == c {
			return n.getChild(idx - 1), idx - 1
		}
	case node128:
		i := n.getKeyAtIdx(int(c))
		if i != 0 {
			return n.getChild(int(i - 1)), int(i - 1)
		}
	case node256:
		ch := n.getChild(int(c))
		if ch != nil {
			return ch, int(c)
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
	case node4:
		return t.removeChild4(n, c)
	case node8:
		return t.removeChild8(n, c)
	case node16:
		return t.removeChild16(n, c)
	case node32:
		return t.removeChild32(n, c)
	case node64:
		return t.removeChild64(n, c)
	case node128:
		return t.removeChild128(n, c)
	case node256:
		return t.removeChild256(n, c)
	default:
		panic("invalid node type")
	}
}

func (t *Txn[T]) removeChild4(n Node[T], c byte) Node[T] {
	pos := sort.Search(int(n.getNumChildren()), func(i int) bool {
		return n.getKeyAtIdx(i) >= c
	})

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

func (t *Txn[T]) removeChild8(n Node[T], c byte) Node[T] {
	pos := sort.Search(int(n.getNumChildren()), func(i int) bool {
		return n.getKeyAtIdx(i) >= c
	})

	copy(n.getKeys()[pos:], n.getKeys()[pos+1:])
	children := n.getChildren()
	slow := 0
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

	if n.getNumChildren() == 3 {
		t.trackChannel(n)
		newNode := t.allocNode(node4)
		n4 := newNode.(*Node4[T])
		t.copyHeader(newNode, n)
		copy(n4.keys[:], n.getKeys()[:4])
		copy(n4.children[:], n.getChildren()[:4])
		if n.getNodeLeaf() != nil {
			nL := t.writeNode(n.getNodeLeaf(), true)
			newNode.setNodeLeaf(nL.(*NodeLeaf[T]))
		}
		newNode.setNodeLeaf(n.getNodeLeaf())
		return newNode
	}
	return n
}

func (t *Txn[T]) removeChild16(n Node[T], c byte) Node[T] {
	pos := sort.Search(int(n.getNumChildren()), func(i int) bool {
		return n.getKeyAtIdx(i) >= c
	})

	copy(n.getKeys()[pos:], n.getKeys()[pos+1:])
	children := n.getChildren()
	slow := 0
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

	if n.getNumChildren() == 6 {
		t.trackChannel(n)
		newNode := t.allocNode(node8)
		n4 := newNode.(*Node8[T])
		t.copyHeader(newNode, n)
		copy(n4.keys[:], n.getKeys()[:8])
		copy(n4.children[:], n.getChildren()[:8])
		if n.getNodeLeaf() != nil {
			nL := t.writeNode(n.getNodeLeaf(), true)
			newNode.setNodeLeaf(nL.(*NodeLeaf[T]))
		}
		newNode.setNodeLeaf(n.getNodeLeaf())
		return newNode
	}
	return n
}

func (t *Txn[T]) removeChild32(n Node[T], c byte) Node[T] {
	pos := sort.Search(int(n.getNumChildren()), func(i int) bool {
		return n.getKeyAtIdx(i) >= c
	})

	copy(n.getKeys()[pos:], n.getKeys()[pos+1:])
	children := n.getChildren()
	slow := 0
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

	if n.getNumChildren() == 14 {
		t.trackChannel(n)
		newNode := t.allocNode(node16)
		n4 := newNode.(*Node16[T])
		t.copyHeader(newNode, n)
		copy(n4.keys[:], n.getKeys()[:16])
		copy(n4.children[:], n.getChildren()[:16])
		if n.getNodeLeaf() != nil {
			nL := t.writeNode(n.getNodeLeaf(), true)
			newNode.setNodeLeaf(nL.(*NodeLeaf[T]))
		}
		newNode.setNodeLeaf(n.getNodeLeaf())
		return newNode
	}
	return n
}

func (t *Txn[T]) removeChild64(n Node[T], c uint8) Node[T] {
	pos := sort.Search(int(n.getNumChildren()), func(i int) bool {
		return n.getKeyAtIdx(i) >= c
	})

	copy(n.getKeys()[pos:], n.getKeys()[pos+1:])
	children := n.getChildren()
	slow := 0
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

	if n.getNumChildren() == 30 {
		t.trackChannel(n)
		newNode := t.allocNode(node32)
		n4 := newNode.(*Node32[T])
		t.copyHeader(newNode, n)
		copy(n4.keys[:], n.getKeys()[:32])
		copy(n4.children[:], n.getChildren()[:32])
		if n.getNodeLeaf() != nil {
			nL := t.writeNode(n.getNodeLeaf(), true)
			newNode.setNodeLeaf(nL.(*NodeLeaf[T]))
		}
		newNode.setNodeLeaf(n.getNodeLeaf())
		return newNode
	}
	return n
}

func (t *Txn[T]) removeChild128(n Node[T], c uint8) Node[T] {
	pos := n.getKeyAtIdx(int(c))
	n.setKeyAtIdx(int(c), 0)
	t.trackChannel(n.getChild(int(pos - 1)))
	n.setChild(int(pos-1), nil)
	n.setNumChildren(n.getNumChildren() - 1)

	if n.getNumChildren() == 60 {
		newNode := t.allocNode(node64)
		t.trackChannel(n)
		t.copyHeader(newNode, n)
		if n.getNodeLeaf() != nil {
			nL := t.writeNode(n.getNodeLeaf(), true)
			newNode.setNodeLeaf(nL.(*NodeLeaf[T]))
		}
		child := 0
		for i := 0; i < 256; i++ {
			pos = n.getKeyAtIdx(i)
			if pos != 0 {
				newNode.setKeyAtIdx(child, byte(i))
				newNode.setChild(child, n.getChild(int(pos-1)))
				child++
			}
		}
		return newNode
	}
	return n
}

func (t *Txn[T]) removeChild256(n Node[T], c uint8) Node[T] {
	n.setChild(int(c), nil)
	n.setNumChildren(n.getNumChildren() - 1)

	// Resize to a Node128 on underflow, not immediately to prevent
	// trashing if we sit on the 48/49 boundary
	if n.getNumChildren() == 120 {
		newNode := t.allocNode(node128)
		t.copyHeader(newNode, n)
		t.trackChannel(n)
		if n.getNodeLeaf() != nil {
			nL := t.writeNode(n.getNodeLeaf(), true)
			newNode.setNodeLeaf(nL.(*NodeLeaf[T]))
		}
		pos := 0
		for i := 0; i < 256; i++ {
			if n.getChild(i) != nil {
				newNode.setChild(pos, n.getChild(i))
				newNode.setKeyAtIdx(i, byte(pos+1))
				pos++
			}
		}
		return newNode
	}
	return n
}

func hasPrefix(key []byte, prefix []byte) bool {
	if len(prefix) == 0 {
		return true
	}
	return bytes.HasPrefix(key, prefix)
}
