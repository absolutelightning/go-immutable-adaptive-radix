package go_immutable_adaptive_radix_tree

import "C"
import (
	"bytes"
	"math/bits"
)

const MaxPrefixLen = 10
const LEAF = 0
const NODE4 = 1
const NODE16 = 2
const NODE48 = 3
const NODE256 = 4

type ArtTree struct {
	root *ArtNode
	size uint64
}

func allocNode(nodeType uint8) ArtNode {
	var n ArtNode
	switch nodeType {
	case LEAF:
		n = &ArtNodeLeaf{}
	case NODE4:
		n = &ArtNode4{}
	case NODE16:
		n = &ArtNode16{}
	case NODE48:
		n = &ArtNode48{}
	case NODE256:
		n = &ArtNode256{}
	default:
		panic("Unknown node type")
	}
	n.setArtNodeType(nodeType)
	return n
}

// IS_LEAF checks whether the least significant bit of the pointer x is set.
func isLeaf(node ArtNode) bool {
	return node.isLeaf()
}

// ArtTreeInit /**
func ArtTreeInit(t *ArtTree) {
	t.root = nil
	t.size = 0
}

func destroyNode(n ArtNode) {
	// Break if null
	if n == nil {
		return
	}

	// Special case leafs
	if isLeaf(n) {
		leaf, ok := n.(*ArtNodeLeaf)
		if !ok {
			// Handle the case where n is not of type *ArtNodeLeaf
			return
		}
		// Free the key
		// You need to free the key only if it was dynamically allocated
		// Use appropriate cleanup depending on your use case
		leaf.key = nil
		// Free the leaf node itself
		return
	}

	// Handle each node type
	switch n.getArtNodeType() {
	case NODE4:
		node := n.(*ArtNode4)
		for i := 0; i < int(n.getNumChildren()); i++ {
			destroyNode(*node.children[i])
		}

	case NODE16:
		node := n.(*ArtNode16)
		for i := 0; i < int(n.getNumChildren()); i++ {
			destroyNode(*node.children[i])
		}

	case NODE48:
		node := n.(*ArtNode48)
		for i := 0; i < 256; i++ {
			idx := node.keys[i]
			if idx == 0 {
				continue
			}
			destroyNode(*node.children[idx-1])
		}

	case NODE256:
		node := n.(*ArtNode256)
		for i := 0; i < 256; i++ {
			if node.children[i] != nil {
				destroyNode(*node.children[i])
			}
		}

	default:
		panic("Unknown node type")
	}
}

func artTreeDestroy(tree *ArtTree) int {
	destroyNode(*tree.root)
	return 0
}

// findChild finds the child node pointer based on the given character in the ART tree node.
func findChild(n ArtNode, c byte) **ArtNode {
	switch n.getArtNodeType() {
	case NODE4:
		node := n.(*ArtNode4)
		for i := 0; i < int(n.getNumChildren()); i++ {
			if node.keys[i] == c {
				return &node.children[i]
			}
		}
	case NODE16:
		node := n.(*ArtNode16)

		// Compare the key to all 16 stored keys
		var bitfield uint16
		for i := 0; i < int(n.getNumChildren()); i++ {
			if node.keys[i] == c {
				bitfield |= 1 << uint(i)
			}
		}

		// Use a mask to ignore children that don't exist
		mask := (1 << n.getNumChildren()) - 1
		bitfield &= uint16(mask)

		// If we have a match (any bit set), return the pointer match
		if bitfield != 0 {
			return &node.children[bits.TrailingZeros16(bitfield)]
		}
	case NODE48:
		node := n.(*ArtNode48)
		i := node.keys[c]
		if i != 0 {
			return &node.children[i-1]
		}
	case NODE256:
		node := n.(*ArtNode256)
		if node.children[c] != nil {
			return &node.children[c]
		}
	default:
		panic("Unknown node type")
	}
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func checkPrefix(n ArtNode, key []byte, keyLen, depth int) int {
	maxCmp := min(min(int(n.getPartialLen()), MaxPrefixLen), keyLen-depth)
	var idx int
	for idx = 0; idx < maxCmp; idx++ {
		if n.getPartial()[idx] != key[depth+idx] {
			return idx
		}
	}
	return idx
}

func leafMatches(n *ArtNodeLeaf, key []byte, keyLen, depth int) int {
	// Ignore the depth parameter in Go

	// Fail if the key lengths are different
	if int(n.keyLen) != keyLen {
		return 1
	}

	// Compare the keys
	return bytes.Compare(n.key, key)
}

func artSearch(t *ArtTree, key []byte, keyLen int) interface{} {
	var child **ArtNode
	n := *t.root
	depth := 0

	for n != nil {
		// Might be a leaf
		if isLeaf(n) {
			leaf, ok := n.(*ArtNodeLeaf)
			if !ok {
				continue
			}
			// Check if the expanded path matches
			if leafMatches(leaf, key, keyLen, depth) == 0 {
				return leaf.value
			}
			return nil
		}

		// Bail if the prefix does not match
		if n.getPartialLen() > 0 {
			prefixLen := checkPrefix(n, key, keyLen, depth)
			if prefixLen != min(MaxPrefixLen, int(n.getPartialLen())) {
				return nil
			}
			depth += int(n.getPartialLen())
		}

		// Recursively search
		child = findChild(n, key[depth])
		if child != nil {
			n = **child
		} else {
			n = nil
		}
		depth++
	}

	return nil
}

// minimum finds the minimum leaf under a node.
func minimum(n ArtNode) *ArtNodeLeaf {
	// Handle base cases
	if n == nil {
		return nil
	}
	if isLeaf(n) {
		return n.(*ArtNodeLeaf)
	}

	var idx int
	switch n.getArtNodeType() {
	case NODE4:
		return minimum(*(n.(*ArtNode4)).children[0])
	case NODE16:
		return minimum(*(n.(*ArtNode16)).children[0])
	case NODE48:
		idx = 0
		node := n.(*ArtNode48)
		for idx < 256 && node.children[idx] == nil {
			idx++
		}
		if idx < 256 {
			return minimum(*node.children[idx])
		}
	case 4:
		node := n.(*ArtNode256)
		idx = 0
		for idx < 256 && *node.children[idx] == nil {
			idx++
		}
		if idx < 256 {
			return minimum(*node.children[idx])
		}
	default:
		panic("Unknown node type")
	}
	return nil
}

// maximum finds the maximum leaf under a node.
func maximum(n ArtNode) *ArtNodeLeaf {
	// Handle base cases
	if n == nil {
		return nil
	}
	if isLeaf(n) {
		return n.(*ArtNodeLeaf)
	}

	var idx int
	switch n.getArtNodeType() {
	case NODE4:
		return maximum(*n.(*ArtNode4).children[n.getNumChildren()-1])
	case NODE16:
		return maximum(*n.(*ArtNode16).children[n.getNumChildren()-1])
	case NODE48:
		node := n.(*ArtNode48)
		idx = 255
		for idx >= 0 && *node.children[idx] == nil {
			idx--
		}
		if idx >= 0 {
			return maximum(*node.children[idx])
		}
	case NODE256:
		idx = 255
		node := n.(*ArtNode256)
		for idx >= 0 && *node.children[idx] == nil {
			idx--
		}
		if idx >= 0 {
			return maximum(*node.children[idx])
		}
	default:
		panic("Unknown node type")
	}
	return nil
}

// makeLeaf creates a new leaf node.
func makeLeaf(key []byte, keyLen int, value interface{}) *ArtNodeLeaf {
	// Allocate memory for the leaf node
	l := &ArtNodeLeaf{}
	if l == nil {
		return nil
	}

	// Set the value and key length
	l.value = value
	l.keyLen = uint32(keyLen)

	// Copy the key
	copy(l.key[:], key)

	return l
}

// longestCommonPrefix finds the length of the longest common prefix between two leaf nodes.
func longestCommonPrefix(l1, l2 *ArtNodeLeaf, depth int) int {
	maxCmp := min(int(l1.keyLen), int(l2.keyLen)) - depth
	var idx int
	for idx = 0; idx < maxCmp; idx++ {
		if l1.key[depth+idx] != l2.key[depth+idx] {
			return idx
		}
	}
	return idx
}

// copyHeader copies header information from src to dest node.
func copyHeader(dest, src ArtNode) {
	dest.setNumChildren(src.getNumChildren())
	dest.setPartialLen(src.getPartialLen())
	partialToCopy := src.getPartial()[:min(MaxPrefixLen, int(src.getPartialLen()))]
	copy(dest.getPartial()[:min(MaxPrefixLen, int(src.getPartialLen()))], partialToCopy)
}

// addChild256 adds a child node to a node256.
func addChild256(n *ArtNode256, c byte, child *ArtNode) {
	n.numChildren++
	n.children[c] = child
}

// addChild48 adds a child node to a node48.
func addChild48(n *ArtNode48, c byte, child *ArtNode) {
	if n.numChildren < 48 {
		pos := 0
		for n.children[pos] != nil {
			pos++
		}
		n.children[pos] = child
		n.keys[c] = byte(pos + 1)
		n.numChildren++
	} else {
		new_node := (allocNode(NODE256)).(*ArtNode256)
		for i := 0; i < 256; i++ {
			if n.keys[i] != 0 {
				new_node.children[i] = n.children[int(n.keys[i])-1]
			}
		}
		copyHeader(new_node, n)
		addChild256(new_node, c, child)
	}
}

// addChild16 adds a child node to a node16.
func addChild16(n *ArtNode16, c byte, child ArtNode) {
	if n.numChildren < 16 {
		var mask uint32 = (1 << n.numChildren) - 1
		var bitfield uint32

		// Compare the key to all 16 stored keys
		for i := 0; i < 16; i++ {
			if c < n.keys[i] {
				bitfield |= 1 << i
			}
		}

		// Use a mask to ignore children that don't exist
		bitfield &= mask

		// Check if less than any
		var idx int
		if bitfield != 0 {
			idx = bits.TrailingZeros32(bitfield)
			copy(n.keys[idx+1:], n.keys[idx:])
			copy(n.children[idx+1:], n.children[idx:])
		} else {
			idx = int(n.numChildren)
		}

		// Set the child
		n.keys[idx] = c
		n.children[idx] = &child
		n.numChildren++

	} else {
		new_node := allocNode(NODE48).(*ArtNode48)

		// Copy the child pointers and populate the key map
		copy(new_node.children[:], n.children[:n.numChildren])
		for i := 0; i < int(n.numChildren); i++ {
			new_node.keys[n.keys[i]] = byte(i + 1)
		}

		copyHeader(new_node, n)
		addChild48(new_node, c, &child)
	}
}
