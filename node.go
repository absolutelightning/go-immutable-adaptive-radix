// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

type Node[T any] interface {
	getId() uint64
	setId(uint64)
	getPartialLen() uint32
	setPartialLen(uint32)
	getArtNodeType() nodeType
	getNumChildren() uint8
	setNumChildren(uint8)
	getPartial() []byte
	setPartial([]byte)
	isLeaf() bool
	matchPrefix([]byte) bool
	getChild(int) Node[T]
	setChild(int, Node[T])
	clone(bool) Node[T]
	setMutateCh(chan struct{})
	getKey() []byte
	getValue() T
	setValue(T)
	setKey([]byte)
	getKeyLen() uint32
	setKeyLen(uint32)
	getKeyAtIdx(int) byte
	setKeyAtIdx(int, byte)
	getChildren() []Node[T]
	getKeys() []byte
	getMutateCh() chan struct{}
	getLowerBoundCh(byte) int
	getNodeLeaf() *NodeLeaf[T]
	setNodeLeaf(*NodeLeaf[T])
	incrementMemory()

	Iterator() *Iterator[T]
	LowerBoundIterator() *LowerBoundIterator[T]
	PathIterator([]byte) *PathIterator[T]
	ReverseIterator() *ReverseIterator[T]
}
