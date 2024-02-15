package go_immutable_adaptive_radix_tree

type ArtNode48 struct {
	partialLen  uint32
	artNodeType uint8
	numChildren uint8
	partial     []byte
	keys        [256]byte
	children    [48]*ArtNode
}

func (n *ArtNode48) getPartialLen() uint32 {
	return n.partialLen
}

func (n *ArtNode48) setPartialLen(partialLen uint32) {
	n.partialLen = partialLen
}

func (n *ArtNode48) getArtNodeType() uint8 {
	return n.artNodeType
}

func (n *ArtNode48) setArtNodeType(artNodeType uint8) {
	n.artNodeType = artNodeType
}

func (n *ArtNode48) getNumChildren() uint8 {
	return n.numChildren
}

func (n *ArtNode48) setNumChildren(numChildren uint8) {
	n.numChildren = numChildren
}

func (n *ArtNode48) getPartial() []byte {
	return n.partial
}

func (n *ArtNode48) setPartial(partial []byte) {
	n.partial = partial
}

func (n *ArtNode48) isLeaf() bool {
	return false
}
