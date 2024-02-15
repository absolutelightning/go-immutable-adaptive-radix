package go_immutable_adaptive_radix_tree

type ArtNode4 struct {
	partialLen  uint32
	artNodeType uint8
	numChildren uint8
	partial     []byte
	keys        [4]byte
	children    [4]*ArtNode
}

func (n *ArtNode4) getPartialLen() uint32 {
	return n.partialLen
}

func (n *ArtNode4) setPartialLen(partialLen uint32) {
	n.partialLen = partialLen
}

func (n *ArtNode4) getArtNodeType() uint8 {
	return n.artNodeType
}

func (n *ArtNode4) setArtNodeType(artNodeType uint8) {
	n.artNodeType = artNodeType
}

func (n *ArtNode4) getNumChildren() uint8 {
	return n.numChildren
}

func (n *ArtNode4) setNumChildren(numChildren uint8) {
	n.numChildren = numChildren
}

func (n *ArtNode4) getPartial() []byte {
	return n.partial
}

func (n *ArtNode4) setPartial(partial []byte) {
	n.partial = partial
}

func (n *ArtNode4) isLeaf() bool {
	return false
}
