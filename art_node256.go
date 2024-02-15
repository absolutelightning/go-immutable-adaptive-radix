package go_immutable_adaptive_radix_tree

type ArtNode256 struct {
	partialLen  uint32
	artNodeType uint8
	numChildren uint8
	partial     []byte
	keys        [16]byte
	children    [16]*ArtNode
}

func (n *ArtNode256) getPartialLen() uint32 {
	return n.partialLen
}

func (n *ArtNode256) setPartialLen(partialLen uint32) {
	n.partialLen = partialLen
}

func (n *ArtNode256) getArtNodeType() uint8 {
	return n.artNodeType
}

func (n *ArtNode256) setArtNodeType(artNodeType uint8) {
	n.artNodeType = artNodeType
}

func (n *ArtNode256) getNumChildren() uint8 {
	return n.numChildren
}

func (n *ArtNode256) setNumChildren(numChildren uint8) {
	n.numChildren = numChildren
}

func (n *ArtNode256) getPartial() []byte {
	return n.partial
}

func (n *ArtNode256) setPartial(partial []byte) {
	n.partial = partial
}

func (n *ArtNode256) isLeaf() bool {
	return false
}
