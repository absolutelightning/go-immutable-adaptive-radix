package go_immutable_adaptive_radix_tree

type ArtNodeLeaf struct {
	value       interface{}
	keyLen      uint32
	key         []byte
	artNodeType uint8
}

func (n *ArtNodeLeaf) getPartialLen() uint32 {
	// no-op
	return 0
}

func (n *ArtNodeLeaf) setPartialLen(partialLen uint32) {
	// no-op
}

func (n *ArtNodeLeaf) getArtNodeType() uint8 {
	return n.artNodeType
}

func (n *ArtNodeLeaf) setArtNodeType(artNodeType uint8) {
	n.artNodeType = artNodeType
}

func (n *ArtNodeLeaf) getNumChildren() uint8 {
	return 0
}

func (n *ArtNodeLeaf) setNumChildren(numChildren uint8) {
	// no-op
}

func (n *ArtNodeLeaf) isLeaf() bool {
	return true
}

func (n *ArtNodeLeaf) getValue() interface{} {
	return n.value
}

func (n *ArtNodeLeaf) setValue(value interface{}) {
	n.value = value
}

func (n *ArtNodeLeaf) getKeyLen() uint32 {
	return n.keyLen
}

func (n *ArtNodeLeaf) setKeyLen(keyLen uint32) {
	n.keyLen = keyLen
}

func (n *ArtNodeLeaf) getKey() []byte {
	return n.key
}

func (n *ArtNodeLeaf) setKey(key []byte) {
	n.key = key
}

func (n *ArtNodeLeaf) getPartial() []byte {
	//no-op
	return []byte{}
}

func (n *ArtNodeLeaf) setPartial(partial []byte) {
	// no-op
}
