package go_immutable_adaptive_radix_tree

type ArtTree struct {
	root *ArtNode
	size uint64
}

func NewArtTree() *ArtTree {
	return &ArtTree{root: nil, size: 0}
}

func (t *ArtTree) Insert(key []byte, value interface{}) {
	keyLen := len(key) + 1
	newKey := make([]byte, keyLen)
	copy(newKey, key)
	artInsert(t, newKey, value)
}

func (t *ArtTree) Search(key []byte) interface{} {
	keyLen := len(key) + 1
	newKey := make([]byte, keyLen)
	copy(newKey, key)
	return artSearch(t, newKey)
}
