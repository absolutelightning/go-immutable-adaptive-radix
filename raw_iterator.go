package adaptive

import "bytes"

// rawIterator visits each of the nodes in the tree, even the ones that are not
// leaves. It keeps track of the effective path (what a leaf at a given node
// would be called), which is useful for comparing trees.
type rawIterator[T any] struct {
	// node is the starting node in the tree for the iterator.
	node Node[T]

	// stack keeps track of edges in the frontier.
	stack []rawStackEntry[T]

	// pos is the current position of the iterator.
	pos Node[T]

	// path is the effective path of the current iterator position,
	// regardless of whether the current node is a leaf.
	path string
}

// rawStackEntry is used to keep track of the cumulative common path as well as
// its associated edges in the frontier.
type rawStackEntry[T any] struct {
	path  string
	nodes []Node[T]
}

// Front returns the current node that has been iterated to.
func (i *rawIterator[T]) Front() Node[T] {
	return i.pos
}

// Path returns the effective path of the current node, even if it's not actually
// a leaf.
func (i *rawIterator[T]) Path() string {
	return i.path
}

// Next advances the iterator to the next node.
func (i *rawIterator[T]) Next() {
	// Initialize our stack if needed.
	if i.stack == nil && i.node != nil {
		i.stack = []rawStackEntry[T]{
			{
				nodes: []Node[T]{
					i.node,
				},
			},
		}
	}

	for len(i.stack) > 0 {
		// Inspect the last element of the stack.
		n := len(i.stack)
		last := i.stack[n-1]
		elem := last.nodes[0]

		// Update the stack.
		if len(last.nodes) > 1 {
			i.stack[n-1].nodes = last.nodes[1:]
		} else {
			i.stack = i.stack[:n-1]
		}

		// Push the edges onto the frontier.
		if elem != nil && elem.getNumChildren() > 0 {
			partial := bytes.Trim(elem.getPartial(), "\x00")
			length := min(maxPrefixLen, int(elem.getPartialLen()))
			if len(partial) > length {
				i.path = last.path + string(partial[:length])
			} else {
				i.path = last.path + string(partial)
			}
			path := last.path + string(elem.getPartial()[:elem.getPartialLen()])
			i.stack = append(i.stack, rawStackEntry[T]{path, elem.getChildren()})
		}

		if elem != nil {
			i.pos = elem
			partial := bytes.Trim(elem.getPartial(), "\x00")
			length := min(maxPrefixLen, int(elem.getPartialLen()))
			if len(partial) > length {
				i.path = last.path + string(partial[:length])
			} else {
				i.path = last.path + string(partial)
			}
			return
		}
	}

	i.pos = nil
	i.path = ""
}
