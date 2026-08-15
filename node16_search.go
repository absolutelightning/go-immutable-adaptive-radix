// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

import (
	"encoding/binary"
	"math/bits"
)

// node16FindIdx returns the lowest index i in [0, n) where keys[i] == c, or -1.
// n must be <= 16.
//
// SWAR (SIMD-Within-A-Register): broadcast c across a word, XOR, then use the
// classic zero-byte detection trick
//
//	(x - 0x01..01) & ^x & 0x80..80
//
// which sets the high bit of byte k iff byte k is zero — i.e. iff keys[k] == c
// after the XOR. This costs a constant handful of ALU ops regardless of how
// many of the 16 slots are filled, so unlike a linear scan it does not slow
// down as node16 nodes fill up (e.g. hex keys, which fill every slot).
func node16FindIdx(keys *[16]byte, n uint8, c byte) int {
	const (
		ones   uint64 = 0x0101010101010101
		eights uint64 = 0x8080808080808080
	)
	bc := uint64(c) * ones
	lo := binary.LittleEndian.Uint64(keys[0:8]) ^ bc
	hi := binary.LittleEndian.Uint64(keys[8:16]) ^ bc

	loMask := (lo - ones) & ^lo & eights
	hiMask := (hi - ones) & ^hi & eights

	var idx int
	switch {
	case loMask != 0:
		idx = bits.TrailingZeros64(loMask) >> 3
	case hiMask != 0:
		idx = 8 + bits.TrailingZeros64(hiMask)>>3
	default:
		return -1
	}
	if idx < int(n) {
		return idx
	}
	return -1
}
