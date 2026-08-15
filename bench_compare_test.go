// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

// Comparative benchmarks: this Adaptive Radix Tree (ART) vs.
// hashicorp/go-immutable-radix/v2 (iradix), a persistent binary radix tree.
//
// Both libraries are immutable/persistent and generic, so the comparison is
// apples-to-apples on the same fixed key sets. Keys are loaded once, outside the
// timed region, so we measure only tree operations (not key generation).
//
// Run:
//
//	go test -bench 'Compare' -benchmem -run '^$'
//
// Narrow to one dataset/operation:
//
//	go test -bench 'Compare/uuid/Get' -benchmem -run '^$'

import (
	"bufio"
	"math/rand"
	"os"
	"testing"

	iradix "github.com/hashicorp/go-immutable-radix/v2"
)

// loadKeys reads up to max lines from a file under test-text/ and returns them
// as byte slices. It skips the benchmark (rather than failing) if the corpus is
// missing, so the suite stays runnable in minimal checkouts.
func loadKeys(tb testing.TB, path string, max int) [][]byte {
	tb.Helper()
	f, err := os.Open(path)
	if err != nil {
		tb.Skipf("corpus %s unavailable: %v", path, err)
	}
	defer f.Close()

	keys := make([][]byte, 0, max)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() && len(keys) < max {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		k := make([]byte, len(line))
		copy(k, line)
		keys = append(keys, k)
	}
	if err := sc.Err(); err != nil {
		tb.Fatalf("reading %s: %v", path, err)
	}
	if len(keys) == 0 {
		tb.Skipf("corpus %s empty", path)
	}
	return keys
}

// benchDatasets returns the named key sets used across the comparative
// benchmarks. Loaded lazily per benchmark so a missing corpus only skips the
// benchmarks that need it.
func benchDatasets(tb testing.TB) map[string][][]byte {
	const n = 100_000
	return map[string][][]byte{
		"uuid":  loadKeys(tb, "test-text/uuid.txt", n),
		"words": loadKeys(tb, "test-text/words.txt", n),
	}
}

// distinctCount reports the number of unique keys in the set.
func distinctCount(keys [][]byte) int {
	seen := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		seen[string(k)] = struct{}{}
	}
	return len(seen)
}

// ---- ART builders ----

func buildART(keys [][]byte) *RadixTree[int] {
	t := NewRadixTree[int]()
	txn := t.Txn()
	for i, k := range keys {
		txn.Insert(k, i)
	}
	return txn.Commit()
}

func buildIradix(keys [][]byte) *iradix.Tree[int] {
	t := iradix.New[int]()
	txn := t.Txn()
	for i, k := range keys {
		txn.Insert(k, i)
	}
	return txn.Commit()
}

// ---- Bulk build (insert all keys into a fresh tree) ----

func BenchmarkCompareInsert(b *testing.B) {
	for name, keys := range benchDatasets(b) {
		keys := keys
		b.Run("ART/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = buildART(keys)
			}
		})
		b.Run("iradix/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = buildIradix(keys)
			}
		})
	}
}

// ---- Point lookups on a prebuilt tree ----

func BenchmarkCompareGet(b *testing.B) {
	for name, keys := range benchDatasets(b) {
		keys := keys
		// Fixed pseudo-random lookup order, identical for both trees.
		order := rand.New(rand.NewSource(1)).Perm(len(keys))

		art := buildART(keys)
		b.Run("ART/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				k := keys[order[i%len(order)]]
				if _, ok := art.Get(k); !ok {
					b.Fatalf("ART missing key %q", k)
				}
			}
		})

		ir := buildIradix(keys)
		b.Run("iradix/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				k := keys[order[i%len(order)]]
				if _, ok := ir.Get(k); !ok {
					b.Fatalf("iradix missing key %q", k)
				}
			}
		})
	}
}

// ---- Single-key insert into an existing tree (persistent update path) ----

func BenchmarkCompareUpdate(b *testing.B) {
	for name, keys := range benchDatasets(b) {
		keys := keys
		order := rand.New(rand.NewSource(2)).Perm(len(keys))

		art := buildART(keys)
		b.Run("ART/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				k := keys[order[i%len(order)]]
				art, _, _ = art.Insert(k, i)
			}
		})

		ir := buildIradix(keys)
		b.Run("iradix/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				k := keys[order[i%len(order)]]
				ir, _, _ = ir.Insert(k, i)
			}
		})
	}
}

// ---- Ordered iteration over the whole tree ----

func BenchmarkCompareIterate(b *testing.B) {
	for name, keys := range benchDatasets(b) {
		keys := keys

		// Distinct keys only: the corpus may contain duplicates, and both
		// trees collapse those, so the iteration count is the tree size.
		want := distinctCount(keys)

		art := buildART(keys)
		b.Run("ART/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				it := art.Root().Iterator()
				it.SeekPrefixWatch([]byte("")) // seed the stack with the root
				count := 0
				for _, _, ok := it.Next(); ok; _, _, ok = it.Next() {
					count++
				}
				if count != want {
					b.Fatalf("ART iterated %d keys, want %d", count, want)
				}
			}
		})

		ir := buildIradix(keys)
		b.Run("iradix/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				it := ir.Root().Iterator()
				count := 0
				for _, _, ok := it.Next(); ok; _, _, ok = it.Next() {
					count++
				}
				if count != want {
					b.Fatalf("iradix iterated %d keys, want %d", count, want)
				}
			}
		})
	}
}
