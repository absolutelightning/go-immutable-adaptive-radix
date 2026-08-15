// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

// Comparative benchmarks: this Adaptive Radix Tree (ART) vs.
// hashicorp/go-immutable-radix/v2 (iradix), a persistent binary radix tree.
//
// Both libraries are immutable/persistent and generic, so the comparison is
// apples-to-apples on the same fixed key sets. Each tree is built INSIDE its own
// sub-benchmark and a GC is forced before the timer starts, so only the tree
// under test is live during timing — this keeps the numbers low-variance.
//
// Datasets deliberately span a range of node fanouts, because that is where ART
// and iradix differ most: iradix binary-searches its sorted edges at every node
// (O(log fanout)), while ART indexes children directly (Node48/Node256) or with
// a branchless SWAR compare (Node16). The gap therefore widens as fanout grows.
//
//	uuid   - 36-char hex, alphabet of 16  -> Node16-heavy (low fanout)
//	words  - natural language             -> mixed, mostly low fanout
//	seq    - sequential uint64 big-endian -> dense Node256 in the low bytes
//	rand8  - random 8-byte keys           -> dense Node48/Node256 near the root
//
// Run:
//
//	go test -bench 'Compare' -benchmem -run '^$'
//
// Narrow to one operation/dataset, e.g. the high-fanout Get comparison:
//
//	go test -bench 'CompareGet/.*/seq' -benchmem -run '^$'

import (
	"bufio"
	"encoding/binary"
	"math/rand"
	"os"
	"runtime"
	"testing"

	iradix "github.com/hashicorp/go-immutable-radix/v2"
)

const benchN = 100_000

type dataset struct {
	name string
	keys [][]byte
}

// datasets returns the key sets used across the comparative benchmarks and the
// differential correctness test, capped at n keys each.
func datasets(tb testing.TB, n int) []dataset {
	return []dataset{
		{"uuid", loadKeys(tb, "test-text/uuid.txt", n)},
		{"words", loadKeys(tb, "test-text/words.txt", n)},
		{"seq", seqKeys(n)},
		{"rand8", randKeys(n, 8)},
	}
}

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

// seqKeys returns n sequential big-endian uint64 keys. The shared high-order
// zero bytes compress away, leaving fully populated (256-way) nodes in the low
// bytes — the case where ART's direct child indexing most outruns a binary
// search over edges.
func seqKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(i))
		keys[i] = b
	}
	return keys
}

// randKeys returns n distinct random keys of the given width (deterministic
// seed for reproducibility).
func randKeys(n, width int) [][]byte {
	r := rand.New(rand.NewSource(42))
	seen := make(map[string]struct{}, n)
	keys := make([][]byte, 0, n)
	for len(keys) < n {
		b := make([]byte, width)
		for j := range b {
			b[j] = byte(r.Intn(256))
		}
		if _, ok := seen[string(b)]; ok {
			continue
		}
		seen[string(b)] = struct{}{}
		keys = append(keys, b)
	}
	return keys
}

// distinctCount reports the number of unique keys in the set.
func distinctCount(keys [][]byte) int {
	seen := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		seen[string(k)] = struct{}{}
	}
	return len(seen)
}

// ---- builders ----

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
	for _, ds := range datasets(b, benchN) {
		keys := ds.keys
		b.Run("ART/"+ds.name, func(b *testing.B) {
			runtime.GC()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = buildART(keys)
			}
		})
		b.Run("iradix/"+ds.name, func(b *testing.B) {
			runtime.GC()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = buildIradix(keys)
			}
		})
	}
}

// ---- Point lookups on a prebuilt tree ----

func BenchmarkCompareGet(b *testing.B) {
	for _, ds := range datasets(b, benchN) {
		keys := ds.keys
		// Fixed pseudo-random lookup order, identical for both trees.
		order := rand.New(rand.NewSource(1)).Perm(len(keys))

		b.Run("ART/"+ds.name, func(b *testing.B) {
			art := buildART(keys)
			runtime.GC()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, ok := art.Get(keys[order[i%len(order)]]); !ok {
					b.Fatal("ART missing key")
				}
			}
		})
		b.Run("iradix/"+ds.name, func(b *testing.B) {
			ir := buildIradix(keys)
			runtime.GC()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, ok := ir.Get(keys[order[i%len(order)]]); !ok {
					b.Fatal("iradix missing key")
				}
			}
		})
	}
}

// ---- Single-key insert into an existing tree (persistent update path) ----

func BenchmarkCompareUpdate(b *testing.B) {
	for _, ds := range datasets(b, benchN) {
		keys := ds.keys
		order := rand.New(rand.NewSource(2)).Perm(len(keys))

		b.Run("ART/"+ds.name, func(b *testing.B) {
			art := buildART(keys)
			runtime.GC()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				art, _, _ = art.Insert(keys[order[i%len(order)]], i)
			}
		})
		b.Run("iradix/"+ds.name, func(b *testing.B) {
			ir := buildIradix(keys)
			runtime.GC()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ir, _, _ = ir.Insert(keys[order[i%len(order)]], i)
			}
		})
	}
}

// ---- Ordered iteration over the whole tree ----

func BenchmarkCompareIterate(b *testing.B) {
	for _, ds := range datasets(b, benchN) {
		keys := ds.keys
		want := distinctCount(keys)

		b.Run("ART/"+ds.name, func(b *testing.B) {
			art := buildART(keys)
			runtime.GC()
			b.ReportAllocs()
			b.ResetTimer()
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
		b.Run("iradix/"+ds.name, func(b *testing.B) {
			ir := buildIradix(keys)
			runtime.GC()
			b.ReportAllocs()
			b.ResetTimer()
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
