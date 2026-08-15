// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

// Differential correctness tests for the optimized Get path. The rewritten
// iterativeSearch (single type-switch per level, SWAR Node16 search, direct
// child indexing) must agree exactly with a map oracle and with iradix across
// every key distribution the benchmarks use — otherwise a "faster" Get would be
// meaningless. Absent-key lookups are checked too, since the fast path returns
// early and must never report a false positive.

import (
	"math/rand"
	"testing"

	iradix "github.com/hashicorp/go-immutable-radix/v2"
)

func TestGetMatchesOracleAndIradix(t *testing.T) {
	const n = 5000
	for _, ds := range datasets(t, n) {
		ds := ds
		t.Run(ds.name, func(t *testing.T) {
			oracle := make(map[string]int, len(ds.keys))
			art := NewRadixTree[int]()
			itxn := iradix.New[int]().Txn()
			for i, k := range ds.keys {
				art, _, _ = art.Insert(k, i)
				itxn.Insert(k, i)
				oracle[string(k)] = i // last write wins, same as both trees
			}
			ir := itxn.Commit()

			if art.Len() != len(oracle) {
				t.Fatalf("ART Len()=%d, want %d distinct keys", art.Len(), len(oracle))
			}

			// Every present key resolves to the same value in all three.
			for k, want := range oracle {
				gotA, okA := art.Get([]byte(k))
				if !okA {
					t.Fatalf("ART missing present key %q", k)
				}
				if gotA != want {
					t.Fatalf("ART Get(%q)=%d, want %d", k, gotA, want)
				}
				gotI, okI := ir.Get([]byte(k))
				if okA != okI || gotA != gotI {
					t.Fatalf("ART/iradix disagree on %q: ART=(%d,%v) iradix=(%d,%v)", k, gotA, okA, gotI, okI)
				}
			}

			// Random absent keys must be reported missing by both.
			r := rand.New(rand.NewSource(7))
			for i := 0; i < 3000; i++ {
				kb := make([]byte, 1+r.Intn(12))
				for j := range kb {
					kb[j] = byte(r.Intn(256))
				}
				if _, ok := oracle[string(kb)]; ok {
					continue
				}
				if _, ok := art.Get(kb); ok {
					t.Fatalf("ART reported absent key %q as present", kb)
				}
				if _, ok := ir.Get(kb); ok {
					t.Fatalf("iradix reported absent key %q as present", kb)
				}
			}
		})
	}
}

// TestGetPrefixKeys exercises the specific case the sentinel/node-leaf machinery
// exists for: keys that are strict prefixes of other keys must each resolve to
// their own value, and a prefix that was never inserted must be absent.
func TestGetPrefixKeys(t *testing.T) {
	present := []string{"a", "ab", "abc", "abcd", "abcde", "b", "ba", "team", "tea", "te"}
	absent := []string{"", "abcdef", "abx", "t", "teamwork", "c"}

	art := NewRadixTree[int]()
	ir := iradix.New[int]().Txn()
	for i, k := range present {
		art, _, _ = art.Insert([]byte(k), i)
		ir.Insert([]byte(k), i)
	}
	irt := ir.Commit()

	for i, k := range present {
		v, ok := art.Get([]byte(k))
		if !ok || v != i {
			t.Fatalf("ART Get(%q)=(%d,%v), want (%d,true)", k, v, ok, i)
		}
		iv, iok := irt.Get([]byte(k))
		if iok != ok || iv != v {
			t.Fatalf("ART/iradix disagree on present %q: ART=(%d,%v) iradix=(%d,%v)", k, v, ok, iv, iok)
		}
	}
	for _, k := range absent {
		if v, ok := art.Get([]byte(k)); ok {
			t.Fatalf("ART Get(%q) unexpectedly present with %d", k, v)
		}
		if _, ok := irt.Get([]byte(k)); ok {
			t.Fatalf("iradix Get(%q) unexpectedly present", k)
		}
	}
}
