go-immutable-adaptive-radix [![Run CI Tests](https://github.com/absolutelightning/go-immutable-adaptive-radix/actions/workflows/ci.yaml/badge.svg)](https://github.com/absolutelightning/go-immutable-adaptive-radix/actions/workflows/ci.yaml)
=========

Provides the `adaptive` package that implements an immutable adaptive [radix tree](http://en.wikipedia.org/wiki/Radix_tree).
The package only provides a single `RadixTree` implementation, optimized for sparse nodes.

As a radix tree, it provides the following:
* O(k) operations. In many cases, this can be faster than a hash table since
  the hash function is an O(k) operation, and hash tables have very poor cache locality.
* Minimum / Maximum value lookups
* Ordered iteration

A tree supports using a transaction to batch multiple updates (insert, delete)
in a more efficient manner than performing each operation one at a time.

Documentation
=============

Example
=======

Below is a simple example of usage

```go
// Create a tree
r := adaptive.NewRadixTree[int]()
r, _, _ = r.Insert([]byte("foo"), 1)
r, _, _ = r.Insert([]byte("bar"), 2)
r, _, _ = r.Insert([]byte("foobar"), 2)

// Find the longest prefix match
m, _, _ := r.LongestPrefix([]byte("foozip"))
if string(m) != "foo" {
    panic("should be foo")
}

```

Here is an example of performing a range scan of the keys.

```go
// Create a tree
r := adaptive.NewRadixTree[int]()
r, _, _ = r.Insert([]byte("001"), 1)
r, _, _ = r.Insert([]byte("002"), 2)
r, _, _ = r.Insert([]byte("005"), 5)
r, _, _ = r.Insert([]byte("010"), 10)
r, _, _ = r.Insert([]byte("100"), 10)
```

Benchmarks
==========

`bench_compare_test.go` compares this adaptive radix tree (ART) against
[hashicorp/go-immutable-radix/v2](https://github.com/hashicorp/go-immutable-radix)
(iradix), a persistent binary radix tree. Both are immutable and generic, so the
comparison runs on identical key sets. Each tree is built inside its own
sub-benchmark with a GC forced before timing, so only the tree under test is live.

Run them with:

```
go test -bench 'Compare' -benchmem -run '^$'
```

The datasets span a range of node fanouts, which is where the two designs differ
most — iradix binary-searches its sorted edges at every node (`O(log fanout)`),
while ART indexes children directly (Node48/Node256) or with a branchless SWAR
compare (Node16):

* `uuid`  — 36-char hex, alphabet of 16 (low fanout)
* `words` — natural language (mixed, mostly low fanout)
* `seq`   — sequential big-endian uint64 (dense Node256 in the low bytes)
* `rand8` — random 8-byte keys (dense Node48/Node256 near the root)

Results below use 100k keys per dataset (ns/op for Get/Update, ms per full build
for Insert, ms per full walk for Iterate). Numbers are hardware- and
load-dependent; run the suite on your own machine. Lower is better; **bold**
marks the winner.

Measured on:

```
Machine : Apple M4, 10 cores, 16 GB RAM
OS      : macOS 26.3 (darwin/arm64)
Go      : go1.24.3
Library : hashicorp/go-immutable-radix/v2 v2.1.0
```

| Operation | dataset | ART | iradix |
|-----------|---------|-----|--------|
| Get       | uuid    | **277 ns**  | 300 ns |
| Get       | words   | **295 ns**  | 374 ns |
| Get       | seq     | **120 ns**  | 144 ns |
| Get       | rand8   | **160 ns**  | 205 ns |
| Insert    | uuid    | **30.9 ms** | 60.6 ms |
| Insert    | words   | **23.4 ms** | 36.4 ms |
| Insert    | seq     | **13.0 ms** | 14.3 ms |
| Insert    | rand8   | **21.8 ms** | 41.8 ms |
| Update    | uuid    | **1420 ns** | 1810 ns |
| Update    | words   | 2391 ns     | **2350 ns** |
| Update    | seq     | 3043 ns     | **2800 ns** |
| Update    | rand8   | 3643 ns     | **2787 ns** |
| Iterate   | uuid    | **2.91 ms** | 3.09 ms |
| Iterate   | words   | **1.28 ms** | 1.48 ms |
| Iterate   | seq     | 0.43 ms     | **0.33 ms** |
| Iterate   | rand8   | 2.32 ms     | **1.62 ms** |

Takeaways:

* **Get** — ART is faster on every distribution, and the gap widens with fanout
  (up to ~22% on dense keys), where its direct child indexing beats a per-node
  binary search. Lookups are allocation-free.
* **Insert** — ART is faster everywhere (up to ~2×). Being persistent, an insert
  copies every node on the root-to-leaf path; ART allocates about half as many
  objects per copied node (no eager mutation channel, children stored inline
  rather than in a separately-allocated slice).
* **Update / Iterate** — ART wins on low-fanout (realistic) keys but loses on
  synthetic dense (Node256) trees, where the inline 256-entry child arrays are
  costly to clone (Update) and to scan slot-by-slot (Iterate).
