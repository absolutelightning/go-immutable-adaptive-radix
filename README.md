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
| Get       | uuid    | 245 ns      | **222 ns** |
| Get       | words   | **208 ns**  | 226 ns |
| Get       | seq     | **71 ns**   | 116 ns |
| Get       | rand8   | **101 ns**  | 155 ns |
| Insert    | uuid    | **25.4 ms** | 48.9 ms |
| Insert    | words   | **21.6 ms** | 32.4 ms |
| Insert    | seq     | **11.1 ms** | 12.2 ms |
| Insert    | rand8   | **18.1 ms** | 35.8 ms |
| Update    | uuid    | **1229 ns** | 1543 ns |
| Update    | words   | 2091 ns     | **2026 ns** |
| Update    | seq     | 2540 ns     | **2399 ns** |
| Update    | rand8   | 3431 ns     | **2420 ns** |
| Iterate   | uuid    | 1.79 ms     | **1.27 ms** |
| Iterate   | words   | **0.96 ms** | 0.95 ms |
| Iterate   | seq     | 0.33 ms     | **0.26 ms** |
| Iterate   | rand8   | 1.57 ms     | **0.91 ms** |

Takeaways:

* **Get** — allocation-free, and faster than iradix on `words` and dramatically
  faster on the high-fanout `seq`/`rand8` sets (~35–40%), where ART indexes
  children directly instead of binary-searching edges. On low-fanout `uuid` (deep
  16-way tree) iradix's longer per-node prefixes mean fewer nodes to walk, so it
  edges ahead by ~10%.
* **Insert** — ART is faster everywhere (up to ~2×). Being persistent, an insert
  copies every node on the root-to-leaf path; ART allocates about half as many
  objects per copied node (no eager mutation channel, children stored inline
  rather than in a separately-allocated slice).
* **Update / Iterate** — ART wins on low-fanout (realistic) keys but loses on
  synthetic dense (Node256) trees. The inline 256-entry child arrays that make
  Get and Insert cheap are the flip side here: costly to clone on Update, and to
  scan slot-by-slot (plus a wide frontier stack) on Iterate.
