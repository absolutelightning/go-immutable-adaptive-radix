package adaptive

import (
	"fmt"
	"slices"
	"sort"
	"testing"
	"testing/quick"
)

type readableString string

func TestIterateLowerBoundFuzz(t *testing.T) {
	r := NewRadixTree[any]()
	var set []string

	// This specifies a property where each call adds a new random key to the radix
	// tree.
	//
	// It also maintains a plain sorted list of the same set of keys and asserts
	// that iterating from some random key to the end using LowerBound produces
	// the same list as filtering all sorted keys that are lower.

	radixAddAndScan := func(newKey, searchKey readableString) []string {
		r.Insert([]byte(newKey), nil)

		t.Logf("NewKey: %q, SearchKey: %q", newKey, searchKey)

		// Now iterate the tree from searchKey to the end
		it := r.root.Iterator()
		var result []string
		it.SeekLowerBound([]byte(searchKey))
		for {
			key, _, ok := it.Next()
			if !ok {
				break
			}
			result = append(result, string(key))
		}
		t.Logf("Radix Set: %#v", result)
		return result
	}

	sliceAddSortAndFilter := func(newKey, searchKey readableString) []string {
		// Append the key to the set and re-sort
		set = append(set, string(newKey))
		sort.Strings(set)

		t.Logf("Current Set: %#v", set)
		t.Logf("Search Key: %#v %v", searchKey, "" >= string(searchKey))

		var result []string
		for i, k := range set {
			// Check this is not a duplicate of the previous value. Note we don't just
			// store the last string to compare because empty string is a valid value
			// in the set and makes comparing on the first iteration awkward.
			if i > 0 && set[i-1] == k {
				continue
			}
			if k >= string(searchKey) {
				result = append(result, k)
			}
		}
		t.Logf("Filtered Set: %#v", result)
		return result
	}

	if err := quick.CheckEqual(radixAddAndScan, sliceAddSortAndFilter, nil); err != nil {
		t.Error(err)
	}
}

func TestIterateLowerBound(t *testing.T) {

	// these should be defined in order
	var fixedLenKeys = []string{
		"00000",
		"00001",
		"00004",
		"00010",
		"00020",
		"20020",
	}

	// these should be defined in order
	var mixedLenKeys = []string{
		"a1",
		"abc",
		"barbazboo",
		"f",
		"foo",
		"found",
		"zap",
		"zip",
	}

	type exp struct {
		keys   []string
		search string
		want   []string
	}
	cases := []exp{
		{
			fixedLenKeys,
			"00000",
			fixedLenKeys,
		},
		{
			fixedLenKeys,
			"00003",
			[]string{
				"00004",
				"00010",
				"00020",
				"20020",
			},
		},
		{
			fixedLenKeys,
			"00010",
			[]string{
				"00010",
				"00020",
				"20020",
			},
		},
		{
			fixedLenKeys,
			"20000",
			[]string{
				"20020",
			},
		},
		{
			fixedLenKeys,
			"20020",
			[]string{
				"20020",
			},
		},
		{
			fixedLenKeys,
			"20022",
			[]string{},
		},
		{
			mixedLenKeys,
			"A", // before all lower case letters
			mixedLenKeys,
		},
		{
			mixedLenKeys,
			"a1",
			mixedLenKeys,
		},
		{
			mixedLenKeys,
			"b",
			[]string{
				"barbazboo",
				"f",
				"foo",
				"found",
				"zap",
				"zip",
			},
		},
		{
			mixedLenKeys,
			"bar",
			[]string{
				"barbazboo",
				"f",
				"foo",
				"found",
				"zap",
				"zip",
			},
		},
		{
			mixedLenKeys,
			"barbazboo0",
			[]string{
				"f",
				"foo",
				"found",
				"zap",
				"zip",
			},
		},
		{
			mixedLenKeys,
			"zippy",
			[]string{},
		},
		{
			mixedLenKeys,
			"zi",
			[]string{
				"zip",
			},
		},

		// This is a case found by TestIterateLowerBoundFuzz simplified by hand. The
		// lowest node should be the first, but it is split on the same char as the
		// second char in the search string. My initial implementation didn't take
		// that into account (i.e. propagate the fact that we already know we are
		// greater than the input key into the recursion). This would skip the first
		// result.
		{
			[]string{
				"bb",
				"bc",
			},
			"ac",
			[]string{"bb", "bc"},
		},

		// This is a case found by TestIterateLowerBoundFuzz.
		{
			[]string{"aaaba", "aabaa", "aabab", "aabcb", "aacca", "abaaa", "abacb", "abbcb", "abcaa", "abcba", "abcbb", "acaaa", "acaab", "acaac", "acaca", "acacb", "acbaa", "acbbb", "acbcc", "accca", "babaa", "babcc", "bbaaa", "bbacc", "bbbab", "bbbac", "bbbcc", "bbcab", "bbcca", "bbccc", "bcaac", "bcbca", "bcbcc", "bccac", "bccbc", "bccca", "caaab", "caacc", "cabac", "cabbb", "cabbc", "cabcb", "cacac", "cacbc", "cacca", "cbaba", "cbabb", "cbabc", "cbbaa", "cbbab", "cbbbc", "cbcbb", "cbcbc", "cbcca", "ccaaa", "ccabc", "ccaca", "ccacc", "ccbac", "cccaa", "cccac", "cccca"},
			"cbacb",
			[]string{"cbbaa", "cbbab", "cbbbc", "cbcbb", "cbcbc", "cbcca", "ccaaa", "ccabc", "ccaca", "ccacc", "ccbac", "cccaa", "cccac", "cccca"},
		},

		// Panic case found be TestIterateLowerBoundFuzz.
		{
			[]string{"gcgc"},
			"",
			[]string{"gcgc"},
		},

		// We SHOULD support keys that are prefixes of each other despite some
		// confusion in the original implementation.
		{
			[]string{"f", "fo", "foo", "food", "bug"},
			"foo",
			[]string{"foo", "food"},
		},

		// We also support the empty key (which is a prefix of every other key) as a
		// valid key to insert and search for.
		{
			[]string{"f", "fo", "foo", "food", "bug", ""},
			"foo",
			[]string{"foo", "food"},
		},
		{
			[]string{"f", "bug", ""},
			"",
			[]string{"", "bug", "f"},
		},
		{
			[]string{"f", "bug", "xylophone"},
			"",
			[]string{"bug", "f", "xylophone"},
		},

		// This is a case we realized we were not covering while fixing
		// SeekReverseLowerBound and could panic before.
		{
			[]string{"bar", "foo00", "foo11"},
			"foo",
			[]string{"foo00", "foo11"},
		},
	}

	for idx, test := range cases {
		t.Run(fmt.Sprintf("case%03d", idx), func(t *testing.T) {
			r := NewRadixTree[any]()

			// Insert keys
			for _, k := range test.keys {
				var ok bool
				r.Insert([]byte(k), nil)
				if ok {
					t.Fatalf("duplicate key %s in keys", k)
				}
			}
			if r.Len() != len(test.keys) {
				t.Fatal("failed adding keys")
			}
			// Get and seek iterator
			root := r.root
			iter := root.Iterator()
			iter.SeekLowerBound([]byte(test.search))

			// Consume all the keys
			var out []string
			for {
				key, _, ok := iter.Next()
				if !ok {
					break
				}
				out = append(out, string(key))
			}
			if !slices.Equal(out, test.want) {
				t.Fatalf("mis-match: key=%s\n  got=%v\n  want=%v", test.search,
					out, test.want)
			}
		})
	}
}
