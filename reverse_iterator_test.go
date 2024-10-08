package adaptive

import (
	"fmt"
	"sort"
	"testing"
	"testing/quick"

	"golang.org/x/exp/slices"
)

func TestReverseIterator_SeekReverseLowerBoundFuzz(t *testing.T) {
	r := NewRadixTree[any]()
	var set []string

	// This specifies a property where each call adds a new random key to the radix
	// tree.
	//
	// It also maintains a plain sorted list of the same set of keys and asserts
	// that iterating from some random key to the beginning using ReverseLowerBound
	// produces the same list as filtering all sorted keys that are bigger.

	radixAddAndScan := func(newKey, searchKey readableString) []string {
		r, _, _ = r.Insert([]byte(newKey), nil)

		// Now iterate the tree from searchKey to the beginning
		it := r.Root().ReverseIterator()
		var result []string
		it.SeekReverseLowerBound([]byte(searchKey))
		for {
			key, _, ok := it.Previous()
			if !ok {
				break
			}
			result = append(result, string(key))
		}
		return result
	}

	sliceAddSortAndFilter := func(newKey, searchKey readableString) []string {
		// Append the key to the set and re-sort
		set = append(set, string(newKey))
		sort.Strings(set)

		t.Logf("Current Set: %#v", set)
		t.Logf("Search Key: %#v %v", searchKey, "" >= string(searchKey))

		var result []string
		for i := len(set) - 1; i >= 0; i-- {
			k := set[i]
			// Check this is not a duplicate of the previous value we just included.
			// Note we don't just store the last string to compare because empty
			// string is a valid value in the set and makes comparing on the first
			// iteration awkward.
			if i < len(set)-1 && set[i+1] == k {
				continue
			}
			if k <= string(searchKey) {
				result = append(result, k)
			}
		}
		return result
	}

	if err := quick.CheckEqual(radixAddAndScan, sliceAddSortAndFilter, &quick.Config{
		MaxCount: 1000,
	}); err != nil {
		t.Error(err)
	}
}

func TestReverseIterator_SeekLowerBound(t *testing.T) {

	// these should be defined in order
	var fixedLenKeys = []string{
		"20020",
		"00020",
		"00010",
		"00004",
		"00001",
		"00000",
	}

	// these should be defined in order
	var mixedLenKeys = []string{
		"zip",
		"zap",
		"found",
		"foo",
		"f",
		"barbazboo",
		"abc",
		"a1",
	}

	type exp struct {
		keys   []string
		search string
		want   []string
	}
	cases := []exp{
		{
			fixedLenKeys,
			"20020",
			fixedLenKeys,
		},
		{
			fixedLenKeys,
			"20000",
			[]string{
				"00020",
				"00010",
				"00004",
				"00001",
				"00000",
			},
		},
		{
			fixedLenKeys,
			"00010",
			[]string{
				"00010",
				"00004",
				"00001",
				"00000",
			},
		},
		{
			fixedLenKeys,
			"00000",
			[]string{
				"00000",
			},
		},
		{
			fixedLenKeys,
			"0",
			[]string{},
		},
		{
			mixedLenKeys,
			"{", // after all lower case letters
			mixedLenKeys,
		},
		{
			mixedLenKeys,
			"zip",
			mixedLenKeys,
		},
		{
			mixedLenKeys,
			"b",
			[]string{
				"abc",
				"a1",
			},
		},
		{
			mixedLenKeys,
			"barbazboo0",
			[]string{
				"barbazboo",
				"abc",
				"a1",
			},
		},
		{
			mixedLenKeys,
			"a",
			[]string{},
		},
		{
			mixedLenKeys,
			"a1",
			[]string{
				"a1",
			},
		},

		// We SHOULD support keys that are prefixes of each other despite some
		// confusion in the original implementation.
		{
			[]string{"f", "fo", "foo", "food", "bug"},
			"foo",
			[]string{"foo", "fo", "f", "bug"},
		},
		{
			[]string{"f", "fo", "foo", "food", "bug"},
			"foozzzzzzzzzz", // larger than any key but with shared prefix
			[]string{"food", "foo", "fo", "f", "bug"},
		},

		// We also support the empty key (which is a prefix of every other key) as a
		// valid key to insert and search for.
		{
			[]string{"f", "fo", "foo", "food", "bug", ""},
			"foo",
			[]string{"foo", "fo", "f", "bug", ""},
		},
		{
			[]string{"f", "bug", ""},
			"",
			[]string{""},
		},
		{
			[]string{"f", "bug", "xylophone"},
			"",
			[]string{},
		},

		// This case could panic before. it involves a node with a shared prefix and
		// children where the reverse lower bound is greater than all the children
		{
			[]string{"foo00", "foo11"},
			"foo",
			[]string{},
		},

		// When fixing the panic above the above test could pass but we need to
		// verify the logic is still correct in the case there was a lower bound in
		// another node.
		{
			[]string{"bar", "foo00", "foo11"},
			"foo",
			[]string{"bar"},
		},

		// Found by fuzz test that hit code that wasn't covered by any other example
		// here.
		{
			[]string{"bdgedcdc", "agcbcaba"},
			"beefdafg",
			[]string{"bdgedcdc", "agcbcaba"},
		},
		{
			[]string{"", "acc", "accea", "accgbbb", "b", "bdebfc", "bdfdcbb", "becccc", "bgefcfc", "c", "cab", "cbd", "cgeaff", "cggfbcb", "cggge", "dcgbd", "ddd", "decfd", "dgb", "e", "edaffec", "ee", "eedc", "efafdbd", "eg", "egf", "egfcd", "f", "fggfdad", "g", "gageecc", "ggd"},
			"adgba",
			[]string{"accgbbb", "accea", "acc", ""},
		},
		{
			[]string{"", "aa", "aacbdbc", "abe", "adbecg", "bfe", "cacaadc", "cbcafd", "cd", "cegbec", "daecbg", "db", "dbfgc", "dcabbbb", "dedadeb", "dfe", "ebeeccc", "ed", "edddf", "eggef", "faffc", "fbgb", "fga", "fggc", "g", "gaffdf", "gag", "gggcea"},
			"gc",
			[]string{"gag", "gaffdf", "g", "fggc", "fga", "fbgb", "faffc", "eggef", "edddf", "ed", "ebeeccc", "dfe", "dedadeb", "dcabbbb", "dbfgc", "db", "daecbg", "cegbec", "cd", "cbcafd", "cacaadc", "bfe", "adbecg", "abe", "aacbdbc", "aa", ""},
		},
		{
			[]string{"acefb", "cafega", "cccg", "cd"},
			"gcffb",
			[]string{"cd", "cccg", "cafega", "acefb"},
		},
		{
			[]string{"af", "afa", "ba", "bacf", "c", "ceed", "cfbedd", "d", "dafbfe", "dc", "faeeb", "fdf", "gafce", "gdcdded", "geb"},
			"bgaegd",
			[]string{"bacf", "ba", "afa", "af"},
		},
		{
			[]string{"",
				"a",
				"aaafbb",
				"ab",
				"abcggbg",
				"acbb",
				"ag",
				"b",
				"bcccea",
				"bcdg",
				"bcgefa",
				"bffeea",
				"bfgccg",
				"bg",
				"c",
				"cafae",
				"cbddbf",
				"ccbeec",
				"cdd",
				"ceegb",
				"cf",
				"d",
				"ddebeeg",
				"de",
				"decb",
				"defccd",
				"e",
				"eaaddb",
				"ecccfd",
				"edbaa",
				"efacgg",
				"egba",
				"egd",
				"facccdc",
				"fbefb",
				"fccbfc",
				"fgcdef",
				"fgcggg",
				"fgg",
				"gab",
				"gada",
			},
			"gga",
			[]string{"gada", "gab", "fgg", "fgcggg", "fgcdef", "fccbfc", "fbefb", "facccdc", "egd", "egba", "efacgg", "edbaa", "ecccfd", "eaaddb", "e", "defccd", "decb", "de", "ddebeeg", "d", "cf", "ceegb", "cdd", "ccbeec", "cbddbf", "cafae", "c", "bg", "bfgccg", "bffeea", "bcgefa", "bcdg", "bcccea", "b", "ag", "acbb", "abcggbg", "ab", "aaafbb", "a", ""},
		},
		{
			[]string{"aagcfgc", "acbcdca", "aceeaca", "ad", "aefab", "afdcec", "b", "badcf", "bbag", "bccdegd", "cafg", "cbb", "ccaagef", "daae", "dabdbb", "dbbgb", "dbcaca", "fbeaab", "ffeec", "fg", "ga", "gbc", "gdcg", "gec", "gecga", "gfa", "gfad"},
			"gg",
			[]string{"gfad", "gfa", "gecga", "gec", "gdcg", "gbc", "ga", "fg", "ffeec", "fbeaab", "dbcaca", "dbbgb", "dabdbb", "daae", "ccaagef", "cbb", "cafg", "bccdegd", "bbag", "badcf", "b", "afdcec", "aefab", "ad", "aceeaca", "acbcdca", "aagcfgc"},
		},
	}

	for idx, test := range cases {
		t.Run(fmt.Sprintf("case%03d", idx), func(t *testing.T) {
			r := NewRadixTree[any]()

			// Insert keys
			for _, k := range test.keys {
				var ok bool
				r, _, _ = r.Insert([]byte(k), nil)
				if ok {
					t.Fatalf("duplicate key %s in keys", k)
				}
			}
			if r.Len() != len(test.keys) {
				t.Fatal("failed adding keys")
			}
			// Get and seek iterator
			root := r.root
			iter := root.ReverseIterator()
			iter.SeekReverseLowerBound([]byte(test.search))

			// Consume all the keys
			var out []string
			for {
				key, _, ok := iter.Previous()
				if !ok {
					break
				}
				out = append(out, string(key))
			}
			if !slices.Equal(test.want, out) {
				t.Fatalf("mis-match: key=%s\n  got=%v\n  want=%v", test.search,
					out, test.want)
			}
		})
	}
}

func TestReverseIterator_SeekPrefix(t *testing.T) {
	r := NewRadixTree[any]()
	keys := []string{"001", "002", "005", "010", "100"}
	for _, k := range keys {
		r, _, _ = r.Insert([]byte(k), nil)
	}

	cases := []struct {
		name         string
		prefix       string
		expectResult bool
	}{
		{
			name:         "existing prefix",
			prefix:       "005",
			expectResult: true,
		},
		{
			name:         "non-existing prefix",
			prefix:       "2",
			expectResult: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			it := r.Root().ReverseIterator()
			it.SeekPrefix([]byte(c.prefix))

			if c.expectResult && it.i.node == nil {
				t.Errorf("expexted prefix %s to exist", c.prefix)
				return
			}

			//if !c.expectResult && it.i.node != nil {
			//	t.Errorf("unexpected node for prefix '%s'", c.prefix)
			//	return
			//}
		})
	}
}

func TestReverseIterator_SeekPrefixWatch(t *testing.T) {
	key := []byte("key")

	// Create tree
	r := NewRadixTree[any]()
	r, _, _ = r.Insert(key, nil)

	// Find mutate channel
	it := r.Root().ReverseIterator()
	ch := it.SeekPrefixWatch(key)

	// Change prefix
	tx := r.Txn()
	tx.TrackMutate(true)
	tx.Insert(key, "value")
	tx.Commit()

	// Check if channel closed
	select {
	case <-ch:
	default:
		t.Errorf("channel not closed")
	}
}

func TestReverseIterator_Previous(t *testing.T) {
	r := NewRadixTree[any]()
	keys := []string{"001", "002", "005", "010", "100"}
	for _, k := range keys {
		r, _, _ = r.Insert([]byte(k), nil)
	}

	it := r.Root().ReverseIterator()

	for i := len(keys) - 1; i >= 0; i-- {
		got, _, _ := it.Previous()
		want := keys[i]

		if string(got) != want {
			t.Errorf("got: %v, want: %v", got, want)
		}
	}
}
