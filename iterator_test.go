package adaptive

import (
	"fmt"
	"golang.org/x/exp/slices"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"testing/quick"
)

type readableString string

func (s readableString) Generate(rand *rand.Rand, size int) reflect.Value {
	// Pick a random string from a limited alphabet that makes it easy to read the
	// failure cases.
	const letters = "abcdefg"

	// Ignore size and make them all shortish to provoke bigger chance of hitting
	// prefixes and more intersting tree shapes.
	size = rand.Intn(8)

	b := make([]byte, size)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return reflect.ValueOf(readableString(b))
}

func TestIterateLowerBoundFuzz(t *testing.T) {
	r := NewRadixTree[string]()
	var set []string

	// This specifies a property where each call adds a new random key to the radix
	// tree.
	//
	// It also maintains a plain sorted list of the same set of keys and asserts
	// that iterating from some random key to the end using LowerBound produces
	// the same list as filtering all sorted keys that are lower.

	radixAddAndScan := func(newKey, searchKey readableString) []string {
		r, _, _ = r.Insert([]byte(newKey), "")

		t.Log("NewKey: ", newKey, "SearchKey: ", searchKey)

		// Now iterate the tree from searchKey to the end
		it := r.Root().LowerBoundIterator()
		var result []string
		it.SeekLowerBound([]byte(searchKey))
		for {
			key, _, ok := it.Next()
			if !ok {
				break
			}
			result = append(result, string(key))
		}
		t.Log("Radix Set: ", result)
		return result
	}

	sliceAddSortAndFilter := func(newKey, searchKey readableString) []string {
		// Append the key to the set and re-sort
		set = append(set, string(newKey))
		sort.Strings(set)

		t.Log("Current Set: ", set)
		t.Log("Search Key: ", searchKey, "" >= string(searchKey))

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
		t.Log("Filtered Set: ", result)
		return result
	}

	if err := quick.CheckEqual(radixAddAndScan, sliceAddSortAndFilter, &quick.Config{
		MaxCount: 1000,
	}); err != nil {
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
		{
			[]string{"deaaa", "deabb", "fcdbbbb"},
			"",
			[]string{"deaaa", "deabb", "fcdbbbb"},
		},
		{
			[]string{"fd", "fddg", "gbcf", "gcdbgg", "gdffbb"},
			"fcb",
			[]string{"fd", "fddg", "gbcf", "gcdbgg", "gdffbb"},
		},
		{
			[]string{"eef", "efafcb", "fb", "fgbga"},
			"ag",
			[]string{"eef", "efafcb", "fb", "fgbga"},
		},
		{
			[]string{"age", "bga", "ccb", "ccfde", "fedggad", "gaa", "gaed", "gdbfc", "geagce"},
			"ggcd",
			[]string(nil),
		},
		{
			[]string{"a", "afab", "dbg", "ecfdfbg", "gc"},
			"",
			[]string{"a", "afab", "dbg", "ecfdfbg", "gc"},
		},
		{
			[]string{"a", "abcdgfc", "abff", "agefdf", "b", "bbfbdf", "be", "bedaa", "cb", "cd", "cfc", "daa", "dcafb", "dcf", "deabfa", "degab", "df", "dge", "ea", "eagbffe", "ec", "efbbdeg", "f", "fbceag", "fffbgfb", "gbc", "gbge", "gggbaa"},
			"dbfbad",
			[]string{"dcafb", "dcf", "deabfa", "degab", "df", "dge", "ea", "eagbffe", "ec", "efbbdeg", "f", "fbceag", "fffbgfb", "gbc", "gbge", "gggbaa"},
		},
		{
			[]string{"cda", "cdacg", "deeadeg"},
			"bgfged",
			[]string{"cda", "cdacg", "deeadeg"},
		},
		{
			[]string{"aecfd", "b", "ddf", "dfae"},
			"adad",
			[]string{"aecfd", "b", "ddf", "dfae"},
		},
		{
			[]string{"", "a", "abdd", "aedd", "b", "bcc", "bcg", "bffg", "bgfa", "cadcefa", "cbfg", "cc", "ced", "cfddfc", "da", "dabca", "dad", "dcccbag", "ddcd", "de", "e", "eab", "eaebg", "ebaedbb", "ecd", "ee", "effgecf", "egfafga", "fef", "fffe", "g", "gccef", "gg"},
			"",
			[]string{"", "a", "abdd", "aedd", "b", "bcc", "bcg", "bffg", "bgfa", "cadcefa", "cbfg", "cc", "ced", "cfddfc", "da", "dabca", "dad", "dcccbag", "ddcd", "de", "e", "eab", "eaebg", "ebaedbb", "ecd", "ee", "effgecf", "egfafga", "fef", "fffe", "g", "gccef", "gg"},
		},
		{
			[]string{"fcg", "g", "ga", "ggbab"},
			"eecdce",
			[]string{"fcg", "g", "ga", "ggbab"},
		},
		{
			[]string{"bfdgcfe", "ca", "dbf"},
			"a",
			[]string{"bfdgcfe", "ca", "dbf"},
		},
		{
			[]string{"a", "abcbad", "acgcfcd", "afadb", "ageafge", "agecd", "bcce", "becgcda", "bfbg", "cbece", "cbgebaa", "cef", "d", "eaee", "f", "fcccfdb", "ffbdf", "geacded", "geccfff", "gefdgaf"},
			"",
			[]string{"a", "abcbad", "acgcfcd", "afadb", "ageafge", "agecd", "bcce", "becgcda", "bfbg", "cbece", "cbgebaa", "cef", "d", "eaee", "f", "fcccfdb", "ffbdf", "geacded", "geccfff", "gefdgaf"},
		},
		{
			[]string{"g", "ecfa", "fccdcab", "ffb", "gbcdab", "ge", "gegaa", "ggfd"},
			"ece",
			[]string{"ecfa", "fccdcab", "ffb", "g", "gbcdab", "ge", "gegaa", "ggfd"},
		},
		{
			[]string{"ebfbbda", "eedg", "fcgcfgd", "fgbgefa", "g", "gfab", "e"},
			"dfcda",
			[]string{"e", "ebfbbda", "eedg", "fcgcfgd", "fgbgefa", "g", "gfab"},
		},
		{
			[]string{"eace", "cbecg", "eacee", "eadfcdc", "eadgbc", "eafga", "ebbggd", "ebdcd", "ecc", "ececb", "ecefaef", "edbffd", "edbge", "eddd", "ede", "edeb", "edgbgf", "ee", "eedee", "ef", "efa", "efadaf", "effb", "efg", "eg", "egcbbd", "egdcaff", "f", "fa", "fadbff", "fagfec", "fbgfaf", "fcadb", "fcde", "fcegb", "fcfbfgd", "fd", "fdagf", "fdc", "fdcbf", "fdfb", "fdgc", "fefb", "ff", "ffce", "ffd", "ffec", "ffeef", "ffeg", "fff", "fgbde", "fgdbebc", "fgegaf", "g", "gaaff", "gab", "gaeddc", "gbaead", "gbbgcgf", "gbcdbac", "gbef", "gcf", "gddb", "ge", "gea", "geaga", "gebe", "gecgae", "gfb", "gfeba", "gfgeecd", "gg", "ggdac"},
			"eaccfd",
			[]string{"eace", "eacee", "eadfcdc", "eadgbc", "eafga", "ebbggd", "ebdcd", "ecc", "ececb", "ecefaef", "edbffd", "edbge", "eddd", "ede", "edeb", "edgbgf", "ee", "eedee", "ef", "efa", "efadaf", "effb", "efg", "eg", "egcbbd", "egdcaff", "f", "fa", "fadbff", "fagfec", "fbgfaf", "fcadb", "fcde", "fcegb", "fcfbfgd", "fd", "fdagf", "fdc", "fdcbf", "fdfb", "fdgc", "fefb", "ff", "ffce", "ffd", "ffec", "ffeef", "ffeg", "fff", "fgbde", "fgdbebc", "fgegaf", "g", "gaaff", "gab", "gaeddc", "gbaead", "gbbgcgf", "gbcdbac", "gbef", "gcf", "gddb", "ge", "gea", "geaga", "gebe", "gecgae", "gfb", "gfeba", "gfgeecd", "gg", "ggdac"},
		},
		{
			[]string{"dbacgg", "gfg", "gfggefe", "ggggde"},
			"gfd",
			[]string{"gfg", "gfggefe", "ggggde"},
		},
		{
			[]string{"gccdcc", "gcc", "gccdf", "gcdbdde", "gcdgac", "gcea", "gcebb", "gcedg", "gcefbgd", "gcf", "gcgaff", "gcgddgg", "gd", "gda", "gdaefc", "gdbebe", "gdbeffd", "gdceb", "gdcefcd", "ge", "gebeag", "gecd", "gecead", "gede", "geeaff", "gefacgc", "gefag", "geg", "gf", "gfa", "gfc", "gfca", "gfe", "gfeg", "gfg", "gfgcbg", "gfgdae", "gga", "ggadd", "ggafdbd", "ggbad", "ggbgadg", "ggfec", "ggffeb", "gggce"},
			"gcafede",
			[]string{"gcc", "gccdcc", "gccdf", "gcdbdde", "gcdgac", "gcea", "gcebb", "gcedg", "gcefbgd", "gcf", "gcgaff", "gcgddgg", "gd", "gda", "gdaefc", "gdbebe", "gdbeffd", "gdceb", "gdcefcd", "ge", "gebeag", "gecd", "gecead", "gede", "geeaff", "gefacgc", "gefag", "geg", "gf", "gfa", "gfc", "gfca", "gfe", "gfeg", "gfg", "gfgcbg", "gfgdae", "gga", "ggadd", "ggafdbd", "ggbad", "ggbgadg", "ggfec", "ggffeb", "gggce"},
		},
		{
			[]string{"g", "gfaabb"},
			"g",
			[]string{"g", "gfaabb"},
		},
		{
			[]string{"adee", "ccb", "d", "ecf", "ecgd", "ggac"},
			"fd",
			[]string{"ggac"},
		},
		{
			[]string{"aa",
				"aagbe",
				"aged",
				"bbbdef",
				"bgce",
				"c",
				"cd",
				"cedad",
				"cfd",
				"cfg",
				"cgcb",
				"cggcc",
				"e",
				"eadgfcf",
				"ebeefdd",
				"egegff",
				"f",
				"fbfg",
				"fcbfbba",
				"g",
				"ge",
				"gfff"},
			"gbe",
			[]string{"ge", "gfff"},
		},
		{
			[]string{"ee", "eeedd", "eeegd", "efbb", "eg", "fbaacf", "fbffgg", "fced", "fcedfba", "ffbaef", "ffdcgfe", "ffdf", "fgadb", "gacbbec", "gafe", "gbb", "gcfbaab", "gf"},
			"edcffef",
			[]string{"ee", "eeedd", "eeegd", "efbb", "eg", "fbaacf", "fbffgg", "fced", "fcedfba", "ffbaef", "ffdcgfe", "ffdf", "fgadb", "gacbbec", "gafe", "gbb", "gcfbaab", "gf"},
		},
		{
			[]string{"e", "egcde", "fc", "fgagga"},
			"dcebed",
			[]string{"e", "egcde", "fc", "fgagga"},
		},
		{
			[]string{"ebg", "fcbfa", "ffgc", "gbbed", "gbdaa", "gda"},
			"ebebadd",
			[]string{"ebg", "fcbfa", "ffgc", "gbbed", "gbdaa", "gda"},
		},
		{
			[]string{"dg", "dgc", "dgcf", "dgefddf", "dgge", "e", "eaagcga", "ebaab", "ec", "ecbbda", "ecc", "ecfce", "edb", "edbdadf", "edbffcb", "eed", "eeecaf", "efcbfc", "effab", "f", "faa", "fafe", "fag", "fagbab", "fbddbd", "fbf", "fc", "fcc", "fd", "fdd", "fdfddga", "fdfgebf", "fdgbfdd", "fef", "fff", "ffgbe", "fgddd", "fggac", "g", "ga", "gadaa", "gadbb", "gaggafb", "gb", "gbafd", "gbceag", "gbe", "gbffdc", "gc", "gcba", "gcgag", "gd", "gddadgd", "gddbae", "gde", "gegg", "ggbffg", "ggecdb"},
			"dfbgcg",
			[]string{"dg", "dgc", "dgcf", "dgefddf", "dgge", "e", "eaagcga", "ebaab", "ec", "ecbbda", "ecc", "ecfce", "edb", "edbdadf", "edbffcb", "eed", "eeecaf", "efcbfc", "effab", "f", "faa", "fafe", "fag", "fagbab", "fbddbd", "fbf", "fc", "fcc", "fd", "fdd", "fdfddga", "fdfgebf", "fdgbfdd", "fef", "fff", "ffgbe", "fgddd", "fggac", "g", "ga", "gadaa", "gadbb", "gaggafb", "gb", "gbafd", "gbceag", "gbe", "gbffdc", "gc", "gcba", "gcgag", "gd", "gddadgd", "gddbae", "gde", "gegg", "ggbffg", "ggecdb"},
		},
		{
			[]string{"ad", "adbfda", "ae", "aegffaf", "affcbae", "b", "baafb", "bacbdca", "bb", "bg", "bgfgag", "c", "cc", "ccd", "cdg", "ce", "cfca", "d", "deef", "e", "eadefa", "f", "fb", "fccdaa", "gaf"},
			"ac",
			[]string{"ad", "adbfda", "ae", "aegffaf", "affcbae", "b", "baafb", "bacbdca", "bb", "bg", "bgfgag", "c", "cc", "ccd", "cdg", "ce", "cfca", "d", "deef", "e", "eadefa", "f", "fb", "fccdaa", "gaf"},
		},
		{
			[]string{"a", "cddbdcc", "cddff", "cdfagd", "cdg", "ceaabeb", "cef", "cefa", "cf", "cfbaa", "cfegd", "cg", "d", "dbdaf", "dbf", "dbg", "dc", "dcadbfd", "dcceaeb", "ddbf", "dddcf", "defeff", "dfagcdc", "dfc", "dfd", "dgda", "dgdc", "dgea", "dgefgg", "e", "eaagb", "ead", "eae", "eb", "ebaeefa", "ebd", "ebg", "eca", "ecadf", "eccf", "ecffd", "ecg", "eebfaba", "eeceb", "efe", "efgbabf", "efgdadf", "egegcgc", "f", "fabe", "fad", "faga", "fbcae", "fbcege", "fbeeebe", "fc", "fcab", "fcgffc", "fddd", "fe", "feaf", "feefeda", "ffb", "fffag", "ffffc", "fgaacca", "fgcbb", "fgcec", "g", "gb", "gbcgc", "gbfcbbb", "gccabee", "gcccg", "gdbaeea", "ge", "gecebfc", "gefe", "gfaeabg", "gfbbgbf", "gfgbde", "ggd", "ggdeae"},
			"cdbce",
			[]string{"cddbdcc", "cddff", "cdfagd", "cdg", "ceaabeb", "cef", "cefa", "cf", "cfbaa", "cfegd", "cg", "d", "dbdaf", "dbf", "dbg", "dc", "dcadbfd", "dcceaeb", "ddbf", "dddcf", "defeff", "dfagcdc", "dfc", "dfd", "dgda", "dgdc", "dgea", "dgefgg", "e", "eaagb", "ead", "eae", "eb", "ebaeefa", "ebd", "ebg", "eca", "ecadf", "eccf", "ecffd", "ecg", "eebfaba", "eeceb", "efe", "efgbabf", "efgdadf", "egegcgc", "f", "fabe", "fad", "faga", "fbcae", "fbcege", "fbeeebe", "fc", "fcab", "fcgffc", "fddd", "fe", "feaf", "feefeda", "ffb", "fffag", "ffffc", "fgaacca", "fgcbb", "fgcec", "g", "gb", "gbcgc", "gbfcbbb", "gccabee", "gcccg", "gdbaeea", "ge", "gecebfc", "gefe", "gfaeabg", "gfbbgbf", "gfgbde", "ggd", "ggdeae"},
		},
		{
			[]string{
				"a",
				"aa",
				"aaad",
				"aabb",
				"aafdcc",
				"accgbbf",
				"adaa",
				"addec",
				"aeadacg",
				"aefgga",
				"afa",
				"ag",
				"baaffba",
				"baegde",
				"bba",
				"bba",
				"bbf",
				"bcdd",
				"bdbbe",
				"bdg",
				"beaabfd",
				"beggfga",
				"bfdg",
				"bfecbda",
				"bfgade",
				"bg",
				"bgff",
				"bgg",
				"c",
				"ca",
				"ca",
				"caa",
				"cad",
				"cb",
				"cbeff",
				"cc",
				"ccfe",
				"cd",
				"cdbda",
				"cdgge",
				"ce",
				"ceeccgc",
				"cfbd",
				"cgbdc",
				"d",
				"dadeee",
				"db",
				"dbabcec",
				"dbe",
				"ddg",
				"de",
				"decgbec",
				"deddgeb",
				"dfbfb",
				"dfcag",
				"dfeb",
				"ea",
				"eabbdgd",
				"eadc",
				"ebbfb",
				"ecebdc",
				"edbbbcb",
				"edbe",
				"ee",
				"eef",
				"eefaac",
				"eegg",
				"ef",
				"efb",
				"efdf",
				"efdgea",
				"efffca",
				"efg",
				"egbf",
				"fadaf",
				"fagddac",
				"fbb",
				"fcffdc",
				"febdfc",
				"feeafcb",
				"fff",
				"fffcbf",
				"fgbbefe",
				"g",
				"gac",
				"gbc",
				"gcbba",
				"ge",
				"gebgaf",
				"gec",
				"geecea",
				"geed",
				"geeg",
				"gfd",
			},
			"gd",
			[]string{"ge", "gebgaf", "gec", "geecea", "geed", "geeg", "gfd"},
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
				//t.Fatal("failed adding keys")
			}
			// Get and seek iterator
			root := r.root
			iter := root.LowerBoundIterator()
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
