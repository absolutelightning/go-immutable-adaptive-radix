// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package adaptive

import (
	"bufio"
	"fmt"
	"github.com/hashicorp/go-uuid"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"slices"
	"sort"
	"testing"
	"time"
)

func TestRadix_HugeTxn(t *testing.T) {
	r := NewRadixTree[int]()

	// Insert way more nodes than the cache can fit
	txn1 := r.Txn(true)
	var expect []string
	for i := 0; i < defaultModifiedCache*100; i++ {
		gen, err := uuid.GenerateUUID()
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		txn1.Insert([]byte(gen), i)
		expect = append(expect, gen)
	}
	r = txn1.Commit()
	sort.Strings(expect)

	// Collect the output, should be sorted
	var out []string
	fn := func(k []byte, v int) bool {
		out = append(out, string(k))
		return false
	}
	r.Walk(fn)

	// Verify the match
	if len(out) != len(expect) {
		t.Fatalf("length mis-match: %d vs %d", len(out), len(expect))
	}
	for i := 0; i < len(out); i++ {
		if out[i] != expect[i] {
			t.Fatalf("mis-match: %v %v", out[i], expect[i])
		}
	}
}

func TestInsert_UpdateFeedback(t *testing.T) {
	r := NewRadixTree[any]()
	txn1 := r.Txn(true)

	for i := 0; i < 10; i++ {
		var old interface{}
		var didUpdate bool
		old, didUpdate = txn1.Insert([]byte("helloworld"), i)
		if i == 0 {
			if old != nil || didUpdate {
				t.Fatalf("bad: %d %v %v", i, old, didUpdate)
			}
		} else {
			if old == nil || old.(int) != i-1 || !didUpdate {
				t.Fatalf("bad: %d %v %v", i, old, didUpdate)
			}
		}
	}
}

func TestARTree_InsertAndSearchWords(t *testing.T) {
	t.Parallel()

	art := NewRadixTree[int]()

	file, err := os.Open("test-text/words.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var lines []string

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	lineNumber := 1
	for scanner.Scan() {
		line := scanner.Text()
		art, _, _ = art.Insert([]byte(line), lineNumber)
		lineNumber += 1
		lines = append(lines, scanner.Text())
	}

	// optionally, resize scanner's capacity for lines over 64K, see next example
	lineNumber = 1
	for _, line := range lines {
		lineNumberFetched, f := art.Get([]byte(line))
		require.True(t, f)
		require.Equal(t, lineNumberFetched, lineNumber)
		lineNumber += 1
	}

	artLeafMin := art.Minimum()
	artLeafMax := art.Maximum()
	require.Equal(t, artLeafMin.key, getTreeKey([]byte("A")))
	require.Equal(t, artLeafMax.key, getTreeKey([]byte("zythum")))
}

func TestARTree_InsertVeryLongKey(t *testing.T) {
	t.Parallel()

	key1 := []byte{16, 0, 0, 0, 7, 10, 0, 0, 0, 2, 17, 10, 0, 0, 0, 120, 10, 0, 0, 0, 120, 10, 0,
		0, 0, 216, 10, 0, 0, 0, 202, 10, 0, 0, 0, 194, 10, 0, 0, 0, 224, 10, 0, 0, 0,
		230, 10, 0, 0, 0, 210, 10, 0, 0, 0, 206, 10, 0, 0, 0, 208, 10, 0, 0, 0, 232,
		10, 0, 0, 0, 124, 10, 0, 0, 0, 124, 2, 16, 0, 0, 0, 2, 12, 185, 89, 44, 213,
		251, 173, 202, 211, 95, 185, 89, 110, 118, 251, 173, 202, 199, 101, 0,
		8, 18, 182, 92, 236, 147, 171, 101, 150, 195, 112, 185, 218, 108, 246,
		139, 164, 234, 195, 58, 177, 0, 8, 16, 0, 0, 0, 2, 12, 185, 89, 44, 213,
		251, 173, 202, 211, 95, 185, 89, 110, 118, 251, 173, 202, 199, 101, 0,
		8, 18, 180, 93, 46, 151, 9, 212, 190, 95, 102, 178, 217, 44, 178, 235,
		29, 190, 218, 8, 16, 0, 0, 0, 2, 12, 185, 89, 44, 213, 251, 173, 202,
		211, 95, 185, 89, 110, 118, 251, 173, 202, 199, 101, 0, 8, 18, 180, 93,
		46, 151, 9, 212, 190, 95, 102, 183, 219, 229, 214, 59, 125, 182, 71,
		108, 180, 220, 238, 150, 91, 117, 150, 201, 84, 183, 128, 8, 16, 0, 0,
		0, 2, 12, 185, 89, 44, 213, 251, 173, 202, 211, 95, 185, 89, 110, 118,
		251, 173, 202, 199, 101, 0, 8, 18, 180, 93, 46, 151, 9, 212, 190, 95,
		108, 176, 217, 47, 50, 219, 61, 134, 207, 97, 151, 88, 237, 246, 208,
		8, 18, 255, 255, 255, 219, 191, 198, 134, 5, 223, 212, 72, 44, 208,
		250, 180, 14, 1, 0, 0, 8}
	key2 := []byte{16, 0, 0, 0, 7, 10, 0, 0, 0, 2, 17, 10, 0, 0, 0, 120, 10, 0, 0, 0, 120, 10, 0,
		0, 0, 216, 10, 0, 0, 0, 202, 10, 0, 0, 0, 194, 10, 0, 0, 0, 224, 10, 0, 0, 0,
		230, 10, 0, 0, 0, 210, 10, 0, 0, 0, 206, 10, 0, 0, 0, 208, 10, 0, 0, 0, 232,
		10, 0, 0, 0, 124, 10, 0, 0, 0, 124, 2, 16, 0, 0, 0, 2, 12, 185, 89, 44, 213,
		251, 173, 202, 211, 95, 185, 89, 110, 118, 251, 173, 202, 199, 101, 0,
		8, 18, 182, 92, 236, 147, 171, 101, 150, 195, 112, 185, 218, 108, 246,
		139, 164, 234, 195, 58, 177, 0, 8, 16, 0, 0, 0, 2, 12, 185, 89, 44, 213,
		251, 173, 202, 211, 95, 185, 89, 110, 118, 251, 173, 202, 199, 101, 0,
		8, 18, 180, 93, 46, 151, 9, 212, 190, 95, 102, 178, 217, 44, 178, 235,
		29, 190, 218, 8, 16, 0, 0, 0, 2, 12, 185, 89, 44, 213, 251, 173, 202,
		211, 95, 185, 89, 110, 118, 251, 173, 202, 199, 101, 0, 8, 18, 180, 93,
		46, 151, 9, 212, 190, 95, 102, 183, 219, 229, 214, 59, 125, 182, 71,
		108, 180, 220, 238, 150, 91, 117, 150, 201, 84, 183, 128, 8, 16, 0, 0,
		0, 3, 12, 185, 89, 44, 213, 251, 133, 178, 195, 105, 183, 87, 237, 150,
		155, 165, 150, 229, 97, 182, 0, 8, 18, 161, 91, 239, 50, 10, 61, 150,
		223, 114, 179, 217, 64, 8, 12, 186, 219, 172, 150, 91, 53, 166, 221,
		101, 178, 0, 8, 18, 255, 255, 255, 219, 191, 198, 134, 5, 208, 212, 72,
		44, 208, 250, 180, 14, 1, 0, 0, 8}

	art := NewRadixTree[string]()
	art, val1, _ := art.Insert(key1, string(key1))
	art, val2, _ := art.Insert(key2, string(key2))
	require.Equal(t, val1, "")
	require.Equal(t, val2, "")

	art.Insert(key2, string(key2))
	require.Equal(t, art.size, uint64(2))
}

func TestARTree_InsertAndSearchAndDeleteWords(t *testing.T) {
	t.Parallel()

	art := NewRadixTree[int]()

	file, err := os.Open("test-text/words.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var lines []string

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	lineNumber := 1
	for scanner.Scan() {
		line := scanner.Text()
		art, _, _ = art.Insert([]byte(line), lineNumber)
		lineNumber += 1
		lines = append(lines, scanner.Text())
	}

	// optionally, resize scanner's capacity for lines over 64K, see next example
	var val int
	lineNumber = 1
	for _, line := range lines {
		lineNumberFetched, f := art.Get([]byte(line))
		require.True(t, f)
		art, val, _ = art.Delete([]byte(line))
		require.Equal(t, val, lineNumber)
		require.Equal(t, lineNumberFetched, lineNumber)
		lineNumber += 1
		require.Equal(t, art.size, uint64(len(lines)-lineNumber+1))
	}

}

func TestDebug(t *testing.T) {
	r := NewRadixTree[any]()

	keys := []string{
		"sneakiness",
		"sneaking",
		"sneakingly",
		"sneakingness",
		"sneakish",
		"sneakishly",
		"sneakishness",
		"Zwinglianist", "zwitter", "zwitterion", "zwitterionic", "zyga",
		"zygadenine", "Zygadenus", "Zygaena", "zygaenid", "Zygaenidae", "zygal", "zygantra", "zygantrum", "zygapophyseal",
		"zygapophysis", "zygion", "zygite", "Zygnema", "Zygnemaceae",
		"A",
		"a",
		"aa",
		"aal",
		"aalii",
		"aam",
		"Aani",
		"aardvark",
		"aardwolf",
		"Aaron",
		"Aaronic",
		"Aaronical",
		"Aaronite",
		"Aaronitic",
		"Ab",
		"aba",
		"Ababdeh",
		"Ababua",
		"abac",
		"abaca",
		"abacate",
		"abacay",
		"abacinate",
		"abacination",
		"abaciscus",
		"abacist",
		"aback",
		"abactinal",
		"abactinally",
		"abaction",
		"abactor",
		"abaculus",
		"abacus",
		"Abadite",
		"abaff",
		"abaft",
		"abaisance",
		"abaiser",
		"abaissed",
		"abalienate",
		"abalienation",
		"abalone",
		"Abama",
		"abampere",
		"abandon",
		"abiston",
		"Abitibi",
		"abiuret",
		"abject",
		"abjectedness",
		"abjection",
		"abjective",
		"abjectly",
		"abjectness",
		"abjoint",
		"abjudge",
		"abjudicate",
		"abjudication",
		"abjunction",
		"abjunctive",
		"abjuration",
		"abjuratory",
		"abjure",
		"abjurement",
		"abjurer",
		"abkar",
		"abkari",
		"Abkhas",
		"Abkhasian",
		"ablach",
		"ablactate",
		"ablactation",
		"ablare",
		"ablastemic",
		"ablastous",
		"ablate",
		"ablation",
		"ablatitious",
		"ablatival",
		"ablative",
		"ablator",
		"ablaut",
		"ablaze",
		"able",
		"ableeze",
		"ablegate",
		"ableness",
		"ablepharia",
		"ablepharon",
		"ablepharous",
		"Ablepharus",
		"ablepsia",
		"ableptical",
		"ableptically",
		"abler",
		"ablest",
		"ablewhackets",
		"ablins",
		"abloom",
		"ablow",
		"ablude",
		"abluent",
		"ablush",
		"ablution",
		"ablutionary",
		"abluvion",
		"ably",
		"abmho",
		"Abnaki",
		"abnegate",
		"abnegation",
		"abnegative",
		"abnegator",
		"Abner",
		"abnerval",
		"abnet",
		"abneural",
		"abnormal",
		"abnormalism",
		"abnormalist",
		"abnormality",
		"abnormalize",
		"abnormally",
		"abnormalness",
		"abnormity",
		"abnormous",
		"abnumerable",
		"Abo",
		"aboard",
		"Abobra",
		"abode",
		"abodement",
		"abody",
		"Miastor",
		"miaul",
		"miauler",
		"mib",
		"mica",
		"micaceous",
		"micacious",
		"micacite",
		"Micah",
		"micasization",
		"micasize",
		"micate",
		"mication",
		"Micawberish",
		"Micawberism",
		"mice",
		"micellar",
		"micelle",
		"Michabo",
		"Michabou",
		"Michael",
		"Michaelites",
		"Michaelmas",
		"Michaelmastide",
		"miche",
		"Micheal",
		"Michel",
		"Michelangelesque",
		"Michelangelism",
		"Michelia",
		"Michelle",
		"micher",
		"Michiel",
		"Michigamea",
		"Michigan",
		"michigan",
		"Michigander",
		"Michiganite",
		"miching",
		"Michoacan",
		"Michoacano",
		"micht",
		"Mick",
		"mick",
		"Mickey",
		"mickle",
		"Micky",
		"Micmac",
		"mico",
		"miconcave",
		"Miconia",
		"micramock",
		"Micrampelis",
		"micranatomy",
		"micrander",
		"micrandrous",
		"micraner",
		"micranthropos",
		"Micraster",
		"micrencephalia",
		"micrencephalic",
		"micrencephalous",
		"micrencephalus",
		"micrencephaly",
		"micrergate",
		"micresthete",
		"micrify",
		"micro",
		"microammeter",
		"microampere",
		"microanalysis",
		"microanalyst",
		"microanalytical",
		"microangstrom",
		"microapparatus",
		"microbal",
		"microbalance",
		"microbar",
		"microbarograph",
		"microbattery",
		"microbe",
		"microbeless",
		"microbeproof",
		"microbial",
		"microbian",
		"microbic",
		"microbicidal",
		"microbicide",
		"microbiologic",
		"microbiological",
		"microbiologically",
		"microbiologist",
		"microbiology",
		"microbion",
		"microbiosis",
		"microbiota",
		"microbiotic",
		"microbious",
		"microbism",
		"microbium",
		"microblast",
		"microblepharia",
		"microblepharism",
		"microblephary",
		"microbrachia",
		"microbrachius",
		"microburet",
		"microburette",
		"microburner",
		"microcaltrop",
		"microcardia",
		"microcardius",
		"microcarpous",
		"Microcebus",
		"microcellular",
		"microcentrosome",
		"microcentrum",
		"microcephal",
		"microcephalia",
		"microcephalic",
		"microcephalism",
		"microcephalous",
		"microcephalus",
		"microcephaly",
		"microceratous",
		"microchaeta",
		"microcharacter",
		"microcheilia",
		"microcheiria",
		"microchemic",
		"microchemical",
		"microchemically",
		"zoogony",
		"zoograft",
		"zoografting",
		"zoographer",
		"zoographic",
		"zoographical",
		"zoographically",
		"zoographist",
		"zoography",
		"zooid",
		"zooidal",
		"zooidiophilous",
		"zooks",
		"zoolater",
		"zoolatria",
		"zoolatrous",
		"zoolatry",
		"zoolite",
		"zoolith",
		"zoolithic",
		"zoolitic",
		"zoologer",
		"zoologic",
		"zoological",
		"zoologically",
		"zoologicoarchaeologist",
		"zoologicobotanical",
		"zoologist",
		"zoologize",
		"zoology",
		"zoom",
		"zoomagnetic",
		"zoomagnetism",
		"zoomancy",
		"zoomania",
		"zoomantic",
		"zoomantist",
		"Zoomastigina",
		"Zoomastigoda",
		"zoomechanical",
		"zoomechanics",
		"zoomelanin",
		"zoometric",
		"zoometry",
		"zoomimetic",
		"zoomimic",
		"zoomorph",
		"zoomorphic",
		"zoomorphism",
		"zoomorphize",
		"zoomorphy",
		"zoon",
		"zoonal",
		"zoonerythrin",
		"zoonic",
		"zoonist",
		"zoonite",
		"zoonitic",
		"zoonomia",
		"zoonomic",
		"zoonomical",
		"zoonomist",
		"zoonomy",
		"zoonosis",
		"zoonosologist",
		"zoonosology",
		"zoonotic",
		"zoons",
		"zoonule",
		"zoopaleontology",
		"zoopantheon",
		"zooparasite",
		"zooparasitic",
		"zoopathological",
		"zoopathologist",
		"zoopathology",
		"zoopathy",
		"zooperal",
		"zooperist",
		"zoopery",
		"Zoophaga",
		"zoophagan",
		"Zoophagineae",
		"zoophagous",
		"zoopharmacological",
		"zoopharmacy",
		"zoophile",
		"zoophilia",
		"zoophilic",
		"zoophilism",
		"zoophilist",
		"zoophilite",
		"zoophilitic",
		"zoophilous",
		"zoophily",
		"zoophobia",
		"zoophobous",
		"zoophoric",
		"zoophorus",
		"zoophysical",
		"zoophysics",
		"zoophysiology",
		"Zoophyta",
		"zoophytal",
		"zoophyte",
		"zoophytic",
		"zoophytical",
		"zoophytish",
		"zoophytography",
		"zoophytoid",
		"zoophytological",
		"zoophytologist",
		"zoophytology",
		"zooplankton",
		"zooplanktonic",
		"zooplastic",
		"zooplasty",
		"zoopraxiscope",
		"zoopsia",
		"zoopsychological",
		"zoopsychologist",
		"zoopsychology",
		"zooscopic",
		"zooscopy",
		"zoosis",
		"zoosmosis",
		"zoosperm",
		"zoospermatic",
		"zoospermia",
		"zoospermium",
		"zoosphere",
		"zoosporange",
		"zoosporangia",
		"zoosporangial",
		"zoosporangiophore",
		"zoosporangium",
		"zoospore",
		"zoosporic",
		"zoosporiferous",
		"zoosporocyst",
		"zoosporous",
		"zootaxy",
		"zootechnic",
		"zootechnics",
		"zootechny",
		"zooter",
		"zoothecia",
		"zoothecial",
		"zoothecium",
		"zootheism",
		"zootheist",
		"zootheistic",
		"zootherapy",
		"zoothome",
		"zootic",
		"Zootoca",
		"zootomic",
		"zootomical",
		"zootomically",
		"zootomist",
		"zootomy",
		"zoototemism",
		"zootoxin",
		"zootrophic",
		"zootrophy",
		"zootype",
		"zootypic",
		"zooxanthella",
		"zooxanthellae",
		"zooxanthin",
		"zoozoo",
		"zopilote",
		"Zoquean",
		"Zoraptera",
		"zorgite",
		"zorilla",
		"Zorillinae",
		"zorillo",
		"Zoroastrian",
		"Zoroastrianism",
		"Zoroastrism",
		"Zorotypus",
		"zorrillo",
		"Zosma",
		"zoster",
		"Zostera",
		"Zosteraceae",
		"zosteriform",
		"Zosteropinae",
		"Zosterops",
		"Zouave",
		"zounds",
		"Zoysia",
		"Zubeneschamali",
		"zuccarino",
		"zucchetto",
		"zucchini",
		"zugtierlast",
		"zugtierlaster",
		"zuisin",
		"Zuleika",
		"Zulhijjah",
		"Zulinde",
		"Zulkadah",
		"Zuludom",
		"Zuluize",
		"zumatic",
		"zumbooruk",
		"Zunian",
		"zunyite",
		"zupanate",
		"Zutugil",
		"zuurveldt",
		"zwanziger",
		"Zwieback",
		"zwieback",
		"Zwinglian",
		"Zwinglianism",
		"Zwinglianist",
		"zwitter",
		"zwitterion",
		"zwitterionic",
		"zyga",
		"zygadenine",
		"Zygadenus",
		"Zygaena",
		"zygaenid",
		"Zygaenidae",
		"zygal",
		"zygantra",
		"zygantrum",
		"zygapophyseal",
		"zygapophysis",
		"zygion",
		"zygite",
		"Zygnema",
		"Zygnemaceae",
		"Zygnemales",
		"Zygnemataceae",
		"zygnemataceous",
		"Zygnematales",
		"zygobranch",
		"Zygobranchia",
		"Zygobranchiata",
		"zygobranchiate",
		"Zygocactus",
		"zygodactyl",
		"Zygodactylae",
		"Zygodactyli",
		"zygodactylic",
		"zygodactylism",
		"zygodactylous",
		"zygodont",
		"zygolabialis",
		"zygoma",
		"zygomata",
		"zygomatic",
		"zygomaticoauricular",
		"zygomaticoauricularis",
		"zygomaticofacial",
		"zygomaticofrontal",
		"zygomaticomaxillary",
		"zygomaticoorbital",
		"zygomaticosphenoid",
		"zygomaticotemporal",
		"zygomaticum",
		"zygomaticus",
		"zygomaxillare",
		"zygomaxillary",
		"zygomorphic",
		"zygomorphism",
		"zygomorphous",
		"zygomycete",
		"Zygomycetes",
		"zygomycetous",
		"zygoneure",
		"zygophore",
		"zygophoric",
		"Zygophyceae",
		"zygophyceous",
		"Zygophyllaceae",
		"zygophyllaceous",
		"Zygophyllum",
		"zygophyte",
		"zygopleural",
		"Zygoptera",
		"Zygopteraceae",
		"zygopteran",
		"zygopterid",
		"Zygopterides",
		"Zygopteris",
		"zygopteron",
		"zygopterous",
		"Zygosaccharomyces",
		"zygose",
		"zygosis",
		"zygosperm",
		"zygosphenal",
		"zygosphene",
		"zygosphere",
		"zygosporange",
		"zygosporangium",
		"zygospore",
		"zygosporic",
		"zygosporophore",
		"zygostyle",
		"zygotactic",
		"zygotaxis",
		"zygote",
		"zygotene",
		"zygotic",
		"zygotoblast",
		"zygotoid",
		"zygotomere",
		"zygous",
		"zygozoospore",
		"zymase",
		"zymic",
		"zymin",
		"zymite",
		"zymogen",
		"zymogene",
		"zymogenesis",
		"zymogenic",
		"zymogenous",
		"zymoid",
		"zymologic",
		"zymological",
		"zymologist",
		"zymology",
		"zymolyis",
		"zymolysis",
		"zymolytic",
		"zymome",
		"zymometer",
		"zymomin",
		"zymophore",
		"zymophoric",
		"zymophosphate",
		"zymophyte",
		"zymoplastic",
		"zymoscope",
		"zymosimeter",
		"zymosis",
		"zymosterol",
		"zymosthenic",
		"zymotechnic",
		"zymotechnical",
		"zymotechnics",
		"zymotechny",
		"zymotic",
		"zymotically",
		"zymotize",
		"zymotoxic",
		"zymurgy",
		"Zyrenian",
		"Zyrian",
		"Zyryan",
		"zythem",
		"Zythia",
		"zythum",
		"Zyzomys",
		"Zyzzogeton",
	}
	for _, k := range keys {
		r, _, _ = r.Insert([]byte(k), nil)
	}

}

func TestLongestPrefix(t *testing.T) {
	r := NewRadixTree[any]()

	keys := []string{
		"",
		"foo",
		"foobar",
		"foobarbaz",
		"foobarbazzip",
		"foozip",
	}
	for _, k := range keys {
		r, _, _ = r.Insert([]byte(k), nil)
	}
	if int(r.size) != len(keys) {
		t.Fatalf("bad len: %v %v", r.size, len(keys))
	}

	type exp struct {
		inp string
		out string
	}
	cases := []exp{
		{"a", ""},
		{"abc", ""},
		{"fo", ""},
		{"foo", "foo"},
		{"foob", "foo"},
		{"foobar", "foobar"},
		{"foobarba", "foobar"},
		{"foobarbaz", "foobarbaz"},
		{"foobarbazzi", "foobarbaz"},
		{"foobarbazzip", "foobarbazzip"},
		{"foozi", "foo"},
		{"foozip", "foozip"},
		{"foozipzap", "foozip"},
	}
	for _, test := range cases {
		m, _, ok := r.LongestPrefix([]byte(test.inp))
		if !ok {
			t.Fatalf("no match: %v", test)
		}
		if string(m) != test.out {
			t.Fatalf("mis-match: %v %v", string(m), test)
		}
	}
}

func TestDeletePrefix(t *testing.T) {

	type exp struct {
		desc        string
		treeNodes   []string
		prefix      string
		expectedOut []string
	}

	//various test cases where DeletePrefix should succeed
	cases := []exp{
		{
			"prefix not a node in tree",
			[]string{
				"",
				"test/test1",
				"test/test2",
				"test/test3",
				"R",
				"RA"},
			"test",
			[]string{
				"",
				"R",
				"RA",
			},
		},
		{
			"prefix matches a node in tree",
			[]string{
				"",
				"test",
				"test/test1",
				"test/test2",
				"test/test3",
				"test/testAAA",
				"R",
				"RA",
			},
			"test",
			[]string{
				"",
				"R",
				"RA",
			},
		},
		{
			"longer prefix, but prefix is not a node in tree",
			[]string{
				"",
				"test/test1",
				"test/test2",
				"test/test3",
				"test/testAAA",
				"R",
				"RA",
			},
			"test/test",
			[]string{
				"",
				"R",
				"RA",
			},
		},
		{
			"prefix only matches one node",
			[]string{
				"",
				"AB",
				"ABC",
				"AR",
				"R",
				"RA",
			},
			"AR",
			[]string{
				"",
				"AB",
				"ABC",
				"R",
				"RA",
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.desc, func(t *testing.T) {
			r := NewRadixTree[bool]()
			for _, ss := range testCase.treeNodes {
				r, _, _ = r.Insert([]byte(ss), true)
			}
			if got, want := r.Len(), len(testCase.treeNodes); got != want {
				t.Fatalf("Unexpected tree length after insert, got %d want %d ", got, want)
			}
			r, ok := r.DeletePrefix([]byte(testCase.prefix))
			if !ok {
				t.Fatalf("DeletePrefix should have returned true for tree %v, deleting prefix %v", testCase.treeNodes, testCase.prefix)
			}
			if got, want := r.Len(), len(testCase.expectedOut); got != want {
				t.Fatalf("Bad tree length, got %d want %d tree %v, deleting prefix %v ", got, want, testCase.treeNodes, testCase.prefix)
			}

			//verifyTree(t, testCase.expectedOut, r)
			//Delete a non-existant node
			r, ok = r.DeletePrefix([]byte("CCCCC"))
			if ok {
				t.Fatalf("Expected DeletePrefix to return false ")
			}
		})
	}
}

func TestIteratePrefix(t *testing.T) {
	r := NewRadixTree[any]()

	keys := []string{
		"foo/bar/baz",
		"foo/baz/bar",
		"foo/zip/zap",
		"foobar",
		"zipzap",
	}
	for _, k := range keys {
		r, _, _ = r.Insert([]byte(k), nil)
	}
	if r.Len() != len(keys) {
		t.Fatalf("bad len: %v %v", r.Len(), len(keys))
	}

	type exp struct {
		inp string
		out []string
	}
	cases := []exp{
		{
			"",
			keys,
		},
		{
			"f",
			[]string{
				"foo/bar/baz",
				"foo/baz/bar",
				"foo/zip/zap",
				"foobar",
			},
		},
		{
			"foo",
			[]string{
				"foo/bar/baz",
				"foo/baz/bar",
				"foo/zip/zap",
				"foobar",
			},
		},
		{
			"foob",
			[]string{"foobar"},
		},
		{
			"foo/",
			[]string{"foo/bar/baz", "foo/baz/bar", "foo/zip/zap"},
		},
		{
			"foo/b",
			[]string{"foo/bar/baz", "foo/baz/bar"},
		},
		{
			"foo/ba",
			[]string{"foo/bar/baz", "foo/baz/bar"},
		},
		{
			"foo/bar",
			[]string{"foo/bar/baz"},
		},
		{
			"foo/bar/baz",
			[]string{"foo/bar/baz"},
		},
		{
			"foo/bar/bazoo",
			[]string{},
		},
		{
			"z",
			[]string{"zipzap"},
		},
	}

	for idx, test := range cases {
		iter := r.Root().Iterator()
		iter.SeekPrefix([]byte(test.inp))

		// Consume all the keys
		var out []string
		for {
			key, _, ok := iter.Next()
			if !ok {
				break
			}
			out = append(out, string(key))
		}
		if !slices.Equal(out, test.out) {
			t.Fatalf("mis-match: %d %v %v", idx, out, test.out)
		}
	}
}

func TestTrackMutate_DeletePrefix(t *testing.T) {

	r := NewRadixTree[any]()

	keys := []string{
		"foo",
		"foo/bar/baz",
		"foo/baz/bar",
		"foo/zip/zap",
		"bazbaz",
		"zipzap",
	}
	for _, k := range keys {
		r, _, _ = r.Insert([]byte(k), nil)
	}
	if r.Len() != len(keys) {
		t.Fatalf("bad len: %v %v", r.Len(), len(keys))
	}

	rootWatch, _, _ := r.GetWatch(nil)
	if rootWatch == nil {
		t.Fatalf("Should have returned a watch")
	}

	nodeWatch1, _, _ := r.GetWatch([]byte("foo/bar/baz"))
	if nodeWatch1 == nil {
		t.Fatalf("Should have returned a watch")
	}

	nodeWatch2, _, _ := r.GetWatch([]byte("foo/baz/bar"))
	if nodeWatch2 == nil {
		t.Fatalf("Should have returned a watch")
	}

	nodeWatch3, _, _ := r.GetWatch([]byte("foo/zip/zap"))
	if nodeWatch3 == nil {
		t.Fatalf("Should have returned a watch")
	}

	unknownNodeWatch, _, _ := r.GetWatch([]byte("bazbaz"))
	if unknownNodeWatch == nil {
		t.Fatalf("Should have returned a watch")
	}

	// Verify that deleting prefixes triggers the right set of watches
	txn := r.Txn(true)
	txn.TrackMutate(true)
	ok := txn.DeletePrefix([]byte("foo"))

	if !ok {
		t.Fatalf("Expected delete prefix to return true")
	}
	if hasAnyClosedMutateCh(r) {
		t.Fatalf("Transaction was not committed, no channel should have been closed")
	}

	txn.Commit()

	// Verify that all the leaf nodes we set up watches for above get triggered from the delete prefix call
	select {
	case <-rootWatch:
	default:
		t.Fatalf("root watch was not triggered")
	}
	select {
	case <-nodeWatch1:
	default:
		t.Fatalf("node watch was not triggered")
	}
	select {
	case <-nodeWatch2:
	default:
		t.Fatalf("node watch was not triggered")
	}
	select {
	case <-nodeWatch3:
	default:
		t.Fatalf("node watch was not triggered")
	}
	select {
	case <-unknownNodeWatch:
		t.Fatalf("Unrelated node watch was triggered during a prefix delete")
	default:
	}

}

// hasAnyClosedMutateCh scans the given tree and returns true if there are any
// closed mutate channels on any nodes or leaves.
func hasAnyClosedMutateCh[T any](r *RadixTree[T]) bool {
	iter := r.rawIterator()
	iter.Next()
	for ; iter.Front() != nil; iter.Next() {
		n := iter.Front()
		if isClosed(n.getMutateCh()) {
			return true
		}
		if n.isLeaf() && isClosed(n.getMutateCh()) {
			return true
		}
	}
	return false
}

func TestInsertNewStructure(t *testing.T) {

	r := NewRadixTree[any]()

	keys := []string{"aagcfgc", "acbcdca", "aceeaca", "ad", "aefab", "afdcec", "b",
		"badcf", "bbag", "bccdegd", "cafg", "cbb", "ccaagef", "daae",
		"dabdbb", "dbbgb", "dbcaca", "fbeaab", "ffeec", "fg", "ga", "gbc", "gdcg", "gec", "gecga", "gfa", "gfad"}

	for itr := 0; itr < 10; itr++ {
		for _, k := range keys {
			r, _, _ = r.Insert([]byte(k), nil)
		}
	}

}

func TestTrackMutate_SeekPrefixWatch(t *testing.T) {
	for i := 0; i < 3; i++ {
		r := NewRadixTree[any]()

		keys := []string{
			"foo/bar/baz",
			"foo/baz/bar",
			"foo/zip/zap",
			"foobar",
			"zipzap",
		}
		for _, k := range keys {
			r, _, _ = r.Insert([]byte(k), nil)
		}
		if r.Len() != len(keys) {
			t.Fatalf("bad len: %v %v", r.Len(), len(keys))
		}

		iter := r.Root().Iterator()
		rootWatch := iter.SeekPrefixWatch([]byte("nope"))

		iter = r.Root().Iterator()
		parentWatch := iter.SeekPrefixWatch([]byte("foo"))

		iter = r.Root().Iterator()
		leafWatch := iter.SeekPrefixWatch([]byte("foobar"))

		iter = r.Root().Iterator()
		missingWatch := iter.SeekPrefixWatch([]byte("foobarbaz"))

		iter = r.Root().Iterator()
		otherWatch := iter.SeekPrefixWatch([]byte("foo/b"))

		// Write to a sub-child should trigger the leaf!
		txn := r.Txn(true)
		txn.TrackMutate(true)
		txn.Insert([]byte("foobarbaz"), nil)
		switch i {
		case 0:
			r = txn.Commit()
		case 1:
			r = txn.CommitOnly()
			txn.Notify()
		default:
			r = txn.CommitOnly()
			txn.slowNotify()
		}
		if hasAnyClosedMutateCh(r) {
			t.Fatalf("bad")
		}

		// Verify root and parent triggered, and leaf affected
		select {
		case <-rootWatch:
		default:
			t.Fatalf("bad")
		}
		select {
		case <-parentWatch:
		default:
			t.Fatalf("bad")
		}
		select {
		case <-leafWatch:
		default:
			t.Fatalf("bad")
		}
		select {
		case <-missingWatch:
		default:
			t.Fatalf("bad")
		}
		select {
		case <-otherWatch:
			t.Fatalf("bad")
		default:
		}

		iter = r.Root().Iterator()
		rootWatch = iter.SeekPrefixWatch([]byte("nope"))

		iter = r.Root().Iterator()
		parentWatch = iter.SeekPrefixWatch([]byte("foo"))

		iter = r.Root().Iterator()
		leafWatch = iter.SeekPrefixWatch([]byte("foobar"))

		iter = r.Root().Iterator()
		missingWatch = iter.SeekPrefixWatch([]byte("foobarbaz"))

		// Delete to a sub-child should trigger the leaf!
		txn = r.Txn(true)
		txn.TrackMutate(true)
		txn.Delete([]byte("foobarbaz"))
		switch i {
		case 0:
			r = txn.Commit()
		case 1:
			r = txn.CommitOnly()
			txn.Notify()
		default:
			r = txn.CommitOnly()
			txn.slowNotify()
		}
		if hasAnyClosedMutateCh(r) {
			t.Fatalf("bad")
		}

		// Verify root and parent triggered, and leaf affected
		select {
		case <-rootWatch:
		default:
			t.Fatalf("bad")
		}
		select {
		case <-parentWatch:
		default:
			t.Fatalf("bad")
		}
		fmt.Println("leafwatchhere", leafWatch)
		select {
		case <-leafWatch:
		default:
			t.Fatalf("bad")
		}
		select {
		case <-missingWatch:
		default:
			t.Fatalf("bad")
		}
		select {
		case <-otherWatch:
			t.Fatalf("bad")
		default:
		}
	}
}

func TestTrackMutate_GetWatch(t *testing.T) {
	for i := 0; i < 3; i++ {
		r := NewRadixTree[any]()

		keys := []string{
			"foo/bar/baz",
			"foo/baz/bar",
			"foo/zip/zap",
			"foobar",
			"zipzap",
		}
		for _, k := range keys {
			r, _, _ = r.Insert([]byte(k), nil)
		}
		if r.Len() != len(keys) {
			t.Fatalf("bad len: %v %v", r.Len(), len(keys))
		}

		rootWatch, _, ok := r.GetWatch(nil)
		if rootWatch == nil {
			t.Fatalf("bad")
		}

		parentWatch, _, ok := r.GetWatch([]byte("foo"))
		if parentWatch == nil {
			t.Fatalf("bad")
		}

		leafWatch, _, ok := r.GetWatch([]byte("foobar"))

		if !ok {
			t.Fatalf("should be found")
		}
		if leafWatch == nil {
			t.Fatalf("bad")
		}

		otherWatch, _, ok := r.GetWatch([]byte("foo/b"))
		if otherWatch == nil {
			t.Fatalf("bad")
		}
		// Write to a sub-child should not trigger the leaf!
		txn := r.Txn(true)
		txn.TrackMutate(true)
		txn.Insert([]byte("foobarbaz"), nil)
		switch i {
		case 0:
			r = txn.Commit()
		case 1:
			r = txn.CommitOnly()
			txn.Notify()
		default:
			r = txn.CommitOnly()
			txn.slowNotify()
		}
		if hasAnyClosedMutateCh(r) {
			t.Fatalf("bad")
		}

		// Verify root and parent triggered, not leaf affected
		select {
		case <-rootWatch:
		default:
			t.Fatalf("bad")
		}
		select {
		case <-parentWatch:
		default:
			t.Fatalf("bad")
		}
		select {
		case <-leafWatch:
			t.Fatalf("bad")
		default:
		}
		select {
		case <-otherWatch:
			t.Fatalf("bad")
		default:
		}

		// Setup new watchers
		rootWatch, _, ok = r.GetWatch(nil)
		if rootWatch == nil {
			t.Fatalf("bad")
		}

		parentWatch, _, ok = r.GetWatch([]byte("foo"))
		if parentWatch == nil {
			t.Fatalf("bad")
		}

		// Write to a exactly leaf should trigger the leaf!
		txn = r.Txn(true)
		txn.TrackMutate(true)
		txn.Insert([]byte("foobar"), nil)
		switch i {
		case 0:
			r = txn.Commit()
		case 1:
			r = txn.CommitOnly()
			txn.Notify()
		default:
			r = txn.CommitOnly()
			txn.slowNotify()
		}
		if hasAnyClosedMutateCh(r) {
			t.Fatalf("bad")
		}

		select {
		case <-rootWatch:
		default:
			t.Fatalf("bad")
		}
		select {
		case <-parentWatch:
		default:
			t.Fatalf("bad")
		}
		select {
		case <-leafWatch:
		default:
			t.Fatalf("bad")
		}
		select {
		case <-otherWatch:
			t.Fatalf("bad")
		default:
		}

		// Setup all the watchers again
		rootWatch, _, ok = r.GetWatch(nil)
		if rootWatch == nil {
			t.Fatalf("bad")
		}

		parentWatch, _, ok = r.GetWatch([]byte("foo"))
		if parentWatch == nil {
			t.Fatalf("bad")
		}

		leafWatch, _, ok = r.GetWatch([]byte("foobar"))
		if !ok {
			t.Fatalf("should be found")
		}
		if leafWatch == nil {
			t.Fatalf("bad")
		}

		// Delete to a sub-child should not trigger the leaf!
		txn = r.Txn(true)
		txn.TrackMutate(true)
		txn.Delete([]byte("foobarbaz"))
		switch i {
		case 0:
			r = txn.Commit()
		case 1:
			r = txn.CommitOnly()
			txn.Notify()
		default:
			r = txn.CommitOnly()
			txn.slowNotify()
		}
		if hasAnyClosedMutateCh(r) {
			t.Fatalf("bad")
		}

		// Verify root and parent triggered, not leaf affected
		select {
		case <-rootWatch:
		default:
			t.Fatalf("bad")
		}
		select {
		case <-parentWatch:
		default:
			t.Fatalf("bad")
		}
		select {
		case <-leafWatch:
			t.Fatalf("bad")
		default:
		}
		select {
		case <-otherWatch:
			t.Fatalf("bad")
		default:
		}

		// Setup new watchers
		rootWatch, _, ok = r.GetWatch(nil)
		if rootWatch == nil {
			t.Fatalf("bad")
		}

		parentWatch, _, ok = r.GetWatch([]byte("foo"))
		if parentWatch == nil {
			t.Fatalf("bad")
		}

		// Write to a exactly leaf should trigger the leaf!
		txn = r.Txn(true)
		txn.TrackMutate(true)
		txn.Delete([]byte("foobar"))
		switch i {
		case 0:
			r = txn.Commit()
		case 1:
			r = txn.CommitOnly()
			txn.Notify()
		default:
			r = txn.CommitOnly()
			txn.slowNotify()
		}
		if hasAnyClosedMutateCh(r) {
			t.Fatalf("bad")
		}

		select {
		case <-rootWatch:
		default:
			t.Fatalf("bad")
		}
		select {
		case <-parentWatch:
		default:
			t.Fatalf("bad")
		}
		select {
		case <-leafWatch:
		default:
			t.Fatalf("bad")
		}
		select {
		case <-otherWatch:
			t.Fatalf("bad")
		default:
		}
	}
}

func TestTrackMutate_HugeTxn(t *testing.T) {
	r := NewRadixTree[any]()

	keys := []string{
		"foo/bar/baz",
		"foo/baz/bar",
		"foo/zip/zap",
		"foobar",
		"nochange",
	}
	for i := 0; i < defaultModifiedCache; i++ {
		key := fmt.Sprintf("aaa%d", i)
		r, _, _ = r.Insert([]byte(key), nil)
	}
	for _, k := range keys {
		r, _, _ = r.Insert([]byte(k), nil)
	}
	for i := 0; i < defaultModifiedCache; i++ {
		key := fmt.Sprintf("zzz%d", i)
		r, _, _ = r.Insert([]byte(key), nil)
	}
	if r.Len() != len(keys)+2*defaultModifiedCache {
		t.Fatalf("bad len: %v %v", r.Len(), len(keys))
	}

	rootWatch, _, ok := r.GetWatch(nil)
	if rootWatch == nil {
		t.Fatalf("bad")
	}

	parentWatch, _, ok := r.GetWatch([]byte("foo"))
	if parentWatch == nil {
		t.Fatalf("bad")
	}

	leafWatch, _, ok := r.GetWatch([]byte("foobar"))
	if !ok {
		t.Fatalf("should be found")
	}
	if leafWatch == nil {
		t.Fatalf("bad")
	}

	nopeWatch, _, ok := r.GetWatch([]byte("nochange"))
	if !ok {
		t.Fatalf("should be found")
	}
	if nopeWatch == nil {
		t.Fatalf("bad")
	}

	beforeWatch, _, ok := r.GetWatch([]byte("aaa123"))
	if beforeWatch == nil {
		t.Fatalf("bad")
	}

	afterWatch, _, ok := r.GetWatch([]byte("zzz123"))
	if afterWatch == nil {
		t.Fatalf("bad")
	}

	// Start the transaction.
	txn := r.Txn(true)
	txn.TrackMutate(true)

	// Add new nodes on both sides of the tree and delete enough nodes to
	// overflow the tracking.
	txn.Insert([]byte("aaa"), nil)
	for i := 0; i < defaultModifiedCache; i++ {
		key := fmt.Sprintf("aaa%d", i)
		txn.Delete([]byte(key))
	}
	for i := 0; i < defaultModifiedCache; i++ {
		key := fmt.Sprintf("zzz%d", i)
		txn.Delete([]byte(key))
	}
	txn.Insert([]byte("zzz"), nil)

	// Hit the leaf, and add a child so we make multiple mutations to the
	// same node.
	txn.Insert([]byte("foobar"), nil)
	txn.Insert([]byte("foobarbaz"), nil)

	// Commit and make sure we overflowed but didn't take on extra stuff.
	r = txn.CommitOnly()
	if !txn.trackOverflow || txn.trackChnSlice != nil {
		t.Fatalf("bad")
	}

	// Now do the trigger.
	txn.Notify()

	// Make sure no closed channels escaped the transaction.
	if hasAnyClosedMutateCh(r) {
		t.Fatalf("bad")
	}

	// Verify the watches fired as expected.
	select {
	case <-rootWatch:
	default:
		t.Fatalf("bad")
	}
	select {
	case <-parentWatch:
	default:
		t.Fatalf("bad")
	}
	select {
	case <-leafWatch:
	default:
		t.Fatalf("bad")
	}
	select {
	case <-nopeWatch:
		t.Fatalf("bad")
	default:
	}
	select {
	case <-beforeWatch:
	default:
		t.Fatalf("bad")
	}
	select {
	case <-afterWatch:
	default:
		t.Fatalf("bad")
	}
}

func TestLenTxn(t *testing.T) {
	r := NewRadixTree[any]()

	if r.Len() != 0 {
		t.Fatalf("not starting with empty tree")
	}

	txn := r.Txn(true)
	keys := []string{
		"foo/bar/baz",
		"foo/baz/bar",
		"foo/zip/zap",
		"foobar",
		"nochange",
	}
	for _, k := range keys {
		txn.Insert([]byte(k), nil)
	}
	r = txn.Commit()

	if r.Len() != len(keys) {
		t.Fatalf("bad: expected %d, got %d", len(keys), r.Len())
	}

	txn = r.Txn(true)
	for _, k := range keys {
		txn.Delete([]byte(k))
	}
	r = txn.Commit()

	if r.Len() != 0 {
		t.Fatalf("tree len should be zero, got %d", r.Len())
	}
}

const datasetSize = 100000

func generateDataset(size int) []string {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	dataset := make([]string, size)
	for i := 0; i < size; i++ {
		uuid1, _ := uuid.GenerateUUID()
		dataset[i] = uuid1
	}
	return dataset
}

func BenchmarkMixedOperations(b *testing.B) {
	dataset := generateDataset(datasetSize)
	art := NewRadixTree[int]()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < datasetSize; j++ {
			key := dataset[j]

			// Randomly choose an operation
			switch rand.Intn(3) {
			case 0:
				art, _, _ = art.Insert([]byte(key), j)
			case 1:
				art.Get([]byte(key))
			case 2:
				art, _, _ = art.Delete([]byte(key))
			}
		}
	}
}

func loadTestFile(path string) [][]byte {
	file, err := os.Open(path)
	if err != nil {
		panic("Couldn't open " + path)
	}
	defer file.Close()

	var words [][]byte
	reader := bufio.NewReader(file)
	for {
		if line, err := reader.ReadBytes(byte('\n')); err != nil {
			break
		} else {
			if len(line) > 0 {
				words = append(words, line[:len(line)-1])
			}
		}
	}
	return words
}

func TestTreeInsertAndDeleteAllUUIDs(t *testing.T) {
	uuids := loadTestFile("test-text/uuid.txt")
	tree := NewRadixTree[any]()
	for _, w := range uuids {
		tree, _, _ = tree.Insert(w, w)
	}

	for _, w := range uuids {
		newT, v, deleted := tree.Delete(w)
		tree = newT
		require.True(t, deleted)
		require.Equal(t, w, v)
	}

	require.Equal(t, uint64(0), tree.size)
}

func BenchmarkGroupedOperations(b *testing.B) {
	dataset := generateDataset(datasetSize)
	art := NewRadixTree[int]()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Insert all keys
		for _, key := range dataset {
			art, _, _ = art.Insert([]byte(key), 0)
		}

		// Search all keys
		for _, key := range dataset {
			art.Get([]byte(key))
		}

		// Delete all keys
		for _, key := range dataset {
			art, _, _ = art.Delete([]byte(key))
		}
	}
}

func BenchmarkInsertART(b *testing.B) {
	r := NewRadixTree[int]()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		uuid1, _ := uuid.GenerateUUID()
		r, _, _ = r.Insert([]byte(uuid1), n)
	}
}

func BenchmarkSearchART(b *testing.B) {
	r := NewRadixTree[int]()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		uuid1, _ := uuid.GenerateUUID()
		r, _, _ = r.Insert([]byte(uuid1), n)
		r.Get([]byte(uuid1))
	}
}

func BenchmarkDeleteART(b *testing.B) {
	r := NewRadixTree[int]()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		uuid1, _ := uuid.GenerateUUID()
		r, _, _ = r.Insert([]byte(uuid1), n)
		r, _, _ = r.Delete([]byte(uuid1))
	}
}

func BenchmarkDeletePrefixART(b *testing.B) {
	r := NewRadixTree[int]()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		uuid1, _ := uuid.GenerateUUID()
		r, _, _ = r.Insert([]byte(uuid1), n)
		r, _ = r.DeletePrefix([]byte(""))
	}
}

func BenchmarkLongestPrefixART(b *testing.B) {
	r := NewRadixTree[int]()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		uuid1, _ := uuid.GenerateUUID()
		r, _, _ = r.Insert([]byte(uuid1), n)
		_, _, _ = r.LongestPrefix([]byte(""))
	}
}

func BenchmarkSeekPrefixWatchART(b *testing.B) {
	r := NewRadixTree[int]()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		uuid1, _ := uuid.GenerateUUID()
		r, _, _ = r.Insert([]byte(uuid1), n)
		iter := r.root.Iterator()
		iter.SeekPrefixWatch([]byte(""))
		count := 0
		for {
			_, _, f := iter.Next()
			if f {
				count++
			} else {
				break
			}
		}
		if r.Len() != count {
			//b.Fatalf("hello")
		}
	}
}

func BenchmarkSeekLowerBound(b *testing.B) {
	r := NewRadixTree[int]()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		uuid1, _ := uuid.GenerateUUID()
		r, _, _ = r.Insert([]byte(uuid1), n)
		iter := r.root.LowerBoundIterator()
		iter.SeekLowerBound([]byte(""))
		count := 0
		for {
			_, _, f := iter.Next()
			if f {
				count++
			} else {
				break
			}
		}
		if r.Len() != count {
			//b.Fatalf("hello")
		}
	}
}

func BenchmarkSeekReverseLowerBound(b *testing.B) {
	r := NewRadixTree[int]()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		uuid1, _ := uuid.GenerateUUID()
		r, _, _ = r.Insert([]byte(uuid1), n)
		iter := r.root.ReverseIterator()
		iter.SeekReverseLowerBound([]byte(""))
		count := 0
		for {
			_, _, f := iter.Previous()
			if f {
				count++
			} else {
				break
			}
		}
		if r.Len() != count {
			//b.Fatalf("hello")
		}
	}
}
