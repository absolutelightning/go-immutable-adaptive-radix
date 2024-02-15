package go_immutable_adaptive_radix_tree

import (
	"bufio"
	"log"
	"os"
	"testing"
)

func TestArtTree_InsertAndSearch(t *testing.T) {

	art := NewArtTree()

	file, err := os.Open("words.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var lines []string

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	lineNumber := 1
	for scanner.Scan() {
		art.Insert(scanner.Bytes(), lineNumber)
		lineNumber += 1
		lines = append(lines, scanner.Text())
	}

	// optionally, resize scanner's capacity for lines over 64K, see next example
	lineNumber = 1
	for _, line := range lines {
		lineNumberFetched := art.Search([]byte(line))
		if lineNumberFetched != lineNumber {
			t.Fatal("lineNumberFetched != lineNumber")
		}
		lineNumber += 1
	}
}
