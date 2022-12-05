package main

import (
	"fmt"
	"log"
	"strings"
	"unicode"

	"github.com/KumKeeHyun/gstream"
	"github.com/KumKeeHyun/gstream/materialized"
)

func main() {
	builder := gstream.NewBuilder()

	input := make(chan string)
	words := gstream.Stream[string](builder).From(input).
		Map(func(s string) string {
			return strings.TrimFunc(s, func(r rune) bool {
				return !unicode.IsLetter(r)
			})
		}).
		Map(func(s string) string {
			return strings.ToLower(s)
		}).
		FlatMap(func(s string) []string {
			return strings.Split(s, " ")
		})
	kvWords := gstream.SelectKey(words, func(w string) string {
		return w
	})

	mater, err := materialized.New(
		materialized.WithInMemory[string, int](),
	)
	if err != nil {
		log.Fatal(err)
	}
	gstream.Count[string](kvWords, mater).
		ToStream().
		Foreach(func(w string, i int) {
			fmt.Printf("word: %s, cnt: %d\n", w, i)
		})

	close := builder.BuildAndStart()

	input <- "The proposal notes several limitations of the current implementation. When I was first reviewing the proposal a year ago, one limitation stood out the most: no parameterized methods."
	input <- "As someone who has been maintaining various database clients for Go, the limitation initially sounded as a major compromise."
	input <- "I spent a week redesigning our clients based on the new proposal but felt unsatisfied. If you are coming from another language to Go, you might be familiar with the following API pattern:"
	close()
}
