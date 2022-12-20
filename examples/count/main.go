package main

import (
	"context"
	"fmt"
	"github.com/KumKeeHyun/gstream/state"
	"strings"
	"unicode"

	"github.com/KumKeeHyun/gstream"
)

func main() {
	builder := gstream.NewBuilder()

	input := make(chan string)
	words := gstream.Stream[string](builder).From(input).
		Map(func(_ context.Context, s string) string {
			return strings.TrimFunc(s, func(r rune) bool {
				return !unicode.IsLetter(r)
			})
		}).
		Map(func(_ context.Context, s string) string {
			return strings.ToLower(s)
		}).
		FlatMap(func(_ context.Context, s string) []string {
			return strings.Split(s, " ")
		})
	kvWords := gstream.SelectKey(words, func(w string) string {
		return w
	})

	sopt := state.NewOptions(
		state.WithInMemory[string, int](),
	)
	gstream.Count[string](kvWords, sopt).
		ToStream().
		Foreach(func(_ context.Context, kv gstream.KeyValue[string, int]) {
			fmt.Printf("word: %s, cnt: %d\n", kv.Key, kv.Value)
		})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		builder.BuildAndStart(ctx)
		done <- struct{}{}
	}()

	input <- "The proposal notes several limitations of the current implementation. When I was first reviewing the proposal a year ago, one limitation stood out the most: no parameterized methods."
	input <- "As someone who has been maintaining various database clients for Go, the limitation initially sounded as a major compromise."
	input <- "I spent a week redesigning our clients based on the new proposal but felt unsatisfied. If you are coming from another language to Go, you might be familiar with the following API pattern:"

	cancel()
	<-done
}
