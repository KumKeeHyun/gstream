package main

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/KumKeeHyun/gstream"
)

func main() {
	input := make(chan int)

	builder := gstream.NewBuilder()
	source := gstream.Stream[int](builder).From(input)

	filteredBig := source.Filter(func(i int) bool {
		return i > 10
	})
	mappedBig := gstream.Map(filteredBig, func(_ context.Context, i int) string {
		return fmt.Sprintf("big-%d", i)
	})

	filteredSmall := source.Filter(func(i int) bool {
		return i <= 10
	})
	mappedSmall := gstream.Map(filteredSmall, func(_ context.Context, i int) string {
		return fmt.Sprintf("small-%d", i)
	})
	smallOutput := mappedSmall.To()
	mappedSmall.Merge(mappedBig).
		Foreach(func(_ context.Context, s string) {
			fmt.Println("merged:", s)
		})

	go func() {
		for s := range smallOutput {
			fmt.Println("small:", s)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		builder.BuildAndStart(ctx)
		done <- struct{}{}
	}()

	for i := 0; i < 20; i++ {
		input <- rand.Int() % 20
	}

	cancel()
	<-done
}
