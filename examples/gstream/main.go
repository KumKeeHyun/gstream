package main

import (
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
	mappedBig := gstream.Mapped[int, string](filteredBig).Map(func(i int) string {
		return fmt.Sprintf("big-%d", i)
	})

	filteredSmall := source.Filter(func(i int) bool {
		return i <= 10
	})
	mappedSmall := gstream.Mapped[int, string](filteredSmall).Map(func(i int) string {
		return fmt.Sprintf("small-%d", i)
	})
	smallOutput := mappedSmall.To()
	mappedSmall.Merge(mappedBig).
		Foreach(func(s string) {
			fmt.Println("merged:", s)
		})
		
	go func() {
		for s := range smallOutput {
			fmt.Println("small:", s)
		}
	}()

	close := builder.BuildAndStart()
	for i := 0; i < 20; i++ {
		input <- rand.Int() % 20
	}
	close()
}