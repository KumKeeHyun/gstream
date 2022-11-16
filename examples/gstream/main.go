package main

import (
	"fmt"

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

	go func() {
		for s := range smallOutput {
			fmt.Println("output: ", s)
		}
	}()

	mappedSmall.Merge(mappedBig).Foreach(func(s string) {
		fmt.Println(s)
	})

	close := builder.BuildAndStart()
	for i := 5; i < 15; i++ {
		input <- i
	}
	close()
}
