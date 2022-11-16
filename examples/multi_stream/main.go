package main

import (
	"fmt"
	"sync"

	"github.com/KumKeeHyun/gstream"
)

func main() {
	builder := gstream.NewBuilder()

	input1 := make(chan int)
	source1 := gstream.Stream[int](builder).From(input1)
	source1.To()

	input2 := make(chan int)
	source2 := gstream.Stream[int](builder).From(input2)

	source1.Merge(source2).Foreach(func(i int) {
		fmt.Println(i)
	})

	close := builder.BuildAndStart()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		for i := 0; i < 10; i++ {
			input1 <- i
		}
		wg.Done()
	}()
	go func() {
		for i := 100; i < 110; i++ {
			input2 <- i
		}
		wg.Done()
	}()
	wg.Wait()

	close()
}
