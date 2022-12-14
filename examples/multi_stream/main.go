package main

import (
	"context"
	"fmt"
	"github.com/KumKeeHyun/gstream/options/pipe"
	"github.com/KumKeeHyun/gstream/options/sink"
	"sync"
	"time"

	"github.com/KumKeeHyun/gstream"
)

func main() {
	builder := gstream.NewBuilder()

	input1 := make(chan int)
	source1 := gstream.Stream[int](builder).From(input1)
	source1.To(sink.WithTimeout(time.Millisecond))

	input2 := make(chan int)
	source2 := gstream.Stream[int](builder).From(input2)

	source1.Merge(source2, pipe.WithWorkerPool(3)).Foreach(func(_ context.Context, i int) {
		time.Sleep(time.Second)
		fmt.Println("merged", i)
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		builder.BuildAndStart(ctx)
		close(done)
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		for i := 0; i < 10; i++ {
			input1 <- i
		}
		close(input1)
		wg.Done()
	}()
	go func() {
		for i := 100; i < 110; i++ {
			input2 <- i
		}
		close(input2)
		wg.Done()
	}()
	wg.Wait()
	//time.Sleep(time.Second)

	<-done

	cancel()
}
