package main

import (
	"context"
	"fmt"
	"github.com/KumKeeHyun/gstream"
	"math/rand"
)

func main() {
	input := make(chan int)

	builder := gstream.NewBuilder()
	src := gstream.Stream[int](builder).From(input)

	mapped, failed := gstream.MapErr(src, func(_ context.Context, i int) (string, error) {
		if i > 30 {
			return "", fmt.Errorf("too big: %d", i)
		}
		return fmt.Sprintf("itoa %d", i), nil
	})
	mapped.Foreach(func(_ context.Context, s string) {
		fmt.Println("success:", s)
	})
	failed.Foreach(func(_ context.Context, i int, err error) {
		fmt.Printf("fail: %d, err: %v\n", i, err)
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		builder.BuildAndStart(ctx)
		done <- struct{}{}
	}()

	for i := 0; i < 10; i++ {
		input <- rand.Intn(50)
	}
	cancel()
	<-done
}
