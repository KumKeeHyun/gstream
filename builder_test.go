package gstream

import (
	"context"
	"github.com/KumKeeHyun/gstream/state/materialized"
	"strconv"
	"testing"

	"go.uber.org/goleak"
)

func TestCloseStream(t *testing.T) {
	defer goleak.VerifyNone(t)

	builder := NewBuilder()

	intStream := Stream[int](builder).From(make(chan int))
	strStream := Stream[string](builder).From(make(chan string))

	mater, _ := materialized.New(
		materialized.WithInMemory[int, string](),
	)
	Table[int, string](builder).From(make(chan string),
		func(s string) int { return 0 },
		mater)
	mergedStream := Map(intStream, strconv.Itoa).Merge(strStream)
	mergedStream.Pipe()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		builder.BuildAndStart(ctx)
		done <- struct{}{}
	}()

	cancel()
	<-done
}
