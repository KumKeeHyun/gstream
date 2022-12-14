package gstream

import (
	"context"
	"github.com/KumKeeHyun/gstream/options/source"
	"github.com/KumKeeHyun/gstream/state/materialized"
	"testing"

	"go.uber.org/goleak"
)

func TestStream_SinglePool_CloseChan(t *testing.T) {
	defer goleak.VerifyNone(t)

	ch := make(chan int, 10)
	for i := 0; i < 10; i++ {
		ch <- i
	}
	close(ch)

	builder := NewBuilder()
	Stream[int](builder).From(ch, source.WithWorkerPool(1))
	builder.BuildAndStart(context.Background())
}

func TestStream_SinglePool_Cancel(t *testing.T) {
	defer goleak.VerifyNone(t)

	ch := make(chan int, 10)
	for i := 0; i < 10; i++ {
		ch <- i
	}

	builder := NewBuilder()
	Stream[int](builder).From(ch, source.WithWorkerPool(1))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	cancel()

	go func() {
		builder.BuildAndStart(ctx)
		close(done)
	}()
	<-done
}

func TestStream_MultiPool_CloseChan(t *testing.T) {
	defer goleak.VerifyNone(t)

	ch := make(chan int, 10)
	for i := 0; i < 10; i++ {
		ch <- i
	}
	close(ch)

	builder := NewBuilder()
	Stream[int](builder).From(ch, source.WithWorkerPool(5))
	builder.BuildAndStart(context.Background())
}

func TestStream_MultiPool_Cancel(t *testing.T) {
	defer goleak.VerifyNone(t)

	ch := make(chan int, 10)
	for i := 0; i < 10; i++ {
		ch <- i
	}

	builder := NewBuilder()
	Stream[int](builder).From(ch, source.WithWorkerPool(5))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	cancel()

	go func() {
		builder.BuildAndStart(ctx)
		close(done)
	}()
	<-done
}

func TestTable_SinglePool_CloseChan(t *testing.T) {
	defer goleak.VerifyNone(t)

	ch := make(chan int, 10)
	for i := 0; i < 10; i++ {
		ch <- i
	}
	close(ch)

	builder := NewBuilder()
	mater, _ := materialized.New[int, int]()
	Table[int, int](builder).From(ch,
		func(v int) int { return v },
		mater,
		source.WithWorkerPool(1),
	)
	builder.BuildAndStart(context.Background())
}

func TestTable_SinglePool_Cancel(t *testing.T) {
	defer goleak.VerifyNone(t)

	ch := make(chan int, 10)
	for i := 0; i < 10; i++ {
		ch <- i
	}

	builder := NewBuilder()
	mater, _ := materialized.New[int, int]()
	Table[int, int](builder).From(ch,
		func(v int) int { return v },
		mater,
		source.WithWorkerPool(1),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	cancel()

	go func() {
		builder.BuildAndStart(ctx)
		close(done)
	}()
	<-done
}

func TestTable_MultiPool_CloseChan(t *testing.T) {
	defer goleak.VerifyNone(t)

	ch := make(chan int, 10)
	for i := 0; i < 10; i++ {
		ch <- i
	}
	close(ch)

	builder := NewBuilder()
	mater, _ := materialized.New[int, int]()
	Table[int, int](builder).From(ch,
		func(v int) int { return v },
		mater,
		source.WithWorkerPool(5),
	)
	builder.BuildAndStart(context.Background())
}

func TestTable_MultiPool_Cancel(t *testing.T) {
	defer goleak.VerifyNone(t)

	ch := make(chan int, 10)
	for i := 0; i < 10; i++ {
		ch <- i
	}

	builder := NewBuilder()
	mater, _ := materialized.New[int, int]()
	Table[int, int](builder).From(ch,
		func(v int) int { return v },
		mater,
		source.WithWorkerPool(5),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	cancel()

	go func() {
		builder.BuildAndStart(ctx)
		close(done)
	}()
	<-done
}
