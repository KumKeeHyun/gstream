package gstream

import (
	"context"
	"sync"
)

type Option func(opts *Options)

func WithBufferedChan(size int) Option {
	return func(opts *Options) {
		opts.bufSize = size
	}
}

func WithWorkerPool(size int) Option {
	return func(opts *Options) {
		opts.poolSize = size
	}
}

type Options struct {
	bufSize  int
	poolSize int
}

func newRoutine[T any](pipe <-chan T, poolSize int, processor Processor[T]) *routine[T] {
	return &routine[T]{
		pipe:     pipe,
		poolSize: poolSize,
		process:  processor,
	}
}

type routine[T any] struct {
	pipe     <-chan T
	poolSize int
	process  Processor[T]
}

func (r *routine[T]) Run(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(r.poolSize)
	for i := 0; i < r.poolSize; i++ {
		go worker(ctx, wg, r.pipe, r.process)
	}
}

func worker[T any](ctx context.Context, wg *sync.WaitGroup, pipe <-chan T, process Processor[T]) {
	defer func() {
		wg.Done()
	}()
	for {
		select {
		case d, ok := <-pipe:
			if !ok {
				return
			}

			select {
			case <-ctx.Done():
				return
			default:
				process(ctx, d)
			}
		case <-ctx.Done():
			return
		}
	}
}
