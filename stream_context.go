package gstream

import (
	"context"
	"sync"
)

type Closer interface {
	Close() error
}

func newPipeCloser[T any](pipe chan T) Closer {
	return &pipeCloser[T]{
		pipe: pipe,
	}
}

type pipeCloser[T any] struct {
	pipe chan T
}

func (p *pipeCloser[T]) Close() (err error) {
	defer func() {
		if r := recover(); r != nil {

		}
	}()

	close(p.pipe)
	return nil
}

func newStreamContext() *streamContext {
	return &streamContext{
		ctx: context.Background(),
		wg:  &sync.WaitGroup{},
	}
}

type streamContext struct {
	ctx context.Context
	wg  *sync.WaitGroup

	resources map[Closer]struct{}
}

func (sctx *streamContext) add(closer Closer) {
	if sctx.resources == nil {
		sctx.resources = make(map[Closer]struct{})
	}
	sctx.resources[closer] = struct{}{}
}

func (sctx *streamContext) cleanUp() {
	for resource := range sctx.resources {
		resource.Close()
	}
}
