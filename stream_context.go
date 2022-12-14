package gstream

import (
	"context"
	"sync"
)

type Closer interface {
	Close() error
}

type pipeCloser interface {
	Closer

	Add(n int)
	Done()
}

func newPipeCloser[T any](pipe chan<- T) pipeCloser {
	return &pipeCloserImpl[T]{
		mu:     &sync.Mutex{},
		refCnt: 0,
		pipe:   pipe,
	}
}

type pipeCloserImpl[T any] struct {
	mu     *sync.Mutex
	refCnt int
	pipe   chan<- T
}

func (p *pipeCloserImpl[T]) Close() (err error) {
	defer func() {
		if r := recover(); r != nil {

		}
	}()

	close(p.pipe)
	return nil
}

func (p *pipeCloserImpl[T]) Add(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.refCnt += n
}

func (p *pipeCloserImpl[T]) Done() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.refCnt--
	if p.refCnt == 0 {
		p.Close()
	}
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

	stores map[Closer]struct{}
	pipes  map[routineID]map[pipeCloser]struct{}
}

func (sctx *streamContext) addPipe(pid routineID, pipe pipeCloser) {
	if sctx.pipes == nil {
		sctx.pipes = make(map[routineID]map[pipeCloser]struct{})
	}

	pipes, exists := sctx.pipes[pid]
	if !exists {
		pipes = make(map[pipeCloser]struct{})
		sctx.pipes[pid] = pipes
	}

	pipes[pipe] = struct{}{}
}

func (sctx *streamContext) addStore(closer Closer) {
	if sctx.stores == nil {
		sctx.stores = make(map[Closer]struct{})
	}
	sctx.stores[closer] = struct{}{}
}

func (sctx *streamContext) startRoutine(rid routineID) {
	sctx.wg.Add(1)
	for pipes := range sctx.pipes[rid] {
		pipes.Add(1)
	}
}

func (sctx *streamContext) doneRoutine(rid routineID) {
	for pipes := range sctx.pipes[rid] {
		pipes.Done()
	}
	sctx.wg.Done()
}

// cleanUpStores clean up stores when all routines are closed
func (sctx *streamContext) cleanUpStores() {
	for store := range sctx.stores {
		store.Close()
	}
}
