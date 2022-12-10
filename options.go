package gstream

import (
	"github.com/KumKeeHyun/gstream/options/pipe"
	"github.com/KumKeeHyun/gstream/options/sink"
	"github.com/KumKeeHyun/gstream/options/source"
	"sync"
	"time"
)

type options interface {
	SetWorkerPool(pool int)
	SetBufferedChan(cap int)
	SetTimeout(t time.Duration)
}

type optionsImpl struct {
	workerPool int
	isBuffer   bool
	buffer     int
	timeout    time.Duration
}

func (o *optionsImpl) SetWorkerPool(pool int) {
	o.workerPool = pool
}

func (o *optionsImpl) SetBufferedChan(cap int) {
	o.isBuffer = true
	o.buffer = cap
}

func (o *optionsImpl) SetTimeout(t time.Duration) {
	o.timeout = t
}

type sourceOption struct {
	optionsImpl
}

func (o *sourceOption) WorkerPool() int {
	return o.workerPool
}

func newSourceOption(opts ...source.Option) *sourceOption {
	srcOpt := &sourceOption{
		optionsImpl: optionsImpl{
			workerPool: 1,
		},
	}
	for _, opt := range opts {
		opt(srcOpt)
	}
	return srcOpt
}

type pipeOption[T any] struct {
	optionsImpl
	once sync.Once
	pipe chan T
}

func (o *pipeOption[T]) WorkerPool() int {
	return o.workerPool
}

func (o *pipeOption[T]) BuildPipe() chan T {
	o.once.Do(func() {
		if o.isBuffer {
			o.pipe = make(chan T, o.buffer)
		} else {
			o.pipe = make(chan T)
		}
	})
	return o.pipe
}

func newPipeOption[T any](opts ...pipe.Option) *pipeOption[T] {
	pipeOpt := &pipeOption[T]{
		optionsImpl: optionsImpl{
			workerPool: 1,
			isBuffer:   false,
		},
	}
	for _, opt := range opts {
		opt(pipeOpt)
	}
	return pipeOpt
}

type sinkOption[T any] struct {
	optionsImpl
	once sync.Once
	pipe chan T
}

func (o *sinkOption[T]) BuildPipe() chan T {
	o.once.Do(func() {
		if o.isBuffer {
			o.pipe = make(chan T, o.buffer)
		} else {
			o.pipe = make(chan T)
		}
	})
	return o.pipe
}

func (o *sinkOption[T]) Timeout() time.Duration {
	return o.timeout
}

func newSinkOption[T any](opts ...sink.Option) *sinkOption[T] {
	sinkOpt := &sinkOption[T]{
		optionsImpl: optionsImpl{
			isBuffer: false,
			timeout:  -1,
		},
	}
	for _, opt := range opts {
		opt(sinkOpt)
	}
	return sinkOpt
}
