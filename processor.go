package gstream

import (
	"log"
	"time"
)

type Processor[T any] func(v T)

type ProcessorSupplier[T, TR any] interface {
	Processor(forwards ...Processor[TR]) Processor[T]
}

// -------------------------------

func newVoidProcessorSupplier[T, TR any]() *voidProcessorSupplier[T, TR] {
	return &voidProcessorSupplier[T, TR]{}
}

type voidProcessorSupplier[T, TR any] struct{}

var _ ProcessorSupplier[any, any] = &voidProcessorSupplier[any, any]{}

func (p *voidProcessorSupplier[T, TR]) Processor(forwards ...Processor[TR]) Processor[T] {
	return func(v T) {}
}

// -------------------------------

func newFallThroughProcessorSupplier[T any]() *fallThroughProcessorSupplier[T] {
	return &fallThroughProcessorSupplier[T]{}
}

type fallThroughProcessorSupplier[T any] struct {
}

var _ ProcessorSupplier[any, any] = &fallThroughProcessorSupplier[any]{}

func (p *fallThroughProcessorSupplier[T]) Processor(forwards ...Processor[T]) Processor[T] {
	return func(v T) {
		for _, forward := range forwards {
			forward(v)
		}
	}
}

// -------------------------------

func newFilterProcessorSupplier[T any](filter func(T) bool) *filterProcessorSupplier[T] {
	return &filterProcessorSupplier[T]{
		filter: filter,
	}
}

type filterProcessorSupplier[T any] struct {
	filter func(T) bool
}

var _ ProcessorSupplier[any, any] = &filterProcessorSupplier[any]{}

func (p *filterProcessorSupplier[T]) Processor(forwards ...Processor[T]) Processor[T] {
	return func(v T) {
		if p.filter(v) {
			for _, forward := range forwards {
				forward(v)
			}
		}
	}
}

// -------------------------------

func newForeachProcessorSupplier[T any](foreacher func(T)) *foreachProcessorSupplier[T] {
	return &foreachProcessorSupplier[T]{
		foreacher: foreacher,
	}
}

type foreachProcessorSupplier[T any] struct {
	foreacher func(T)
}

var _ ProcessorSupplier[any, any] = &foreachProcessorSupplier[any]{}

func (p *foreachProcessorSupplier[T]) Processor(forwards ...Processor[T]) Processor[T] {
	return func(v T) {
		p.foreacher(v)
	}
}

// -------------------------------

func newMapProcessorSupplier[T, TR any](mapper func(T) TR) *mapProcessorSupplier[T, TR] {
	return &mapProcessorSupplier[T, TR]{
		mapper: mapper,
	}
}

type mapProcessorSupplier[T, TR any] struct {
	mapper func(T) TR
}

var _ ProcessorSupplier[any, any] = &mapProcessorSupplier[any, any]{}

func (p *mapProcessorSupplier[T, TR]) Processor(forwards ...Processor[TR]) Processor[T] {
	return func(v T) {
		for _, forward := range forwards {
			forward(p.mapper(v))
		}
	}
}

// -------------------------------

func newFlatMapProcessorSupplier[T, TR any](flatMapper func(T) []TR) *flatMapProcessorSupplier[T, TR] {
	return &flatMapProcessorSupplier[T, TR]{
		flatMapper: flatMapper,
	}
}

type flatMapProcessorSupplier[T, TR any] struct {
	flatMapper func(T) []TR
}

var _ ProcessorSupplier[any, any] = &flatMapProcessorSupplier[any, any]{}

func (p *flatMapProcessorSupplier[T, TR]) Processor(forwards ...Processor[TR]) Processor[T] {
	return func(v T) {
		vrs := p.flatMapper(v)
		for _, forward := range forwards {
			for _, vr := range vrs {
				forward(vr)
			}
		}
	}
}

// -------------------------------

func newSinkProcessorSupplier[T any](o chan T, d time.Duration) *sinkProcessorSupplier[T] {
	return &sinkProcessorSupplier[T]{
		output:   o,
		duration: d,
	}
}

type sinkProcessorSupplier[T any] struct {
	output   chan T
	duration time.Duration
}

var _ ProcessorSupplier[any, any] = &sinkProcessorSupplier[any]{}

func (p *sinkProcessorSupplier[T]) Processor(_ ...Processor[T]) Processor[T] {
	return func(v T) {
		bomb := time.After(p.duration)
		select {
		case p.output <- v:
		case <-bomb:
			log.Println("warnning: output channel is busy, ingore:", v)
		}
	}
}

// -------------------------------

func newBlockingSinkProcessorSupplier[T any](o chan T) *blockingSinkProcessorSupplier[T] {
	return &blockingSinkProcessorSupplier[T]{
		output: o,
	}
}

type blockingSinkProcessorSupplier[T any] struct {
	output chan T
}

var _ ProcessorSupplier[any, any] = &blockingSinkProcessorSupplier[any]{}

func (p *blockingSinkProcessorSupplier[T]) Processor(_ ...Processor[T]) Processor[T] {
	return func(v T) {
		p.output <- v
	}
}

// -------------------------------

func newReduceProcessorSupplier[T any](output chan T, init func() T, accumulator func(T, T) T) *reduceProcessorSupplier[T] {
	return &reduceProcessorSupplier[T]{
		output:      output,
		init:        init,
		accumulator: accumulator,
	}
}

type reduceProcessorSupplier[T any] struct {
	output      chan T
	init        func() T
	accumulator func(T, T) T
}

var _ ProcessorSupplier[any, any] = &reduceProcessorSupplier[any]{}

func (p *reduceProcessorSupplier[T]) Processor(forwards ...Processor[T]) Processor[T] {
	total := p.init()
	return func(v T) {
		total = p.accumulator(total, v)
		p.output <- total
	}
}

// -------------------------------

func newStreamToTableProcessorSupplier[K, V any](kvstore KeyValueStore[K, V]) *streamToTableProcessorSupplier[K, V] {
	return &streamToTableProcessorSupplier[K, V]{
		kvstore: kvstore,
	}
}

type streamToTableProcessorSupplier[K, V any] struct {
	kvstore KeyValueStore[K, V]
}

var _ ProcessorSupplier[KeyValue[any, any], KeyValue[any, Change[any]]] = &streamToTableProcessorSupplier[any, any]{}

func (p *streamToTableProcessorSupplier[K, V]) Processor(forwards ...Processor[KeyValue[K, Change[V]]]) Processor[KeyValue[K, V]] {
	return func(kv KeyValue[K, V]) {
		old, _ := p.kvstore.Get(kv.Key)
		p.kvstore.Put(kv.Key, kv.Value)
		change := NewChange(old, kv.Value)
		ckv := NewKeyValue(kv.Key, change)

		for _, forward := range forwards {
			forward(ckv)
		}
	}
}

// -------------------------------

func newTableToStreamProcessorSupplier[K, V any]() *tableToStreamProcessorSupplier[K, V] {
	return &tableToStreamProcessorSupplier[K, V]{}
}

type tableToStreamProcessorSupplier[K, V any] struct {
}

var _ ProcessorSupplier[KeyValue[any, Change[any]], any] = &tableToStreamProcessorSupplier[any, any]{}

func (p *tableToStreamProcessorSupplier[K, V]) Processor(forwards ...Processor[V]) Processor[KeyValue[K, Change[V]]] {
	return func(ckv KeyValue[K, Change[V]]) {
		for _, forward := range forwards {
			forward(ckv.Value.NewValue)
		}
	}
}

// -------------------------------

func newStreamTableJoinProcessorSupplier[K, V, VO, VR any](valueGetter func(K) (VO, error), joiner func(V, VO) VR) *streamTableJoinProcessorSupplier[K, V, VO, VR] {
	return &streamTableJoinProcessorSupplier[K, V, VO, VR]{
		valueGetter: valueGetter,
		joiner:      joiner,
	}
}

type streamTableJoinProcessorSupplier[K, V, VO, VR any] struct {
	valueGetter func(K) (VO, error)
	joiner      func(V, VO) VR
}

var _ ProcessorSupplier[KeyValue[any, any], KeyValue[any, any]] = &streamTableJoinProcessorSupplier[any, any, any, any]{}

func (p *streamTableJoinProcessorSupplier[K, V, VO, VR]) Processor(forwards ...Processor[KeyValue[K, VR]]) Processor[KeyValue[K, V]] {
	return func(kv KeyValue[K, V]) {
		vo, err := p.valueGetter(kv.Key)
		if err == nil {
			for _, forward := range forwards {
				forward(NewKeyValue(kv.Key, p.joiner(kv.Value, vo)))
			}
		}
	}
}