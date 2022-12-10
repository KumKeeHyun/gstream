package gstream

import (
	"context"
	"github.com/KumKeeHyun/gstream/state"
	"log"
	"time"
)

type Processor[T any] func(ctx context.Context, v T)

type ProcessorSupplier[T, TR any] interface {
	Processor(forwards ...Processor[TR]) Processor[T]
}

// -------------------------------

func newVoidSupplier[T, TR any]() *voidSupplier[T, TR] {
	return &voidSupplier[T, TR]{}
}

type voidSupplier[T, TR any] struct{}

var _ ProcessorSupplier[any, any] = &voidSupplier[any, any]{}

func (p *voidSupplier[T, TR]) Processor(_ ...Processor[TR]) Processor[T] {
	return func(ctx context.Context, v T) {}
}

// -------------------------------

func newFallThroughSupplier[T any]() *fallThroughSupplier[T] {
	return &fallThroughSupplier[T]{}
}

type fallThroughSupplier[T any] struct {
}

var _ ProcessorSupplier[any, any] = &fallThroughSupplier[any]{}

func (p *fallThroughSupplier[T]) Processor(forwards ...Processor[T]) Processor[T] {
	if len(forwards) == 1 {
		return forwards[0]
	}
	return func(ctx context.Context, v T) {
		for _, forward := range forwards {
			forward(ctx, v)
		}
	}
}

// -------------------------------

func newFilterSupplier[T any](filter func(T) bool) *filterSupplier[T] {
	return &filterSupplier[T]{
		filter: filter,
	}
}

type filterSupplier[T any] struct {
	filter func(T) bool
}

var _ ProcessorSupplier[any, any] = &filterSupplier[any]{}

func (p *filterSupplier[T]) Processor(forwards ...Processor[T]) Processor[T] {
	return func(ctx context.Context, v T) {
		if p.filter(v) {
			for _, forward := range forwards {
				forward(ctx, v)
			}
		}
	}
}

// -------------------------------

func newForeachSupplier[T any](foreacher func(T)) *foreachSupplier[T] {
	return &foreachSupplier[T]{
		foreacher: foreacher,
	}
}

type foreachSupplier[T any] struct {
	foreacher func(T)
}

var _ ProcessorSupplier[any, any] = &foreachSupplier[any]{}

func (p *foreachSupplier[T]) Processor(_ ...Processor[T]) Processor[T] {
	return func(ctx context.Context, v T) {
		p.foreacher(v)
	}
}

// -------------------------------

func newMapSupplier[T, TR any](mapper func(T) TR) *mapSupplier[T, TR] {
	return &mapSupplier[T, TR]{
		mapper: mapper,
	}
}

type mapSupplier[T, TR any] struct {
	mapper func(T) TR
}

var _ ProcessorSupplier[any, any] = &mapSupplier[any, any]{}

func (p *mapSupplier[T, TR]) Processor(forwards ...Processor[TR]) Processor[T] {
	return func(ctx context.Context, v T) {
		for _, forward := range forwards {
			forward(ctx, p.mapper(v))
		}
	}
}

// -------------------------------

func newFlatMapSupplier[T, TR any](flatMapper func(T) []TR) *flatMapSupplier[T, TR] {
	return &flatMapSupplier[T, TR]{
		flatMapper: flatMapper,
	}
}

type flatMapSupplier[T, TR any] struct {
	flatMapper func(T) []TR
}

var _ ProcessorSupplier[any, any] = &flatMapSupplier[any, any]{}

func (p *flatMapSupplier[T, TR]) Processor(forwards ...Processor[TR]) Processor[T] {
	return func(ctx context.Context, v T) {
		vrs := p.flatMapper(v)
		for _, forward := range forwards {
			for _, vr := range vrs {
				forward(ctx, vr)
			}
		}
	}
}

// -------------------------------

func newSinkSupplier[T any](pipe chan<- T, timeout time.Duration) *sinkSupplier[T] {
	return &sinkSupplier[T]{
		pipe:    pipe,
		timeout: timeout,
	}
}

type sinkSupplier[T any] struct {
	pipe    chan<- T
	timeout time.Duration
}

var _ ProcessorSupplier[any, any] = &sinkSupplier[any]{}

func (p *sinkSupplier[T]) Processor(_ ...Processor[T]) Processor[T] {
	if p.timeout <= 0 {
		return func(ctx context.Context, v T) {
			select {
			case p.pipe <- v:
			case <-ctx.Done():
			}
		}
	}

	return func(ctx context.Context, v T) {
		t := time.NewTimer(p.timeout)
		defer t.Stop()

		select {
		case p.pipe <- v:
		case <-ctx.Done():
		case <-t.C:
			log.Println("warnning: output channel is busy, ignore:", v)
		}
	}
}

// -------------------------------

func newStreamToTableSupplier[K, V any](kvstore state.KeyValueStore[K, V]) *streamToTableSupplier[K, V] {
	return &streamToTableSupplier[K, V]{
		kvstore: kvstore,
	}
}

type streamToTableSupplier[K, V any] struct {
	kvstore state.KeyValueStore[K, V]
}

var _ ProcessorSupplier[KeyValue[any, any], KeyValue[any, Change[any]]] = &streamToTableSupplier[any, any]{}

func (p *streamToTableSupplier[K, V]) Processor(forwards ...Processor[KeyValue[K, Change[V]]]) Processor[KeyValue[K, V]] {
	return func(ctx context.Context, kv KeyValue[K, V]) {
		old, _ := p.kvstore.Get(kv.Key)
		p.kvstore.Put(kv.Key, kv.Value)
		change := NewChange(old, kv.Value)
		ckv := NewKeyValue(kv.Key, change)

		for _, forward := range forwards {
			forward(ctx, ckv)
		}
	}
}

// -------------------------------

func newTableToValueStreamSupplier[K, V any]() *tableToValueStreamSupplier[K, V] {
	return &tableToValueStreamSupplier[K, V]{}
}

type tableToValueStreamSupplier[K, V any] struct {
}

var _ ProcessorSupplier[KeyValue[any, Change[any]], any] = &tableToValueStreamSupplier[any, any]{}

func (p *tableToValueStreamSupplier[K, V]) Processor(forwards ...Processor[V]) Processor[KeyValue[K, Change[V]]] {
	return func(ctx context.Context, ckv KeyValue[K, Change[V]]) {
		for _, forward := range forwards {
			forward(ctx, ckv.Value.NewValue)
		}
	}
}

// -------------------------------

func newTableToStreamSupplier[K, V any]() *tableToStreamSupplier[K, V] {
	return &tableToStreamSupplier[K, V]{}
}

type tableToStreamSupplier[K, V any] struct {
}

var _ ProcessorSupplier[KeyValue[any, Change[any]], KeyValue[any, any]] = &tableToStreamSupplier[any, any]{}

func (p *tableToStreamSupplier[K, V]) Processor(forwards ...Processor[KeyValue[K, V]]) Processor[KeyValue[K, Change[V]]] {
	return func(ctx context.Context, ckv KeyValue[K, Change[V]]) {
		kv := NewKeyValue(ckv.Key, ckv.Value.NewValue)
		for _, forward := range forwards {
			forward(ctx, kv)
		}
	}
}

// -------------------------------

func newStreamTableJoinSupplier[K, V, VO, VR any](valueGetter func(K) (VO, error), joiner func(V, VO) VR) *streamTableJoinSupplier[K, V, VO, VR] {
	return &streamTableJoinSupplier[K, V, VO, VR]{
		valueGetter: valueGetter,
		joiner:      joiner,
	}
}

type streamTableJoinSupplier[K, V, VO, VR any] struct {
	valueGetter func(K) (VO, error)
	joiner      func(V, VO) VR
}

var _ ProcessorSupplier[KeyValue[any, any], KeyValue[any, any]] = &streamTableJoinSupplier[any, any, any, any]{}

func (p *streamTableJoinSupplier[K, V, VO, VR]) Processor(forwards ...Processor[KeyValue[K, VR]]) Processor[KeyValue[K, V]] {
	return func(ctx context.Context, kv KeyValue[K, V]) {
		vo, err := p.valueGetter(kv.Key)
		if err == nil {
			jkv := NewKeyValue(kv.Key, p.joiner(kv.Value, vo))
			for _, forward := range forwards {
				forward(ctx, jkv)
			}
		}
	}
}

// -------------------------------

func newStreamAggregateSupplier[K, V, VR any](initializer func() VR, aggregator func(KeyValue[K, V], VR) VR, kvstore state.KeyValueStore[K, VR]) *streamAggregateSupplier[K, V, VR] {
	return &streamAggregateSupplier[K, V, VR]{
		initializer: initializer,
		aggregator:  aggregator,
		kvstore:     kvstore,
	}
}

type streamAggregateSupplier[K, V, VR any] struct {
	initializer func() VR
	aggregator  func(KeyValue[K, V], VR) VR
	kvstore     state.KeyValueStore[K, VR]
}

var _ ProcessorSupplier[KeyValue[any, any], KeyValue[any, Change[any]]] = &streamAggregateSupplier[any, any, any]{}

func (p *streamAggregateSupplier[K, V, VR]) Processor(forwards ...Processor[KeyValue[K, Change[VR]]]) Processor[KeyValue[K, V]] {
	return func(ctx context.Context, kv KeyValue[K, V]) {
		oldAgg, err := p.kvstore.Get(kv.Key)
		if err != nil { // TODO: error is not exists
			oldAgg = p.initializer()
		}
		newAgg := p.aggregator(kv, oldAgg)
		p.kvstore.Put(kv.Key, newAgg)

		ckv := NewKeyValue(kv.Key, NewChange(oldAgg, newAgg))
		for _, forward := range forwards {
			forward(ctx, ckv)
		}
	}
}
