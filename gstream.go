package gstream

import (
	"context"
	"github.com/KumKeeHyun/gstream/options/pipe"
	"github.com/KumKeeHyun/gstream/options/sink"
	"github.com/KumKeeHyun/gstream/state"
	"github.com/KumKeeHyun/gstream/state/materialized"
)

type GStream[T any] interface {
	Filter(func(T) bool) GStream[T]
	Foreach(func(context.Context, T))
	Map(func(context.Context, T) T) GStream[T]
	FlatMap(func(context.Context, T) []T) GStream[T]
	Merge(GStream[T], ...pipe.Option) GStream[T]
	Pipe(...pipe.Option) GStream[T]
	To(...sink.Option) <-chan T
}

type KeyValueGStream[K, V any] interface {
	Filter(func(K, V) bool) KeyValueGStream[K, V]
	Foreach(func(context.Context, K, V))
	Map(func(context.Context, K, V) (K, V)) KeyValueGStream[K, V]
	MapValues(func(context.Context, V) V) KeyValueGStream[K, V]
	FlatMap(func(context.Context, K, V) []KeyValue[K, V]) KeyValueGStream[K, V]
	FlatMapValues(func(context.Context, V) []V) KeyValueGStream[K, V]
	Merge(KeyValueGStream[K, V], ...pipe.Option) KeyValueGStream[K, V]
	Pipe(...pipe.Option) KeyValueGStream[K, V]
	To(...sink.Option) <-chan KeyValue[K, V]

	ToValueStream() GStream[V]
	ToTable(materialized.Materialized[K, V]) GTable[K, V]
}

type JoinedGStream[K, V, VO, VR any] interface {
	JoinTable(GTable[K, VO], func(V, VO) VR) KeyValueGStream[K, VR]
}

// -------------------------------

type gstream[T any] struct {
	builder  *builder
	rid      routineID
	addChild func(*processorNode[T, T])
}

var _ GStream[any] = &gstream[any]{}

func (s *gstream[T]) Filter(filter func(T) bool) GStream[T] {
	filterSupplier := newFilterSupplier(filter)
	filterNode := newProcessorNode[T, T](filterSupplier)
	s.addChild(filterNode)

	return &gstream[T]{
		builder:  s.builder,
		rid:      s.rid,
		addChild: curryingAddChild[T, T, T](filterNode),
	}
}

func (s *gstream[T]) Foreach(foreacher func(context.Context, T)) {
	foreachSupplier := newForeachSupplier(foreacher)
	foreachNode := newProcessorNode[T, T](foreachSupplier)
	s.addChild(foreachNode)
}

func (s *gstream[T]) Map(mapper func(context.Context, T) T) GStream[T] {
	mapSupplier := newMapSupplier(mapper)
	mapNode := newProcessorNode[T, T](mapSupplier)
	s.addChild(mapNode)

	return &gstream[T]{
		builder:  s.builder,
		rid:      s.rid,
		addChild: curryingAddChild[T, T, T](mapNode),
	}
}

func (s *gstream[T]) FlatMap(flatMapper func(context.Context, T) []T) GStream[T] {
	flatMapNode := newProcessorNode[T, T](newFlatMapSupplier(flatMapper))
	s.addChild(flatMapNode)

	return &gstream[T]{
		builder:  s.builder,
		rid:      s.rid,
		addChild: curryingAddChild[T, T, T](flatMapNode),
	}
}

func (s *gstream[T]) Merge(ms GStream[T], opts ...pipe.Option) GStream[T] {
	msImpl := ms.(*gstream[T])

	// create new routine if streams are in different routine.
	if s.rid != msImpl.rid {
		opt := newPipeOption[T](opts...)
		p := opt.BuildPipe()

		sinkNode := newProcessorNode[T, T](newSinkSupplier(p, 0))
		s.addChild(sinkNode)
		msImpl.addChild(sinkNode)

		srcNode := newSourceNode(s.builder.getRoutineID(), s.builder.sctx, p, opt.WorkerPool())
		addChild(sinkNode, srcNode)

		return &gstream[T]{
			builder:  s.builder,
			rid:      srcNode.RoutineId(),
			addChild: curryingAddChild[T, T, T](srcNode),
		}
	}

	mergeNode := newFallThroughNode[T]()
	s.addChild(mergeNode)
	msImpl.addChild(mergeNode)

	return &gstream[T]{
		builder:  s.builder,
		addChild: curryingAddChild[T, T, T](mergeNode),
	}
}

func (s *gstream[T]) Pipe(opts ...pipe.Option) GStream[T] {
	opt := newPipeOption[T](opts...)
	p := opt.BuildPipe()

	sinkNode := newProcessorNode[T, T](newSinkSupplier(p, 0))
	s.addChild(sinkNode)

	srcNode := newSourceNode(s.builder.getRoutineID(), s.builder.sctx, p, opt.WorkerPool())
	addChild(sinkNode, srcNode)

	s.builder.sctx.add(newPipeCloser(p))

	return &gstream[T]{
		builder:  s.builder,
		rid:      srcNode.RoutineId(),
		addChild: curryingAddChild[T, T, T](srcNode),
	}
}

func (s *gstream[T]) To(opts ...sink.Option) <-chan T {
	opt := newSinkOption[T](opts...)
	p := opt.BuildPipe()

	sinkNode := newProcessorNode[T, T](newSinkSupplier(p, opt.Timeout()))
	s.addChild(sinkNode)
	s.builder.sctx.add(newPipeCloser(p))

	return p
}

// -------------------------------

func Map[T, TR any](s GStream[T], mapper func(context.Context, T) TR) GStream[TR] {
	sImpl := s.(*gstream[T])
	mapSupplier := newMapSupplier(mapper)
	mapNode := newProcessorNode[T, TR](mapSupplier)
	castAddChild[T, TR](sImpl.addChild)(mapNode)

	return &gstream[TR]{
		builder:  sImpl.builder,
		rid:      sImpl.rid,
		addChild: curryingAddChild[T, TR, TR](mapNode),
	}
}

func FlatMap[T, TR any](s GStream[T], flatMapper func(context.Context, T) []TR) GStream[TR] {
	sImpl := s.(*gstream[T])
	flatMapNode := newProcessorNode[T, TR](newFlatMapSupplier(flatMapper))
	castAddChild[T, TR](sImpl.addChild)(flatMapNode)

	return &gstream[TR]{
		builder:  sImpl.builder,
		rid:      sImpl.rid,
		addChild: curryingAddChild[T, TR, TR](flatMapNode),
	}
}

func SelectKey[K, V any](s GStream[V], keySelecter func(V) K) KeyValueGStream[K, V] {
	kvs := Map(s, func(_ context.Context, v V) KeyValue[K, V] {
		return NewKeyValue(keySelecter(v), v)
	}).(*gstream[KeyValue[K, V]])

	return &keyValueGStream[K, V]{
		builder:  kvs.builder,
		rid:      kvs.rid,
		addChild: kvs.addChild,
	}
}

type keyValueGStream[K, V any] struct {
	builder  *builder
	rid      routineID
	addChild func(*processorNode[KeyValue[K, V], KeyValue[K, V]])
}

var _ KeyValueGStream[any, any] = &keyValueGStream[any, any]{}

func (kvs *keyValueGStream[K, V]) gstream() *gstream[KeyValue[K, V]] {
	return &gstream[KeyValue[K, V]]{
		builder:  kvs.builder,
		rid:      kvs.rid,
		addChild: kvs.addChild,
	}
}

func (kvs *keyValueGStream[K, V]) Filter(filter func(K, V) bool) KeyValueGStream[K, V] {
	s := kvs.gstream().Filter(func(kv KeyValue[K, V]) bool {
		return filter(kv.Key, kv.Value)
	}).(*gstream[KeyValue[K, V]])

	return &keyValueGStream[K, V]{
		builder:  s.builder,
		rid:      s.rid,
		addChild: s.addChild,
	}
}

func (kvs *keyValueGStream[K, V]) Foreach(foreacher func(context.Context, K, V)) {
	kvs.gstream().Foreach(func(ctx context.Context, kv KeyValue[K, V]) {
		foreacher(ctx, kv.Key, kv.Value)
	})
}

func (kvs *keyValueGStream[K, V]) Map(mapper func(context.Context, K, V) (K, V)) KeyValueGStream[K, V] {
	s := kvs.gstream().Map(func(ctx context.Context, kv KeyValue[K, V]) KeyValue[K, V] {
		kr, vr := mapper(ctx, kv.Key, kv.Value)
		return NewKeyValue(kr, vr)
	}).(*gstream[KeyValue[K, V]])

	return &keyValueGStream[K, V]{
		builder:  s.builder,
		rid:      s.rid,
		addChild: s.addChild,
	}
}

func (kvs *keyValueGStream[K, V]) MapValues(mapper func(context.Context, V) V) KeyValueGStream[K, V] {
	s := kvs.gstream().Map(func(ctx context.Context, kv KeyValue[K, V]) KeyValue[K, V] {
		return NewKeyValue(kv.Key, mapper(ctx, kv.Value))
	}).(*gstream[KeyValue[K, V]])

	return &keyValueGStream[K, V]{
		builder:  s.builder,
		rid:      s.rid,
		addChild: s.addChild,
	}
}

func (kvs *keyValueGStream[K, V]) FlatMap(flatMapper func(context.Context, K, V) []KeyValue[K, V]) KeyValueGStream[K, V] {
	s := kvs.gstream().FlatMap(func(ctx context.Context, kv KeyValue[K, V]) []KeyValue[K, V] {
		return flatMapper(ctx, kv.Key, kv.Value)
	}).(*gstream[KeyValue[K, V]])

	return &keyValueGStream[K, V]{
		builder:  s.builder,
		rid:      s.rid,
		addChild: s.addChild,
	}
}

func (kvs *keyValueGStream[K, V]) FlatMapValues(flatMapper func(context.Context, V) []V) KeyValueGStream[K, V] {
	s := kvs.gstream().FlatMap(func(ctx context.Context, kv KeyValue[K, V]) []KeyValue[K, V] {
		mvs := flatMapper(ctx, kv.Value)
		kvs := make([]KeyValue[K, V], len(mvs))
		for i, mv := range mvs {
			kvs[i] = NewKeyValue(kv.Key, mv)
		}
		return kvs
	}).(*gstream[KeyValue[K, V]])

	return &keyValueGStream[K, V]{
		builder:  s.builder,
		rid:      s.rid,
		addChild: s.addChild,
	}
}

func (kvs *keyValueGStream[K, V]) Merge(mkvs KeyValueGStream[K, V], opts ...pipe.Option) KeyValueGStream[K, V] {
	mkvsImpl := mkvs.(*keyValueGStream[K, V]).gstream()
	ms := kvs.gstream().Merge(mkvsImpl, opts...).(*gstream[KeyValue[K, V]])

	return &keyValueGStream[K, V]{
		builder:  ms.builder,
		rid:      ms.rid,
		addChild: ms.addChild,
	}
}

func (kvs *keyValueGStream[K, V]) Pipe(opts ...pipe.Option) KeyValueGStream[K, V] {
	s := kvs.gstream().Pipe(opts...).(*gstream[KeyValue[K, V]])

	return &keyValueGStream[K, V]{
		builder:  s.builder,
		rid:      s.rid,
		addChild: s.addChild,
	}
}

func (kvs *keyValueGStream[K, V]) To(opts ...sink.Option) <-chan KeyValue[K, V] {
	return kvs.gstream().To(opts...)
}

func (kvs *keyValueGStream[K, V]) ToValueStream() GStream[V] {
	return Map[KeyValue[K, V], V](kvs.gstream(), func(_ context.Context, kv KeyValue[K, V]) V {
		return kv.Value
	})
}

func (kvs *keyValueGStream[K, V]) ToTable(mater materialized.Materialized[K, V]) GTable[K, V] {
	kvstore := state.NewKeyValueStore(mater)
	if closer, ok := kvstore.(Closer); ok {
		kvs.builder.sctx.add(closer)
	}

	streamToTableSupplier := newStreamToTableSupplier(kvstore)
	streamToTableNode := newStreamToTableNode(streamToTableSupplier)
	castAddChild[KeyValue[K, V], KeyValue[K, Change[V]]](kvs.addChild)(streamToTableNode)

	currying := curryingAddChild[KeyValue[K, V], KeyValue[K, Change[V]], KeyValue[K, Change[V]]](streamToTableNode)
	return &gtable[K, V]{
		builder:  kvs.builder,
		rid:      kvs.rid,
		addChild: currying,
		kvstore:  streamToTableSupplier.kvstore,
	}
}

// -------------------------------

func KeyValueMap[K, V, KR, VR any](kvs KeyValueGStream[K, V], mapper func(context.Context, K, V) (KR, VR)) KeyValueGStream[KR, VR] {
	kvsImpl := kvs.(*keyValueGStream[K, V]).gstream()
	mkvs := Map[KeyValue[K, V], KeyValue[KR, VR]](kvsImpl, func(ctx context.Context, kv KeyValue[K, V]) KeyValue[KR, VR] {
		kr, vr := mapper(ctx, kv.Key, kv.Value)
		return NewKeyValue(kr, vr)
	}).(*gstream[KeyValue[KR, VR]])

	return &keyValueGStream[KR, VR]{
		builder:  mkvs.builder,
		rid:      mkvs.rid,
		addChild: mkvs.addChild,
	}
}

func KeyValueMapValues[K, V, VR any](kvs KeyValueGStream[K, V], mapper func(context.Context, V) VR) KeyValueGStream[K, VR] {
	kvsImpl := kvs.(*keyValueGStream[K, V]).gstream()
	mkvs := Map[KeyValue[K, V], KeyValue[K, VR]](kvsImpl, func(ctx context.Context, kv KeyValue[K, V]) KeyValue[K, VR] {
		vr := mapper(ctx, kv.Value)
		return NewKeyValue(kv.Key, vr)
	}).(*gstream[KeyValue[K, VR]])

	return &keyValueGStream[K, VR]{
		builder:  mkvs.builder,
		rid:      mkvs.rid,
		addChild: mkvs.addChild,
	}
}

// -------------------------------

func Joined[K, V, VO, VR any](kvs KeyValueGStream[K, V]) JoinedGStream[K, V, VO, VR] {
	kvsImpl := kvs.(*keyValueGStream[K, V])
	return &joinedGStream[K, V, VO, VR]{
		builder:  kvsImpl.builder,
		rid:      kvsImpl.rid,
		addChild: castAddChild[KeyValue[K, V], KeyValue[K, VR]](kvsImpl.addChild),
	}
}

type joinedGStream[K, V, VO, VR any] struct {
	builder  *builder
	rid      routineID
	addChild func(*processorNode[KeyValue[K, V], KeyValue[K, VR]])
}

var _ JoinedGStream[any, any, any, any] = &joinedGStream[any, any, any, any]{}

func (js *joinedGStream[K, V, VO, VR]) JoinTable(t GTable[K, VO], joiner func(V, VO) VR) KeyValueGStream[K, VR] {
	valueGetter := t.(*gtable[K, VO]).valueGetter()
	joinProcessorSupplier := newStreamTableJoinSupplier(valueGetter, joiner)
	joinNode := newProcessorNode[KeyValue[K, V], KeyValue[K, VR]](joinProcessorSupplier)
	js.addChild(joinNode)

	currying := curryingAddChild[KeyValue[K, V], KeyValue[K, VR], KeyValue[K, VR]](joinNode)
	return &keyValueGStream[K, VR]{
		builder:  js.builder,
		rid:      js.rid,
		addChild: currying,
	}
}

// -------------------------------

func Aggregate[K, V, VR any](kvs KeyValueGStream[K, V], initializer func() VR, aggregator func(KeyValue[K, V], VR) VR, mater materialized.Materialized[K, VR]) GTable[K, VR] {
	kvsImpl := kvs.(*keyValueGStream[K, V])

	kvstore := state.NewKeyValueStore(mater)
	if closer, ok := kvstore.(Closer); ok {
		kvsImpl.builder.sctx.add(closer)
	}

	aggProcessorSupplier := newStreamAggregateSupplier(initializer, aggregator, kvstore)
	aggNode := newProcessorNode[KeyValue[K, V], KeyValue[K, Change[VR]]](aggProcessorSupplier)
	castAddChild[KeyValue[K, V], KeyValue[K, Change[VR]]](kvsImpl.addChild)(aggNode)

	currying := curryingAddChild[KeyValue[K, V], KeyValue[K, Change[VR]], KeyValue[K, Change[VR]]](aggNode)
	return &gtable[K, VR]{
		builder:  kvsImpl.builder,
		rid:      kvsImpl.rid,
		kvstore:  kvstore,
		addChild: currying,
	}
}

func Count[K, V any](kvs KeyValueGStream[K, V], mater materialized.Materialized[K, int]) GTable[K, int] {
	cntInit := func() int { return 0 }
	cntAgg := func(_ KeyValue[K, V], cnt int) int { return cnt + 1 }

	return Aggregate(kvs, cntInit, cntAgg, mater)
}
