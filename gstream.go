package gstream

import (
	"github.com/KumKeeHyun/gstream/state"
	"github.com/KumKeeHyun/gstream/state/materialized"
	"time"
)

type GStream[T any] interface {
	Filter(func(T) bool) GStream[T]
	Foreach(func(T))
	Map(func(T) T) GStream[T]
	FlatMap(func(T) []T) GStream[T]
	Merge(GStream[T]) GStream[T]
	Pipe() GStream[T]
	To() chan T
	ToWithBlocking() chan T
}

type KeyValueGStream[K, V any] interface {
	Filter(func(K, V) bool) KeyValueGStream[K, V]
	Foreach(func(K, V))
	Map(func(K, V) (K, V)) KeyValueGStream[K, V]
	MapValues(func(V) V) KeyValueGStream[K, V]
	FlatMap(func(K, V) []KeyValue[K, V]) KeyValueGStream[K, V]
	FlatMapValues(func(V) []V) KeyValueGStream[K, V]
	Merge(KeyValueGStream[K, V]) KeyValueGStream[K, V]
	Pipe() KeyValueGStream[K, V]

	ToValueStream() GStream[V]
	ToTable(materialized.Materialized[K, V]) GTable[K, V]
}

type JoinedGStream[K, V, VO, VR any] interface {
	JoinTable(GTable[K, VO], func(V, VO) VR) KeyValueGStream[K, VR]
}

// -------------------------------

type gstream[T any] struct {
	builder  *builder
	rid      GStreamID
	addChild func(*processorNode[T, T])
}

var _ GStream[any] = &gstream[any]{}

func (s *gstream[T]) Filter(filter func(T) bool) GStream[T] {
	filterSupplier := newFilterProcessorSupplier(filter)
	filterNode := newProcessorNode[T, T](filterSupplier)
	s.addChild(filterNode)

	return &gstream[T]{
		builder:  s.builder,
		rid:      s.rid,
		addChild: curryingAddChild[T, T, T](filterNode),
	}
}

func (s *gstream[T]) Foreach(foreacher func(T)) {
	foreachSupplier := newForeachProcessorSupplier(foreacher)
	foreachNode := newProcessorNode[T, T](foreachSupplier)
	s.addChild(foreachNode)
}

func (s *gstream[T]) Map(mapper func(T) T) GStream[T] {
	mapSupplier := newMapProcessorSupplier(mapper)
	mapNode := newProcessorNode[T, T](mapSupplier)
	s.addChild(mapNode)

	return &gstream[T]{
		builder:  s.builder,
		rid:      s.rid,
		addChild: curryingAddChild[T, T, T](mapNode),
	}
}

func (s *gstream[T]) FlatMap(flatMapper func(T) []T) GStream[T] {
	flatMapNode := newProcessorNode[T, T](newFlatMapProcessorSupplier(flatMapper))
	s.addChild(flatMapNode)

	return &gstream[T]{
		builder:  s.builder,
		rid:      s.rid,
		addChild: curryingAddChild[T, T, T](flatMapNode),
	}
}

func (s *gstream[T]) Merge(ms GStream[T]) GStream[T] {
	msImpl := ms.(*gstream[T])

	// create new routine
	if s.rid != msImpl.rid {
		input := make(chan T)
		newSrcNode := newSourceNode(s.builder.getRoutineID(), s.builder.sctx, input, 1)
		s.to(input, newSrcNode)
		msImpl.to(input, newSrcNode)

		return &gstream[T]{
			builder:  s.builder,
			rid:      newSrcNode.RoutineId(),
			addChild: curryingAddChild[T, T, T](newSrcNode),
		}
	}

	passNode := newFallThroughProcessorNode[T]()
	s.addChild(passNode)
	msImpl.addChild(passNode)

	return &gstream[T]{
		builder:  s.builder,
		addChild: curryingAddChild[T, T, T](passNode),
	}
}

func (s *gstream[T]) Pipe() GStream[T] {
	pipe := make(chan T)
	newSrcNode := newSourceNode(s.builder.getRoutineID(), s.builder.sctx, pipe, 1)
	s.to(pipe, newSrcNode)

	return &gstream[T]{
		builder:  s.builder,
		rid:      newSrcNode.RoutineId(),
		addChild: curryingAddChild[T, T, T](newSrcNode),
	}
}

func (s *gstream[T]) To() chan T {
	sinkPipe := make(chan T)
	sinkNode := newProcessorNode[T, T](newSinkProcessorSupplier(sinkPipe, time.Millisecond))
	s.addChild(sinkNode)

	s.builder.sctx.add(newPipeCloser(sinkPipe))

	return sinkPipe
}

func (s *gstream[T]) ToWithBlocking() chan T {
	sinkPipe := make(chan T)
	sinkNode := newProcessorNode[T, T](newBlockingSinkProcessorSupplier(sinkPipe))
	s.addChild(sinkNode)

	s.builder.sctx.add(newPipeCloser(sinkPipe))

	return sinkPipe
}

func (s *gstream[T]) to(pipe chan T, sourceNode *processorNode[T, T]) {
	sinkNode := newProcessorNode[T, T](newBlockingSinkProcessorSupplier(pipe))
	s.addChild(sinkNode)
	addChild(sinkNode, sourceNode)

	s.builder.sctx.add(newPipeCloser(pipe))
}

// -------------------------------

func Map[T, TR any](s GStream[T], mapper func(T) TR) GStream[TR] {
	sImpl := s.(*gstream[T])
	mapSupplier := newMapProcessorSupplier(mapper)
	mapNode := newProcessorNode[T, TR](mapSupplier)
	castAddChild[T, TR](sImpl.addChild)(mapNode)

	return &gstream[TR]{
		builder:  sImpl.builder,
		rid:      sImpl.rid,
		addChild: curryingAddChild[T, TR, TR](mapNode),
	}
}

func FlatMap[T, TR any](s GStream[T], flatMapper func(T) []TR) GStream[TR] {
	sImpl := s.(*gstream[T])
	flatMapNode := newProcessorNode[T, TR](newFlatMapProcessorSupplier(flatMapper))
	castAddChild[T, TR](sImpl.addChild)(flatMapNode)

	return &gstream[TR]{
		builder:  sImpl.builder,
		rid:      sImpl.rid,
		addChild: curryingAddChild[T, TR, TR](flatMapNode),
	}
}

func SelectKey[K, V any](s GStream[V], keySelecter func(V) K) KeyValueGStream[K, V] {
	kvs := Map(s, func(v V) KeyValue[K, V] {
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
	rid      GStreamID
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

func (kvs *keyValueGStream[K, V]) Foreach(foreacher func(K, V)) {
	kvs.gstream().Foreach(func(kv KeyValue[K, V]) {
		foreacher(kv.Key, kv.Value)
	})
}

func (kvs *keyValueGStream[K, V]) Map(mapper func(K, V) (K, V)) KeyValueGStream[K, V] {
	s := kvs.gstream().Map(func(kv KeyValue[K, V]) KeyValue[K, V] {
		kr, vr := mapper(kv.Key, kv.Value)
		return NewKeyValue(kr, vr)
	}).(*gstream[KeyValue[K, V]])

	return &keyValueGStream[K, V]{
		builder:  s.builder,
		rid:      s.rid,
		addChild: s.addChild,
	}
}

func (kvs *keyValueGStream[K, V]) MapValues(mapper func(V) V) KeyValueGStream[K, V] {
	s := kvs.gstream().Map(func(kv KeyValue[K, V]) KeyValue[K, V] {
		return NewKeyValue(kv.Key, mapper(kv.Value))
	}).(*gstream[KeyValue[K, V]])

	return &keyValueGStream[K, V]{
		builder:  s.builder,
		rid:      s.rid,
		addChild: s.addChild,
	}
}

func (kvs *keyValueGStream[K, V]) FlatMap(flatMapper func(K, V) []KeyValue[K, V]) KeyValueGStream[K, V] {
	s := kvs.gstream().FlatMap(func(kv KeyValue[K, V]) []KeyValue[K, V] {
		return flatMapper(kv.Key, kv.Value)
	}).(*gstream[KeyValue[K, V]])

	return &keyValueGStream[K, V]{
		builder:  s.builder,
		rid:      s.rid,
		addChild: s.addChild,
	}
}

func (kvs *keyValueGStream[K, V]) FlatMapValues(flatMapper func(V) []V) KeyValueGStream[K, V] {
	s := kvs.gstream().FlatMap(func(kv KeyValue[K, V]) []KeyValue[K, V] {
		mvs := flatMapper(kv.Value)
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

func (kvs *keyValueGStream[K, V]) Merge(mkvs KeyValueGStream[K, V]) KeyValueGStream[K, V] {
	mkvsImpl := mkvs.(*keyValueGStream[K, V]).gstream()
	ms := kvs.gstream().Merge(mkvsImpl).(*gstream[KeyValue[K, V]])

	return &keyValueGStream[K, V]{
		builder:  ms.builder,
		rid:      ms.rid,
		addChild: ms.addChild,
	}
}

func (kvs *keyValueGStream[K, V]) Pipe() KeyValueGStream[K, V] {
	s := kvs.gstream().Pipe().(*gstream[KeyValue[K, V]])

	return &keyValueGStream[K, V]{
		builder:  s.builder,
		rid:      s.rid,
		addChild: s.addChild,
	}
}

func (kvs *keyValueGStream[K, V]) ToValueStream() GStream[V] {
	return Map[KeyValue[K, V], V](kvs.gstream(), func(kv KeyValue[K, V]) V {
		return kv.Value
	})
}

func (kvs *keyValueGStream[K, V]) ToTable(mater materialized.Materialized[K, V]) GTable[K, V] {
	kvstore := state.NewKeyValueStore(mater)
	if closer, ok := kvstore.(Closer); ok {
		kvs.builder.sctx.add(closer)
	}

	streamToTableSupplier := newStreamToTableProcessorSupplier(kvstore)
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

func KeyValueMap[K, V, KR, VR any](kvs KeyValueGStream[K, V], mapper func(K, V) (KR, VR)) KeyValueGStream[KR, VR] {
	kvsImpl := kvs.(*keyValueGStream[K, V]).gstream()
	mkvs := Map[KeyValue[K, V], KeyValue[KR, VR]](kvsImpl, func(kv KeyValue[K, V]) KeyValue[KR, VR] {
		kr, vr := mapper(kv.Key, kv.Value)
		return NewKeyValue(kr, vr)
	}).(*gstream[KeyValue[KR, VR]])

	return &keyValueGStream[KR, VR]{
		builder:  mkvs.builder,
		rid:      mkvs.rid,
		addChild: mkvs.addChild,
	}
}

func KeyValueMapValues[K, V, VR any](kvs KeyValueGStream[K, V], mapper func(V) VR) KeyValueGStream[K, VR] {
	kvsImpl := kvs.(*keyValueGStream[K, V]).gstream()
	mkvs := Map[KeyValue[K, V], KeyValue[K, VR]](kvsImpl, func(kv KeyValue[K, V]) KeyValue[K, VR] {
		vr := mapper(kv.Value)
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
	rid      GStreamID
	addChild func(*processorNode[KeyValue[K, V], KeyValue[K, VR]])
}

var _ JoinedGStream[any, any, any, any] = &joinedGStream[any, any, any, any]{}

func (js *joinedGStream[K, V, VO, VR]) JoinTable(t GTable[K, VO], joiner func(V, VO) VR) KeyValueGStream[K, VR] {
	valueGetter := t.(*gtable[K, VO]).valueGetter()
	joinProcessorSupplier := newStreamTableJoinProcessorSupplier(valueGetter, joiner)
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

	aggProcessorSupplier := newStreamAggregateProcessorSupplier(initializer, aggregator, kvstore)
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
