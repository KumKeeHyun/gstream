package gstream

import (
	"github.com/KumKeeHyun/gstream/materialized"
	"github.com/KumKeeHyun/gstream/state"
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
	builder   *builder
	routineID GStreamID
	addChild  func(*processorNode[T, T])
}

var _ GStream[any] = &gstream[any]{}

func (s *gstream[T]) Filter(filter func(T) bool) GStream[T] {
	filterNode := newProcessorNode[T, T](newFilterProcessorSupplier(filter))
	s.addChild(filterNode)

	return &gstream[T]{
		builder:   s.builder,
		routineID: s.routineID,
		addChild:  curryingAddChild[T, T, T](filterNode),
	}
}

func (s *gstream[T]) Foreach(foreacher func(T)) {
	foreachNode := newProcessorNode[T, T](newForeachProcessorSupplier(foreacher))
	s.addChild(foreachNode)
}

func (s *gstream[T]) Map(mapper func(T) T) GStream[T] {
	mapNode := newProcessorNode[T, T](newMapProcessorSupplier(mapper))
	s.addChild(mapNode)

	return &gstream[T]{
		builder:   s.builder,
		routineID: s.routineID,
		addChild:  curryingAddChild[T, T, T](mapNode),
	}
}

func (s *gstream[T]) FlatMap(flatMapper func(T) []T) GStream[T] {
	flatMapNode := newProcessorNode[T, T](newFlatMapProcessorSupplier(flatMapper))
	s.addChild(flatMapNode)

	return &gstream[T]{
		builder:   s.builder,
		routineID: s.routineID,
		addChild:  curryingAddChild[T, T, T](flatMapNode),
	}
}

func (s *gstream[T]) Merge(ms GStream[T]) GStream[T] {
	msImpl := ms.(*gstream[T])

	// create new routine
	if s.routineID != msImpl.routineID {
		input := make(chan T)
		newSourceNode := newSourceNode(s.builder.getRoutineID(), s.builder.streamCtx, input)
		s.to(input, newSourceNode)
		msImpl.to(input, newSourceNode)

		return &gstream[T]{
			builder:   s.builder,
			routineID: newSourceNode.RoutineId(),
			addChild:  curryingAddChild[T, T, T](newSourceNode),
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
	newSourceNode := newSourceNode(s.builder.getRoutineID(), s.builder.streamCtx, pipe)
	s.to(pipe, newSourceNode)

	return &gstream[T]{
		builder:   s.builder,
		routineID: newSourceNode.RoutineId(),
		addChild:  curryingAddChild[T, T, T](newSourceNode),
	}
}

func (s *gstream[T]) To() chan T {
	output := make(chan T)
	sinkNode := newProcessorNode[T, T](newSinkProcessorSupplier(output, time.Millisecond))
	s.addChild(sinkNode)

	s.builder.streamCtx.addChanCloser(s.routineID,
		s.builder.getSinkID(s.routineID),
		safeChanCloser(output))

	return output
}

func (s *gstream[T]) ToWithBlocking() chan T {
	output := make(chan T)
	sinkNode := newProcessorNode[T, T](newBlockingSinkProcessorSupplier(output))
	s.addChild(sinkNode)

	s.builder.streamCtx.addChanCloser(s.routineID,
		s.builder.getSinkID(s.routineID),
		safeChanCloser(output))

	return output
}

func (s *gstream[T]) to(output chan T, sourceNode *processorNode[T, T]) {
	sinkNode := newProcessorNode[T, T](newBlockingSinkProcessorSupplier(output))
	s.addChild(sinkNode)
	addChild(sinkNode, sourceNode)

	s.builder.streamCtx.addChanCloser(s.routineID,
		sourceNode.RoutineId(),
		safeChanCloser(output))
}

// -------------------------------

func Map[T, TR any](s GStream[T], mapper func(T) TR) GStream[TR] {
	sImpl := s.(*gstream[T])
	mapNode := newProcessorNode[T, TR](newMapProcessorSupplier(mapper))
	castAddChild[T, TR](sImpl.addChild)(mapNode)

	return &gstream[TR]{
		builder:   sImpl.builder,
		routineID: sImpl.routineID,
		addChild:  curryingAddChild[T, TR, TR](mapNode),
	}
}

func FlatMap[T, TR any](s GStream[T], flatMapper func(T) []TR) GStream[TR] {
	sImpl := s.(*gstream[T])
	flatMapNode := newProcessorNode[T, TR](newFlatMapProcessorSupplier(flatMapper))
	castAddChild[T, TR](sImpl.addChild)(flatMapNode)

	return &gstream[TR]{
		builder:   sImpl.builder,
		routineID: sImpl.routineID,
		addChild:  curryingAddChild[T, TR, TR](flatMapNode),
	}
}

func SelectKey[K, V any](s GStream[V], keySelecter func(V) K) KeyValueGStream[K, V] {
	kvs := Map(s, func(v V) KeyValue[K, V] {
		return NewKeyValue(keySelecter(v), v)
	}).(*gstream[KeyValue[K, V]])

	return &keyValueGStream[K, V]{
		builder:   kvs.builder,
		routineID: kvs.routineID,
		addChild:  kvs.addChild,
	}
}

type keyValueGStream[K, V any] struct {
	builder   *builder
	routineID GStreamID
	addChild  func(*processorNode[KeyValue[K, V], KeyValue[K, V]])
}

var _ KeyValueGStream[any, any] = &keyValueGStream[any, any]{}

func (kvs *keyValueGStream[K, V]) gstream() *gstream[KeyValue[K, V]] {
	return &gstream[KeyValue[K, V]]{
		builder:   kvs.builder,
		routineID: kvs.routineID,
		addChild:  kvs.addChild,
	}
}

func (kvs *keyValueGStream[K, V]) Filter(filter func(K, V) bool) KeyValueGStream[K, V] {
	s := kvs.gstream().Filter(func(kv KeyValue[K, V]) bool {
		return filter(kv.Key, kv.Value)
	}).(*gstream[KeyValue[K, V]])

	return &keyValueGStream[K, V]{
		builder:   s.builder,
		routineID: s.routineID,
		addChild:  s.addChild,
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
		builder:   s.builder,
		routineID: s.routineID,
		addChild:  s.addChild,
	}
}

func (kvs *keyValueGStream[K, V]) MapValues(mapper func(V) V) KeyValueGStream[K, V] {
	s := kvs.gstream().Map(func(kv KeyValue[K, V]) KeyValue[K, V] {
		return NewKeyValue(kv.Key, mapper(kv.Value))
	}).(*gstream[KeyValue[K, V]])

	return &keyValueGStream[K, V]{
		builder:   s.builder,
		routineID: s.routineID,
		addChild:  s.addChild,
	}
}

func (kvs *keyValueGStream[K, V]) FlatMap(flatMapper func(K, V) []KeyValue[K, V]) KeyValueGStream[K, V] {
	s := kvs.gstream().FlatMap(func(kv KeyValue[K, V]) []KeyValue[K, V] {
		return flatMapper(kv.Key, kv.Value)
	}).(*gstream[KeyValue[K, V]])

	return &keyValueGStream[K, V]{
		builder:   s.builder,
		routineID: s.routineID,
		addChild:  s.addChild,
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
		builder:   s.builder,
		routineID: s.routineID,
		addChild:  s.addChild,
	}
}

func (kvs *keyValueGStream[K, V]) Merge(mkvs KeyValueGStream[K, V]) KeyValueGStream[K, V] {
	mkvsImpl := mkvs.(*keyValueGStream[K, V]).gstream()
	ms := kvs.gstream().Merge(mkvsImpl).(*gstream[KeyValue[K, V]])

	return &keyValueGStream[K, V]{
		builder:   ms.builder,
		routineID: ms.routineID,
		addChild:  ms.addChild,
	}
}

func (kvs *keyValueGStream[K, V]) Pipe() KeyValueGStream[K, V] {
	s := kvs.gstream().Pipe().(*gstream[KeyValue[K, V]])

	return &keyValueGStream[K, V]{
		builder:   s.builder,
		routineID: s.routineID,
		addChild:  s.addChild,
	}
}

func (kvs *keyValueGStream[K, V]) ToValueStream() GStream[V] {
	return Map[KeyValue[K, V], V](kvs.gstream(), func(kv KeyValue[K, V]) V {
		return kv.Value
	})
}

func (kvs *keyValueGStream[K, V]) ToTable(mater materialized.Materialized[K, V]) GTable[K, V] {
	kvstore := state.NewKeyValueStore(mater)
	if closer, ok := kvstore.(state.StoreCloser); ok {
		kvs.builder.streamCtx.addStore(kvs.routineID, closer)
	}

	streamToTableSupplier := newStreamToTableProcessorSupplier(kvstore)
	streamToTableNode := newStreamToTableNode(streamToTableSupplier)
	castAddChild[KeyValue[K, V], KeyValue[K, Change[V]]](kvs.addChild)(streamToTableNode)

	currying := curryingAddChild[KeyValue[K, V], KeyValue[K, Change[V]], KeyValue[K, Change[V]]](streamToTableNode)
	return &gtable[K, V]{
		builder:   kvs.builder,
		routineID: kvs.routineID,
		addChild:  currying,
		kvstore:   streamToTableSupplier.kvstore,
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
		builder:   mkvs.builder,
		routineID: mkvs.routineID,
		addChild:  mkvs.addChild,
	}
}

func KeyValueMapValues[K, V, VR any](kvs KeyValueGStream[K, V], mapper func(V) VR) KeyValueGStream[K, VR] {
	kvsImpl := kvs.(*keyValueGStream[K, V]).gstream()
	mkvs := Map[KeyValue[K, V], KeyValue[K, VR]](kvsImpl, func(kv KeyValue[K, V]) KeyValue[K, VR] {
		vr := mapper(kv.Value)
		return NewKeyValue(kv.Key, vr)
	}).(*gstream[KeyValue[K, VR]])

	return &keyValueGStream[K, VR]{
		builder:   mkvs.builder,
		routineID: mkvs.routineID,
		addChild:  mkvs.addChild,
	}
}

// -------------------------------

func Joined[K, V, VO, VR any](kvs KeyValueGStream[K, V]) JoinedGStream[K, V, VO, VR] {
	kvsImpl := kvs.(*keyValueGStream[K, V])
	return &joinedGStream[K, V, VO, VR]{
		builder:   kvsImpl.builder,
		routineID: kvsImpl.routineID,
		addChild:  castAddChild[KeyValue[K, V], KeyValue[K, VR]](kvsImpl.addChild),
	}
}

type joinedGStream[K, V, VO, VR any] struct {
	builder   *builder
	routineID GStreamID
	addChild  func(*processorNode[KeyValue[K, V], KeyValue[K, VR]])
}

var _ JoinedGStream[any, any, any, any] = &joinedGStream[any, any, any, any]{}

func (js *joinedGStream[K, V, VO, VR]) JoinTable(t GTable[K, VO], joiner func(V, VO) VR) KeyValueGStream[K, VR] {
	valueGetter := t.(*gtable[K, VO]).valueGetter()
	joinProcessorSupplier := newStreamTableJoinProcessorSupplier(valueGetter, joiner)
	joinNode := newProcessorNode[KeyValue[K, V], KeyValue[K, VR]](joinProcessorSupplier)
	js.addChild(joinNode)

	currying := curryingAddChild[KeyValue[K, V], KeyValue[K, VR], KeyValue[K, VR]](joinNode)
	return &keyValueGStream[K, VR]{
		builder:   js.builder,
		routineID: js.routineID,
		addChild:  currying,
	}
}

// -------------------------------

func Aggregate[K, V, VR any](kvs KeyValueGStream[K, V], initializer func() VR, aggregator func(KeyValue[K, V], VR) VR, mater materialized.Materialized[K, VR]) GTable[K, VR] {
	kvsImpl := kvs.(*keyValueGStream[K, V])

	kvstore := state.NewKeyValueStore(mater)
	if closer, ok := kvstore.(state.StoreCloser); ok {
		kvsImpl.builder.streamCtx.addStore(kvsImpl.routineID, closer)
	}

	aggProcessorSupplier := newStreamAggregateProcessorSupplier(initializer, aggregator, kvstore)
	aggNode := newProcessorNode[KeyValue[K, V], KeyValue[K, Change[VR]]](aggProcessorSupplier)
	castAddChild[KeyValue[K, V], KeyValue[K, Change[VR]]](kvsImpl.addChild)(aggNode)

	currying := curryingAddChild[KeyValue[K, V], KeyValue[K, Change[VR]], KeyValue[K, Change[VR]]](aggNode)
	return &gtable[K, VR]{
		builder:   kvsImpl.builder,
		routineID: kvsImpl.routineID,
		kvstore:   kvstore,
		addChild:  currying,
	}
}

func Count[K, V any](kvs KeyValueGStream[K, V], mater materialized.Materialized[K, int]) GTable[K, int] {
	cntInit := func() int { return 0 }
	cntAgg := func(_ KeyValue[K, V], cnt int) int { return cnt + 1 }

	return Aggregate(kvs, cntInit, cntAgg, mater)
}
