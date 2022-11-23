package gstream

import (
	"time"
)

type GStream[T any] interface {
	Filter(func(T) bool) GStream[T]
	Foreach(func(T))
	Map(func(T) T) GStream[T]
	FlatMap(func(T) []T) GStream[T]
	Merge(GStream[T]) GStream[T]
	Pipe() GStream[T]
	Reduce(func() T, func(T, T) T) chan T
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
	ToTable(m Materialized[K, V]) GTable[K, V]
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

func (s *gstream[T]) Reduce(init func() T, accumulator func(T, T) T) chan T {
	output := make(chan T)
	reduceNode := newProcessorNode[T, T](newReduceProcessorSupplier(output, init, accumulator))
	s.addChild(reduceNode)

	s.builder.streamCtx.addCloseChan(s.routineID,
		s.builder.getSinkID(s.routineID),
		func() { close(output) })
	return output
}

func (s *gstream[T]) To() chan T {
	output := make(chan T)
	sinkNode := newProcessorNode[T, T](newSinkProcessorSupplier(output, time.Millisecond))
	s.addChild(sinkNode)

	s.builder.streamCtx.addCloseChan(s.routineID,
		s.builder.getSinkID(s.routineID),
		func() { close(output) })

	return output
}

func (s *gstream[T]) ToWithBlocking() chan T {
	output := make(chan T)
	sinkNode := newProcessorNode[T, T](newBlockingSinkProcessorSupplier(output))
	s.addChild(sinkNode)

	s.builder.streamCtx.addCloseChan(s.routineID,
		s.builder.getSinkID(s.routineID),
		func() { close(output) })

	return output
}

func (s *gstream[T]) to(output chan T, sourceNode *processorNode[T, T]) {
	sinkNode := newProcessorNode[T, T](newBlockingSinkProcessorSupplier(output))
	s.addChild(sinkNode)
	addChild(sinkNode, sourceNode)

	s.builder.streamCtx.addCloseChan(s.routineID,
		sourceNode.RoutineId(),
		func() { close(output) })
}

// -------------------------------

func Map[T, TR any](s GStream[T], mapper func(T) TR) GStream[TR] {
	passNode := newFallThroughProcessorNode[T]()
	sImpl := s.(*gstream[T])
	sImpl.addChild(passNode)

	mapNode := newProcessorNode[T, TR](newMapProcessorSupplier(mapper))
	addChild(passNode, mapNode)
	return &gstream[TR]{
		builder:   sImpl.builder,
		routineID: sImpl.routineID,
		addChild:  curryingAddChild[T, TR, TR](mapNode),
	}
}

func FlatMap[T, TR any](s GStream[T], flatMapper func(T) []TR) GStream[TR] {
	passNode := newFallThroughProcessorNode[T]()
	sImpl := s.(*gstream[T])
	sImpl.addChild(passNode)

	flatMapNode := newProcessorNode[T, TR](newFlatMapProcessorSupplier(flatMapper))
	addChild(passNode, flatMapNode)
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
	s := kvs.gstream().
		Map(func(kv KeyValue[K, V]) KeyValue[K, V] {
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
	s := kvs.gstream().
		FlatMap(func(kv KeyValue[K, V]) []KeyValue[K, V] {
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

func (kvs *keyValueGStream[K, V]) ToTable(m Materialized[K, V]) GTable[K, V] {
	passNode := newFallThroughProcessorNode[KeyValue[K, V]]()
	kvs.addChild(passNode)

	kvstore := newKeyValueStore(m)
	streamToTableSupplier := newStreamToTableProcessorSupplier(kvstore)
	streamToTableNode := newStreamToTableNode(streamToTableSupplier)
	addChild(passNode, streamToTableNode)

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
	passNode := newFallThroughProcessorNode[KeyValue[K, V]]()
	kvsImpl := kvs.(*keyValueGStream[K, V])
	kvsImpl.addChild(passNode)

	currying := curryingAddChild[KeyValue[K, V], KeyValue[K, V], KeyValue[K, VR]](passNode)
	return &joinedGStream[K, V, VO, VR]{
		builder:   kvsImpl.builder,
		routineID: kvsImpl.routineID,
		addChild:  currying,
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
	joinNode := newProcessorNode[KeyValue[K, V], KeyValue[K, VR]](newStreamTableJoinProcessorSupplier(valueGetter, joiner))
	js.addChild(joinNode)

	currying := curryingAddChild[KeyValue[K, V], KeyValue[K, VR], KeyValue[K, VR]](joinNode)
	return &keyValueGStream[K, VR]{
		builder:   js.builder,
		routineID: js.routineID,
		addChild:  currying,
	}
}

// -------------------------------

func Aggreate[K, V, VR any](kvs KeyValueGStream[K, V],
	initializer func() VR,
	aggreator func(KeyValue[K, V], VR) VR,
	materialized Materialized[K, VR]) GTable[K, VR] {

	passNode := newFallThroughProcessorNode[KeyValue[K, V]]()
	kvsImpl := kvs.(*keyValueGStream[K, V])
	kvsImpl.addChild(passNode)

	kvstore := newKeyValueStore(materialized)
	aggregateNode := newProcessorNode[KeyValue[K, V], KeyValue[K, Change[VR]]](newStreamAggreateProcessorSupplier(initializer, aggreator, kvstore))
	addChild(passNode, aggregateNode)

	curring := curryingAddChild[KeyValue[K, V], KeyValue[K, Change[VR]], KeyValue[K, Change[VR]]](aggregateNode)
	return &gtable[K, VR]{
		builder:   kvsImpl.builder,
		routineID: kvsImpl.routineID,
		kvstore:   kvstore,
		addChild:  curring,
	}
}
