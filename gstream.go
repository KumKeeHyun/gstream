package gstream

import (
	"time"
)

type GStream[T any] interface {
	Filter(func(T) bool) GStream[T]
	Foreach(func(T))
	Map(func(T) T) GStream[T]
	Merge(GStream[T]) GStream[T]
	Reduce(func() T, func(T, T) T) chan T
	To() chan T
	ToWithBlocking() chan T
}

type MappedGStream[T, TR any] interface {
	Map(func(T) TR) GStream[TR]
}

type KeyValueGStream[K, V any] interface {
	ToStream() GStream[V]
	ToTable(Serde[K]) GTable[K, V]
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

func Mapped[T, TR any](s GStream[T]) MappedGStream[T, TR] {
	passNode := newFallThroughProcessorNode[T]()
	sImpl := s.(*gstream[T])
	sImpl.addChild(passNode)

	return &mappedGStream[T, TR]{
		builder:   sImpl.builder,
		routineID: sImpl.routineID,
		addChild:  curryingAddChild[T, T, TR](passNode),
	}
}

type mappedGStream[T, TR any] struct {
	builder   *builder
	routineID GStreamID
	addChild  func(*processorNode[T, TR])
}

var _ MappedGStream[any, any] = &mappedGStream[any, any]{}

func (ms *mappedGStream[T, TR]) Map(mapper func(T) TR) GStream[TR] {
	mapNode := newProcessorNode[T, TR](newMapProcessorSupplier(mapper))
	ms.addChild(mapNode)
	return &gstream[TR]{
		builder:   ms.builder,
		routineID: ms.routineID,
		addChild:  curryingAddChild[T, TR, TR](mapNode),
	}
}

// -------------------------------

func SelectKey[K, V any](s GStream[V], keySelecter func(V) K) KeyValueGStream[K, V] {
	kvs := Mapped[V, KeyValue[K, V]](s).Map(func(v V) KeyValue[K, V] {
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

func (kvs *keyValueGStream[K, V]) ToStream() GStream[V] {
	passNode := newFallThroughProcessorNode[KeyValue[K, V]]()
	kvs.addChild(passNode)

	mappedNode := newProcessorNode[KeyValue[K, V], V](newMapProcessorSupplier(func(kv KeyValue[K, V]) V {
		return kv.Value
	}))
	addChild(passNode, mappedNode)

	return &gstream[V]{
		builder:   kvs.builder,
		routineID: kvs.routineID,
		addChild:  curryingAddChild[KeyValue[K, V], V, V](mappedNode),
	}
}

func (kvs *keyValueGStream[K, V]) ToTable(keySerde Serde[K]) GTable[K, V] {
	passNode := newFallThroughProcessorNode[KeyValue[K, V]]()
	kvs.addChild(passNode)

	// TODO: 다른 형태의 kvstore 지원하도록 수정. ex: boltDB
	memKvstore := NewMemKeyValueStore[K, V](keySerde)

	streamToTableSupplier := newStreamToTableProcessorSupplier(memKvstore)
	streamToTableNode := newStreamToTableNode(streamToTableSupplier)
	addChild(passNode, streamToTableNode)

	return &gtable[K, V]{
		builder:   kvs.builder,
		routineID: kvs.routineID,
		kvstore:   streamToTableSupplier.kvstore,
		addChild:  curryingAddChild[KeyValue[K, V], KeyValue[K, Change[V]], KeyValue[K, Change[V]]](streamToTableNode),
	}
}

func Joined[K, V, VO, VR any](kvs KeyValueGStream[K, V]) JoinedGStream[K, V, VO, VR] {
	passNode := newFallThroughProcessorNode[KeyValue[K, V]]()
	kvsImpl := kvs.(*keyValueGStream[K, V])
	kvsImpl.addChild(passNode)

	return &joinedGStream[K, V, VO, VR]{
		builder:   kvsImpl.builder,
		routineID: kvsImpl.routineID,
		addChild:  curryingAddChild[KeyValue[K, V], KeyValue[K, V], KeyValue[K, VR]](passNode),
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

	return &keyValueGStream[K, VR]{
		builder:   js.builder,
		routineID: js.routineID,
		addChild:  curryingAddChild[KeyValue[K, V], KeyValue[K, VR], KeyValue[K, VR]](joinNode),
	}
}

func mappedKeyValue[K, V, VR any](kvs *keyValueGStream[K, V]) *mappedKeyValueGStream[K, V, VR] {
	mappedNode := newFallThroughProcessorNode[KeyValue[K, V]]()
	kvs.addChild(mappedNode)

	return &mappedKeyValueGStream[K, V, VR]{
		builder:   kvs.builder,
		routineID: kvs.routineID,
		addChild:  curryingAddChild[KeyValue[K, V], KeyValue[K, V], KeyValue[K, VR]](mappedNode),
	}
}

type mappedKeyValueGStream[K, V, VR any] struct {
	builder   *builder
	routineID GStreamID
	addChild  func(*processorNode[KeyValue[K, V], KeyValue[K, VR]])
}

func (mkvs *mappedKeyValueGStream[K, V, VR]) Map(mapper func(V) VR) KeyValueGStream[K, VR] {
	mkvNode := newProcessorNode[KeyValue[K, V], KeyValue[K, VR]](
		newMapProcessorSupplier(func(kv KeyValue[K, V]) KeyValue[K, VR] {
			return NewKeyValue(kv.Key, mapper(kv.Value))
		}))
	mkvs.addChild(mkvNode)

	return &keyValueGStream[K, VR]{
		builder:   mkvs.builder,
		routineID: mkvs.routineID,
		addChild:  curryingAddChild[KeyValue[K, V], KeyValue[K, VR], KeyValue[K, VR]](mkvNode),
	}
}
