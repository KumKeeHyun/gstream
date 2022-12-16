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
	MapErr(func(context.Context, T) (T, error)) (GStream[T], FailedGStream[T])
	FlatMap(func(context.Context, T) []T) GStream[T]
	FlatMapErr(func(context.Context, T) ([]T, error)) (GStream[T], FailedGStream[T])
	Merge(GStream[T], ...pipe.Option) GStream[T]
	Pipe(...pipe.Option) GStream[T]
	To(...sink.Option) <-chan T
}

type FailedGStream[T any] interface {
	Filter(func(T, error) bool) FailedGStream[T]
	Foreach(func(context.Context, T, error))
	ToStream() GStream[T]
}

type KeyValueGStream[K, V any] interface {
	Filter(func(KeyValue[K, V]) bool) KeyValueGStream[K, V]
	Foreach(func(context.Context, KeyValue[K, V]))
	Map(func(context.Context, KeyValue[K, V]) KeyValue[K, V]) KeyValueGStream[K, V]
	MapErr(func(context.Context, KeyValue[K, V]) (KeyValue[K, V], error)) (KeyValueGStream[K, V], FailedKeyValueGStream[K, V])
	MapValues(func(context.Context, V) V) KeyValueGStream[K, V]
	MapValuesErr(func(context.Context, V) (V, error)) (KeyValueGStream[K, V], FailedKeyValueGStream[K, V])
	FlatMap(func(context.Context, KeyValue[K, V]) []KeyValue[K, V]) KeyValueGStream[K, V]
	FlatMapErr(func(context.Context, KeyValue[K, V]) ([]KeyValue[K, V], error)) (KeyValueGStream[K, V], FailedKeyValueGStream[K, V])
	FlatMapValues(func(context.Context, V) []V) KeyValueGStream[K, V]
	FlatMapValuesErr(func(context.Context, V) ([]V, error)) (KeyValueGStream[K, V], FailedKeyValueGStream[K, V])
	Merge(KeyValueGStream[K, V], ...pipe.Option) KeyValueGStream[K, V]
	Pipe(...pipe.Option) KeyValueGStream[K, V]
	To(...sink.Option) <-chan KeyValue[K, V]

	ToValueStream() GStream[V]
	ToTable(materialized.Materialized[K, V]) GTable[K, V]
}

type FailedKeyValueGStream[K, V any] interface {
	Filter(func(KeyValue[K, V], error) bool) FailedKeyValueGStream[K, V]
	Foreach(func(context.Context, KeyValue[K, V], error))
	ToStream() KeyValueGStream[K, V]
}

type JoinedGStream[K, V, VO, VR any] interface {
	JoinTable(GTable[K, VO], func(K, V, VO) VR) KeyValueGStream[K, VR]
	JoinTableErr(GTable[K, VO], func(K, V, VO) (VR, error)) (KeyValueGStream[K, VR], FailedKeyValueGStream[K, V])
}

// -------------------------------

type gstream[T any] struct {
	builder  *builder
	rid      routineID
	addChild func(*graphNode[T, T])
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
	return Map[T, T](s, mapper)
}

func (s *gstream[T]) MapErr(mapper func(context.Context, T) (T, error)) (GStream[T], FailedGStream[T]) {
	return MapErr[T, T](s, mapper)
}

func (s *gstream[T]) FlatMap(flatMapper func(context.Context, T) []T) GStream[T] {
	return FlatMap[T, T](s, flatMapper)
}

func (s *gstream[T]) FlatMapErr(flatMapper func(context.Context, T) ([]T, error)) (GStream[T], FailedGStream[T]) {
	return FlatMapErr[T, T](s, flatMapper)
}

func (s *gstream[T]) Merge(ms GStream[T], opts ...pipe.Option) GStream[T] {
	msImpl := ms.(*gstream[T])

	// create new routine if streams are in different routine.
	if s.rid != msImpl.rid {
		opt := newPipeOption[T](opts...)
		p := opt.BuildPipe()
		pc := newPipeCloser(p)

		sinkNode := newProcessorNode[T, T](newSinkSupplier(p, 0))
		s.addChild(sinkNode)
		s.builder.sctx.addPipe(s.rid, pc)
		msImpl.addChild(sinkNode)
		s.builder.sctx.addPipe(msImpl.rid, pc)

		srcNode := newSourceNode(s.builder.newRoutineID(), s.builder.sctx, p, opt.WorkerPool())
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
	s.builder.sctx.addPipe(s.rid, newPipeCloser(p))

	srcNode := newSourceNode(s.builder.newRoutineID(), s.builder.sctx, p, opt.WorkerPool())
	addChild(sinkNode, srcNode)

	return &gstream[T]{
		builder:  s.builder,
		rid:      srcNode.RoutineId(),
		addChild: curryingAddChild[T, T, T](srcNode),
	}
}

func (s *gstream[T]) To(opts ...sink.Option) <-chan T {
	opt := newSinkOption[T](opts...)
	p := opt.BuildPipe()
	s.builder.sctx.addPipe(s.rid, newPipeCloser(p))

	sinkNode := newProcessorNode[T, T](newSinkSupplier(p, opt.Timeout()))
	s.addChild(sinkNode)

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

func MapErr[T, TR any](s GStream[T], mapper func(context.Context, T) (TR, error)) (ss GStream[TR], fs FailedGStream[T]) {
	sImpl := s.(*gstream[T])
	resultNode := newProcessorNode[T, result[T, TR]](newMapSupplier[T, result[T, TR]](func(ctx context.Context, d T) result[T, TR] {
		r, err := mapper(ctx, d)
		return result[T, TR]{d, r, err}
	}))
	castAddChild[T, result[T, TR]](sImpl.addChild)(resultNode)

	successNode := newProcessorNode[result[T, TR], result[T, TR]](newFilterSupplier[result[T, TR]](func(r result[T, TR]) bool {
		return r.err == nil
	}))
	addChild(resultNode, successNode)
	mappedNode := newProcessorNode[result[T, TR], TR](newMapSupplier[result[T, TR], TR](func(ctx context.Context, r result[T, TR]) TR {
		return r.success()
	}))
	addChild(successNode, mappedNode)

	failNode := newProcessorNode[result[T, TR], result[T, TR]](newFilterSupplier[result[T, TR]](func(r result[T, TR]) bool {
		return r.err != nil
	}))
	addChild(resultNode, failNode)
	failedNode := newProcessorNode[result[T, TR], Fail[T]](newMapSupplier[result[T, TR], Fail[T]](func(ctx context.Context, r result[T, TR]) Fail[T] {
		return r.fail()
	}))
	addChild(failNode, failedNode)

	return &gstream[TR]{
			builder:  sImpl.builder,
			rid:      sImpl.rid,
			addChild: curryingAddChild[result[T, TR], TR, TR](mappedNode),
		}, &failedGStream[T]{
			builder:  sImpl.builder,
			rid:      sImpl.rid,
			addChild: curryingAddChild[result[T, TR], Fail[T], Fail[T]](failedNode),
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

func FlatMapErr[T, TR any](s GStream[T], flatMapper func(context.Context, T) ([]TR, error)) (ss GStream[TR], fs FailedGStream[T]) {
	sImpl := s.(*gstream[T])
	resultNode := newProcessorNode[T, result[T, []TR]](newMapSupplier[T, result[T, []TR]](func(ctx context.Context, d T) result[T, []TR] {
		r, err := flatMapper(ctx, d)
		return result[T, []TR]{d, r, err}
	}))
	castAddChild[T, result[T, []TR]](sImpl.addChild)(resultNode)

	successNode := newProcessorNode[result[T, []TR], result[T, []TR]](newFilterSupplier[result[T, []TR]](func(r result[T, []TR]) bool {
		return r.err == nil
	}))
	addChild(resultNode, successNode)
	mappedNode := newProcessorNode[result[T, []TR], TR](newFlatMapSupplier[result[T, []TR], TR](func(ctx context.Context, r result[T, []TR]) []TR {
		return r.success()
	}))
	addChild(successNode, mappedNode)

	failNode := newProcessorNode[result[T, []TR], result[T, []TR]](newFilterSupplier[result[T, []TR]](func(r result[T, []TR]) bool {
		return r.err != nil
	}))
	addChild(resultNode, failNode)
	failedNode := newProcessorNode[result[T, []TR], Fail[T]](newMapSupplier[result[T, []TR], Fail[T]](func(ctx context.Context, r result[T, []TR]) Fail[T] {
		return r.fail()
	}))
	addChild(failNode, failedNode)

	return &gstream[TR]{
			builder:  sImpl.builder,
			rid:      sImpl.rid,
			addChild: curryingAddChild[result[T, []TR], TR, TR](mappedNode),
		}, &failedGStream[T]{
			builder:  sImpl.builder,
			rid:      sImpl.rid,
			addChild: curryingAddChild[result[T, []TR], Fail[T], Fail[T]](failedNode),
		}
}

// -------------------------------

func newFailedGStream[T any](s GStream[Fail[T]]) *failedGStream[T] {
	sImpl := s.(*gstream[Fail[T]])
	return &failedGStream[T]{
		builder:  sImpl.builder,
		rid:      sImpl.rid,
		addChild: sImpl.addChild,
	}
}

type failedGStream[T any] struct {
	builder  *builder
	rid      routineID
	addChild func(*graphNode[Fail[T], Fail[T]])
}

var _ FailedGStream[any] = &failedGStream[any]{}

func (fs *failedGStream[T]) Filter(filter func(T, error) bool) FailedGStream[T] {
	filterNode := newProcessorNode[Fail[T], Fail[T]](newFilterSupplier[Fail[T]](func(f Fail[T]) bool {
		return filter(f.Arg, f.Err)
	}))
	fs.addChild(filterNode)

	return &failedGStream[T]{
		builder:  fs.builder,
		rid:      fs.rid,
		addChild: curryingAddChild[Fail[T], Fail[T], Fail[T]](filterNode),
	}
}

func (fs *failedGStream[T]) Foreach(foreacher func(context.Context, T, error)) {
	foreachNode := newProcessorNode[Fail[T], Fail[T]](newForeachSupplier(func(ctx context.Context, f Fail[T]) {
		foreacher(ctx, f.Arg, f.Err)
	}))
	fs.addChild(foreachNode)
}

func (fs *failedGStream[T]) ToStream() GStream[T] {
	mapNode := newProcessorNode[Fail[T], T](newMapSupplier[Fail[T], T](func(ctx context.Context, f Fail[T]) T {
		return f.Arg
	}))
	castAddChild[Fail[T], T](fs.addChild)(mapNode)

	return &gstream[T]{
		builder:  fs.builder,
		rid:      fs.rid,
		addChild: curryingAddChild[Fail[T], T, T](mapNode),
	}
}

// -------------------------------

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
	addChild func(*graphNode[KeyValue[K, V], KeyValue[K, V]])
}

var _ KeyValueGStream[any, any] = &keyValueGStream[any, any]{}

func (kvs *keyValueGStream[K, V]) gstream() *gstream[KeyValue[K, V]] {
	return &gstream[KeyValue[K, V]]{
		builder:  kvs.builder,
		rid:      kvs.rid,
		addChild: kvs.addChild,
	}
}

func (kvs *keyValueGStream[K, V]) Filter(filter func(KeyValue[K, V]) bool) KeyValueGStream[K, V] {
	s := kvs.gstream().Filter(filter).(*gstream[KeyValue[K, V]])

	return &keyValueGStream[K, V]{
		builder:  s.builder,
		rid:      s.rid,
		addChild: s.addChild,
	}
}

func (kvs *keyValueGStream[K, V]) Foreach(foreacher func(context.Context, KeyValue[K, V])) {
	kvs.gstream().Foreach(foreacher)
}

func (kvs *keyValueGStream[K, V]) Map(mapper func(context.Context, KeyValue[K, V]) KeyValue[K, V]) KeyValueGStream[K, V] {
	return KeyValueMap[K, V, K, V](kvs, mapper)
}

func (kvs *keyValueGStream[K, V]) MapErr(mapper func(context.Context, KeyValue[K, V]) (KeyValue[K, V], error)) (KeyValueGStream[K, V], FailedKeyValueGStream[K, V]) {
	return KeyValueMapErr[K, V, K, V](kvs, mapper)
}

func (kvs *keyValueGStream[K, V]) MapValues(mapper func(context.Context, V) V) KeyValueGStream[K, V] {
	return KeyValueMapValues[K, V, V](kvs, mapper)
}

func (kvs *keyValueGStream[K, V]) MapValuesErr(mapper func(context.Context, V) (V, error)) (KeyValueGStream[K, V], FailedKeyValueGStream[K, V]) {
	return KeyValueMapValuesErr[K, V, V](kvs, mapper)
}

func (kvs *keyValueGStream[K, V]) FlatMap(flatMapper func(context.Context, KeyValue[K, V]) []KeyValue[K, V]) KeyValueGStream[K, V] {
	return KeyValueFlatMap[K, V, K, V](kvs, flatMapper)
}

func (kvs *keyValueGStream[K, V]) FlatMapErr(flatMapper func(context.Context, KeyValue[K, V]) ([]KeyValue[K, V], error)) (KeyValueGStream[K, V], FailedKeyValueGStream[K, V]) {
	return KeyValueFlatMapErr[K, V, K, V](kvs, flatMapper)
}

func (kvs *keyValueGStream[K, V]) FlatMapValues(flatMapper func(context.Context, V) []V) KeyValueGStream[K, V] {
	return KeyValueFlatMapValues[K, V, V](kvs, flatMapper)
}

func (kvs *keyValueGStream[K, V]) FlatMapValuesErr(flatMapper func(context.Context, V) ([]V, error)) (KeyValueGStream[K, V], FailedKeyValueGStream[K, V]) {
	return KeyValueFlatMapValuesErr[K, V, V](kvs, flatMapper)
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
		kvs.builder.sctx.addStore(closer)
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

func KeyValueMap[K, V, KR, VR any](kvs KeyValueGStream[K, V], mapper func(context.Context, KeyValue[K, V]) KeyValue[KR, VR]) KeyValueGStream[KR, VR] {
	kvsImpl := kvs.(*keyValueGStream[K, V]).gstream()
	mkvs := Map[KeyValue[K, V], KeyValue[KR, VR]](kvsImpl, mapper).(*gstream[KeyValue[KR, VR]])

	return &keyValueGStream[KR, VR]{
		builder:  mkvs.builder,
		rid:      mkvs.rid,
		addChild: mkvs.addChild,
	}
}

func KeyValueMapErr[K, V, KR, VR any](kvs KeyValueGStream[K, V], mapper func(context.Context, KeyValue[K, V]) (KeyValue[KR, VR], error)) (KeyValueGStream[KR, VR], FailedKeyValueGStream[K, V]) {
	kvsImpl := kvs.(*keyValueGStream[K, V]).gstream()
	mapped, failed := MapErr[KeyValue[K, V], KeyValue[KR, VR]](kvsImpl, mapper)
	mappedImpl := mapped.(*gstream[KeyValue[KR, VR]])
	failedImpl := failed.(*failedGStream[KeyValue[K, V]])

	return &keyValueGStream[KR, VR]{
			builder:  mappedImpl.builder,
			rid:      mappedImpl.rid,
			addChild: mappedImpl.addChild,
		}, &failedKeyValueGStream[K, V]{
			builder:  failedImpl.builder,
			rid:      failedImpl.rid,
			addChild: failedImpl.addChild,
		}
}

func KeyValueMapValues[K, V, VR any](kvs KeyValueGStream[K, V], mapper func(context.Context, V) VR) KeyValueGStream[K, VR] {
	return KeyValueMap[K, V, K, VR](kvs, func(ctx context.Context, kv KeyValue[K, V]) KeyValue[K, VR] {
		return NewKeyValue(kv.Key, mapper(ctx, kv.Value))
	})
}

func KeyValueMapValuesErr[K, V, VR any](kvs KeyValueGStream[K, V], mapper func(context.Context, V) (VR, error)) (KeyValueGStream[K, VR], FailedKeyValueGStream[K, V]) {
	return KeyValueMapErr[K, V, K, VR](kvs, func(ctx context.Context, kv KeyValue[K, V]) (KeyValue[K, VR], error) {
		vr, err := mapper(ctx, kv.Value)
		return NewKeyValue(kv.Key, vr), err
	})
}

func KeyValueFlatMap[K, V, KR, VR any](kvs KeyValueGStream[K, V], flatMapper func(context.Context, KeyValue[K, V]) []KeyValue[KR, VR]) KeyValueGStream[KR, VR] {
	kvsImpl := kvs.(*keyValueGStream[K, V]).gstream()
	mkvs := FlatMap[KeyValue[K, V], KeyValue[KR, VR]](kvsImpl, flatMapper).(*gstream[KeyValue[KR, VR]])

	return &keyValueGStream[KR, VR]{
		builder:  mkvs.builder,
		rid:      mkvs.rid,
		addChild: mkvs.addChild,
	}
}

func KeyValueFlatMapErr[K, V, KR, VR any](kvs KeyValueGStream[K, V], flatMapper func(context.Context, KeyValue[K, V]) ([]KeyValue[KR, VR], error)) (KeyValueGStream[KR, VR], FailedKeyValueGStream[K, V]) {
	kvsImpl := kvs.(*keyValueGStream[K, V]).gstream()
	mapped, failed := FlatMapErr[KeyValue[K, V], KeyValue[KR, VR]](kvsImpl, flatMapper)
	mappedImpl := mapped.(*gstream[KeyValue[KR, VR]])
	failedImpl := failed.(*failedGStream[KeyValue[K, V]])

	return &keyValueGStream[KR, VR]{
			builder:  mappedImpl.builder,
			rid:      mappedImpl.rid,
			addChild: mappedImpl.addChild,
		}, &failedKeyValueGStream[K, V]{
			builder:  failedImpl.builder,
			rid:      failedImpl.rid,
			addChild: failedImpl.addChild,
		}
}

func KeyValueFlatMapValues[K, V, VR any](kvs KeyValueGStream[K, V], flatMapper func(context.Context, V) []VR) KeyValueGStream[K, VR] {
	return KeyValueFlatMap[K, V, K, VR](kvs, func(ctx context.Context, kv KeyValue[K, V]) []KeyValue[K, VR] {
		mvs := flatMapper(ctx, kv.Value)
		kvs := make([]KeyValue[K, VR], len(mvs))
		for i, mv := range mvs {
			kvs[i] = NewKeyValue(kv.Key, mv)
		}
		return kvs
	})
}

func KeyValueFlatMapValuesErr[K, V, VR any](kvs KeyValueGStream[K, V], flatMapper func(context.Context, V) ([]VR, error)) (KeyValueGStream[K, VR], FailedKeyValueGStream[K, V]) {
	return KeyValueFlatMapErr[K, V, K, VR](kvs, func(ctx context.Context, kv KeyValue[K, V]) ([]KeyValue[K, VR], error) {
		vrs, err := flatMapper(ctx, kv.Value)
		kvr := make([]KeyValue[K, VR], len(vrs))
		for i, vr := range vrs {
			kvr[i] = NewKeyValue(kv.Key, vr)
		}
		return kvr, err
	})
}

// -------------------------------

type failedKeyValueGStream[K, V any] struct {
	builder  *builder
	rid      routineID
	addChild func(*graphNode[Fail[KeyValue[K, V]], Fail[KeyValue[K, V]]])
}

var _ FailedKeyValueGStream[any, any] = &failedKeyValueGStream[any, any]{}

func (fkvs *failedKeyValueGStream[K, V]) gstream() *gstream[Fail[KeyValue[K, V]]] {
	return &gstream[Fail[KeyValue[K, V]]]{
		builder:  fkvs.builder,
		rid:      fkvs.rid,
		addChild: fkvs.addChild,
	}
}

func (fkvs *failedKeyValueGStream[K, V]) Filter(filter func(KeyValue[K, V], error) bool) FailedKeyValueGStream[K, V] {
	s := fkvs.gstream().Filter(func(f Fail[KeyValue[K, V]]) bool {
		return filter(f.Arg, f.Err)
	}).(*gstream[Fail[KeyValue[K, V]]])

	return &failedKeyValueGStream[K, V]{
		builder:  s.builder,
		rid:      s.rid,
		addChild: s.addChild,
	}
}

func (fkvs *failedKeyValueGStream[K, V]) Foreach(foreacher func(context.Context, KeyValue[K, V], error)) {
	fkvs.gstream().Foreach(func(ctx context.Context, f Fail[KeyValue[K, V]]) {
		foreacher(ctx, f.Arg, f.Err)
	})
}

func (fkvs *failedKeyValueGStream[K, V]) ToStream() KeyValueGStream[K, V] {
	s := Map[Fail[KeyValue[K, V]], KeyValue[K, V]](fkvs.gstream(), func(_ context.Context, f Fail[KeyValue[K, V]]) KeyValue[K, V] {
		return f.Arg
	}).(*gstream[KeyValue[K, V]])

	return &keyValueGStream[K, V]{
		builder:  s.builder,
		rid:      s.rid,
		addChild: s.addChild,
	}
}

// -------------------------------

func JoinStreamTable[K, V, VO, VR any](s KeyValueGStream[K, V], t GTable[K, VO], joiner func(K, V, VO) VR) KeyValueGStream[K, VR] {
	sImpl := s.(*keyValueGStream[K, V])
	valueGetter := t.(*gtable[K, VO]).valueGetter()
	joinSupplier := newStreamTableJoinSupplier(valueGetter, joiner)
	joinNode := newProcessorNode[KeyValue[K, V], KeyValue[K, VR]](joinSupplier)
	castAddChild[KeyValue[K, V], KeyValue[K, VR]](sImpl.addChild)(joinNode)

	currying := curryingAddChild[KeyValue[K, V], KeyValue[K, VR], KeyValue[K, VR]](joinNode)
	return &keyValueGStream[K, VR]{
		builder:  sImpl.builder,
		rid:      sImpl.rid,
		addChild: currying,
	}
}

func JoinStreamTableErr[K, V, VO, VR any](s KeyValueGStream[K, V], t GTable[K, VO], joiner func(K, V, VO) (VR, error)) (rs KeyValueGStream[K, VR], fs FailedKeyValueGStream[K, V]) {
	sImpl := s.(*keyValueGStream[K, V])
	valueGetter := t.(*gtable[K, VO]).valueGetter()
	joinSupplier := newStreamTableJoinSupplier[K, V, VO, result[KeyValue[K, V], KeyValue[K, VR]]](valueGetter, func(k K, v V, vo VO) result[KeyValue[K, V], KeyValue[K, VR]] {
		vr, err := joiner(k, v, vo)
		return result[KeyValue[K, V], KeyValue[K, VR]]{NewKeyValue(k, v), NewKeyValue(k, vr), err}
	})
	joinNode := newProcessorNode[KeyValue[K, V], KeyValue[K, result[KeyValue[K, V], KeyValue[K, VR]]]](joinSupplier)
	castAddChild[KeyValue[K, V], KeyValue[K, result[KeyValue[K, V], KeyValue[K, VR]]]](sImpl.addChild)(joinNode)

	sfs := newFilterSupplier[KeyValue[K, result[KeyValue[K, V], KeyValue[K, VR]]]](func(r KeyValue[K, result[KeyValue[K, V], KeyValue[K, VR]]]) bool {
		return r.Value.err == nil
	})
	sfn := newProcessorNode[KeyValue[K, result[KeyValue[K, V], KeyValue[K, VR]]], KeyValue[K, result[KeyValue[K, V], KeyValue[K, VR]]]](sfs)
	addChild(joinNode, sfn)
	sms := newMapSupplier[KeyValue[K, result[KeyValue[K, V], KeyValue[K, VR]]], KeyValue[K, VR]](func(_ context.Context, r KeyValue[K, result[KeyValue[K, V], KeyValue[K, VR]]]) KeyValue[K, VR] {
		return r.Value.success()
	})
	smn := newProcessorNode[KeyValue[K, result[KeyValue[K, V], KeyValue[K, VR]]], KeyValue[K, VR]](sms)
	addChild(sfn, smn)

	ffs := newFilterSupplier[KeyValue[K, result[KeyValue[K, V], KeyValue[K, VR]]]](func(r KeyValue[K, result[KeyValue[K, V], KeyValue[K, VR]]]) bool {
		return r.Value.err != nil
	})
	ffn := newProcessorNode[KeyValue[K, result[KeyValue[K, V], KeyValue[K, VR]]], KeyValue[K, result[KeyValue[K, V], KeyValue[K, VR]]]](ffs)
	addChild(joinNode, ffn)
	fms := newMapSupplier[KeyValue[K, result[KeyValue[K, V], KeyValue[K, VR]]], Fail[KeyValue[K, V]]](func(_ context.Context, r KeyValue[K, result[KeyValue[K, V], KeyValue[K, VR]]]) Fail[KeyValue[K, V]] {
		return r.Value.fail()
	})
	fmn := newProcessorNode[KeyValue[K, result[KeyValue[K, V], KeyValue[K, VR]]], Fail[KeyValue[K, V]]](fms)
	addChild(ffn, fmn)

	return &keyValueGStream[K, VR]{
			builder:  sImpl.builder,
			rid:      sImpl.rid,
			addChild: curryingAddChild[KeyValue[K, result[KeyValue[K, V], KeyValue[K, VR]]], KeyValue[K, VR], KeyValue[K, VR]](smn),
		}, &failedKeyValueGStream[K, V]{
			builder:  sImpl.builder,
			rid:      sImpl.rid,
			addChild: curryingAddChild[KeyValue[K, result[KeyValue[K, V], KeyValue[K, VR]]], Fail[KeyValue[K, V]], Fail[KeyValue[K, V]]](fmn),
		}
}

// -------------------------------

func Aggregate[K, V, VR any](kvs KeyValueGStream[K, V], initializer func() VR, aggregator func(KeyValue[K, V], VR) VR, mater materialized.Materialized[K, VR]) GTable[K, VR] {
	kvsImpl := kvs.(*keyValueGStream[K, V])

	kvstore := state.NewKeyValueStore(mater)
	if closer, ok := kvstore.(Closer); ok {
		kvsImpl.builder.sctx.addStore(closer)
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
