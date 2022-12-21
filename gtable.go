package gstream

import "github.com/KumKeeHyun/gstream/state"

// GTable is table interface for DSL.
type GTable[K, V any] interface {
	// ToValueStream convert this table to GStream.
	// GTable[K, V] -> GStream[V]
	ToValueStream() GStream[V]
	// ToStream convert this table to KeyValueGStream.
	// GTable[K, V] -> KeyValueGStream[K, V]
	ToStream() KeyValueGStream[K, V]
}

type gtable[K, V any] struct {
	builder  *builder
	rid      routineID
	kvstore  state.ReadOnlyKeyValueStore[K, V]
	addChild func(*graphNode[KeyValue[K, Change[V]], KeyValue[K, Change[V]]])
}

var _ GTable[any, any] = &gtable[any, any]{}

func (t *gtable[K, V]) ToValueStream() GStream[V] {
	tableToValueStreamNode := newTableToValueStreamNode[K, V]()
	castAddChild[KeyValue[K, Change[V]], V](t.addChild)(tableToValueStreamNode)

	return &gstream[V]{
		builder:  t.builder,
		rid:      t.rid,
		addChild: curryingAddChild[KeyValue[K, Change[V]], V, V](tableToValueStreamNode),
	}
}

func (t *gtable[K, V]) ToStream() KeyValueGStream[K, V] {
	tableToStreamNode := newTableToStreamNode[K, V]()
	castAddChild[KeyValue[K, Change[V]], KeyValue[K, V]](t.addChild)(tableToStreamNode)

	currying := curryingAddChild[KeyValue[K, Change[V]], KeyValue[K, V], KeyValue[K, V]](tableToStreamNode)
	return &keyValueGStream[K, V]{
		builder:  t.builder,
		rid:      t.rid,
		addChild: currying,
	}
}

func (t *gtable[K, V]) valueGetter() func(K) (V, error) {
	return t.kvstore.Get
}

// -------------------------------

// Aggregate aggregate records by the key.
func Aggregate[K, V, VR any](kvs KeyValueGStream[K, V], initializer func() VR, aggregator func(KeyValue[K, V], VR) VR, stateOpt state.Options[K, VR]) GTable[K, VR] {
	kvsImpl := kvs.(*keyValueGStream[K, V])

	kvstore := state.NewKeyValueStore(stateOpt)
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

// Count count the number of records by the key.
func Count[K, V any](kvs KeyValueGStream[K, V], stateOpt state.Options[K, int]) GTable[K, int] {
	cntInit := func() int { return 0 }
	cntAgg := func(_ KeyValue[K, V], cnt int) int { return cnt + 1 }

	return Aggregate(kvs, cntInit, cntAgg, stateOpt)
}
