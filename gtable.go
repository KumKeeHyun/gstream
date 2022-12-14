package gstream

import "github.com/KumKeeHyun/gstream/state"

type GTable[K, V any] interface {
	ToValueStream() GStream[V]
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
