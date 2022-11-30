package gstream

import "github.com/KumKeeHyun/gstream/state"

type GTable[K, V any] interface {
	ToValueStream() GStream[V]
	ToStream() KeyValueGStream[K, V]
}

type gtable[K, V any] struct {
	builder   *builder
	routineID GStreamID
	kvstore   state.ReadOnlyKeyValueStore[K, V]
	addChild  func(*processorNode[KeyValue[K, Change[V]], KeyValue[K, Change[V]]])
}

var _ GTable[any, any] = &gtable[any, any]{}

func (t *gtable[K, V]) ToValueStream() GStream[V] {
	passNode := newFallThroughProcessorNode[KeyValue[K, Change[V]]]()
	t.addChild(passNode)

	tableToStreamNode := newTableToValueStreamNode[K, V]()
	addChild(passNode, tableToStreamNode)

	return &gstream[V]{
		builder:   t.builder,
		routineID: t.routineID,
		addChild:  curryingAddChild[KeyValue[K, Change[V]], V, V](tableToStreamNode),
	}
}

func (t *gtable[K, V]) ToStream() KeyValueGStream[K, V] {
	passNode := newFallThroughProcessorNode[KeyValue[K, Change[V]]]()
	t.addChild(passNode)

	tableToStreamNode := newTableToStreamNode[K, V]()
	addChild(passNode, tableToStreamNode)

	currying := curryingAddChild[KeyValue[K, Change[V]], KeyValue[K, V], KeyValue[K, V]](tableToStreamNode)
	return &keyValueGStream[K, V]{
		builder:   t.builder,
		routineID: t.routineID,
		addChild:  currying,
	}
}

func (t *gtable[K, V]) valueGetter() func(K) (V, error) {
	return t.kvstore.Get
}
