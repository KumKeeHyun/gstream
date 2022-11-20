package gstream

type GTable[K, V any] interface {
	ToValueStream() GStream[V]
}

type gtable[K, V any] struct {
	builder   *builder
	routineID GStreamID
	kvstore   ReadOnlyKeyValueStore[K, V]
	addChild  func(*processorNode[KeyValue[K, Change[V]], KeyValue[K, Change[V]]])
}

var _ GTable[any, any] = &gtable[any, any]{}

func (t *gtable[K, V]) ToValueStream() GStream[V] {
	passNode := newFallThroughProcessorNode[KeyValue[K, Change[V]]]()
	t.addChild(passNode)

	tableToStreamNode := newTableToStreamNode(newTableToStreamProcessorSupplier[K, V]())
	addChild(passNode, tableToStreamNode)

	return &gstream[V]{
		builder:   t.builder,
		routineID: t.routineID,
		addChild:  curryingAddChild[KeyValue[K, Change[V]], V, V](tableToStreamNode),
	}
}

func (t *gtable[K, V]) valueGetter() func(K) (V, error) {
	return t.kvstore.Get
}
