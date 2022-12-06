package gstream

type processorNode[T, TR any] struct {
	processor Processor[T]
	supplier  ProcessorSupplier[T, TR]
	forwards  func() []Processor[TR]

	routineID GStreamID

	sourceNode bool
	streamCtx  *gstreamContext
	source     chan T
}

func (p *processorNode[_, _]) RoutineId() GStreamID {
	return p.routineID
}

func newProcessorNode[T, TR any](supplier ProcessorSupplier[T, TR]) *processorNode[T, TR] {
	return &processorNode[T, TR]{
		supplier: supplier,
		forwards: func() []Processor[TR] { return make([]Processor[TR], 0) },
	}
}

func newFallThroughProcessorNode[T any]() *processorNode[T, T] {
	return newProcessorNode[T, T](newFallThroughProcessorSupplier[T]())
}

func newSourceNode[T any](routineID GStreamID, streamCtx *gstreamContext, source chan T) *processorNode[T, T] {
	node := newFallThroughProcessorNode[T]()
	node.routineID = routineID
	node.sourceNode = true
	node.streamCtx = streamCtx
	node.source = source
	return node
}

func newStreamToTableNode[K, V any](supplier *streamToTableProcessorSupplier[K, V]) *processorNode[KeyValue[K, V], KeyValue[K, Change[V]]] {
	return newProcessorNode[KeyValue[K, V], KeyValue[K, Change[V]]](supplier)
}

func newTableToValueStreamNode[K, V any]() *processorNode[KeyValue[K, Change[V]], V] {
	supplier := newTableToValueStreamProcessorSupplier[K, V]()
	return newProcessorNode[KeyValue[K, Change[V]], V](supplier)
}

func newTableToStreamNode[K, V any]() *processorNode[KeyValue[K, Change[V]], KeyValue[K, V]] {
	supplier := newTableToStreamProcessorSupplier[K, V]()
	return newProcessorNode[KeyValue[K, Change[V]], KeyValue[K, V]](supplier)
}

func addChild[T, TR, TRR any](parent *processorNode[T, TR], child *processorNode[TR, TRR]) {
	current := parent.forwards
	parent.forwards = func() []Processor[TR] {
		return append(current(), buildNode(child))
	}
	if !child.sourceNode {
		child.routineID = parent.routineID
	}
}

func curryingAddChild[T, TR, TRR any](parent *processorNode[T, TR]) func(*processorNode[TR, TRR]) {
	return func(child *processorNode[TR, TRR]) {
		addChild(parent, child)
	}
}

func castAddChild[T, TR any](curriedAddChild func(*processorNode[T, T])) func(*processorNode[T, TR]) {
	return func(child *processorNode[T, TR]) {
		passNode := newFallThroughProcessorNode[T]()
		curriedAddChild(passNode)
		addChild(passNode, child)
	}
}

func buildNode[T, TR any](node *processorNode[T, TR]) Processor[T] {
	if node.processor == nil {
		node.processor = node.supplier.Processor(node.forwards()...)

		if node.sourceNode {
			routine := newProcessorRoutine(node.routineID, node.source, node.processor)
			routine.start(node.streamCtx)
		}
	}

	return node.processor
}
