package gstream

type processorNode[T, TR any] struct {
	processor Processor[T]
	supplier  ProcessorSupplier[T, TR]
	forwards  func() []Processor[TR]

	routineID GStreamID

	isSrc bool
	sctx  *streamContext
	pipe  chan T
	pool  int
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

func newVoidProcessorNode[T any]() *processorNode[T, T] {
	return newProcessorNode[T, T](newVoidProcessorSupplier[T, T]())
}

func newFallThroughProcessorNode[T any]() *processorNode[T, T] {
	return newProcessorNode[T, T](newFallThroughProcessorSupplier[T]())
}

func newSourceNode[T any](routineID GStreamID, sctx *streamContext, pipe chan T, pool int) *processorNode[T, T] {
	node := newFallThroughProcessorNode[T]()
	node.routineID = routineID
	node.isSrc = true
	node.sctx = sctx
	node.pipe = pipe
	node.pool = pool
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
		return append(current(), build(child))
	}
	if !child.isSrc {
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

func build[T, TR any](n *processorNode[T, TR]) Processor[T] {
	if n.processor == nil {
		n.processor = n.supplier.Processor(n.forwards()...)

		if n.isSrc {
			r := newRoutine(n.pipe, n.pool, n.processor)
			r.Run(n.sctx.ctx, n.sctx.wg)
		}
	}

	return n.processor
}
