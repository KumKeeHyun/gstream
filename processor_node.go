package gstream

type processorNode[T, TR any] struct {
	processor Processor[T]
	supplier  ProcessorSupplier[T, TR]
	forwards  func() []Processor[TR]

	rid GStreamID

	isSrc bool
	sctx  *streamContext
	pipe  chan T
	pool  int
}

func (p *processorNode[_, _]) RoutineId() GStreamID {
	return p.rid
}

func newProcessorNode[T, TR any](supplier ProcessorSupplier[T, TR]) *processorNode[T, TR] {
	return &processorNode[T, TR]{
		supplier: supplier,
		forwards: func() []Processor[TR] { return make([]Processor[TR], 0) },
	}
}

func newVoidNode[T, TR any]() *processorNode[T, TR] {
	return newProcessorNode[T, TR](newVoidSupplier[T, TR]())
}

func newFallThroughNode[T any]() *processorNode[T, T] {
	return newProcessorNode[T, T](newFallThroughSupplier[T]())
}

func newSourceNode[T any](rid GStreamID, sctx *streamContext, pipe chan T, pool int) *processorNode[T, T] {
	node := newFallThroughNode[T]()
	node.rid = rid
	node.isSrc = true
	node.sctx = sctx
	node.pipe = pipe
	node.pool = pool
	return node
}

func newStreamToTableNode[K, V any](supplier *streamToTableSupplier[K, V]) *processorNode[KeyValue[K, V], KeyValue[K, Change[V]]] {
	return newProcessorNode[KeyValue[K, V], KeyValue[K, Change[V]]](supplier)
}

func newTableToValueStreamNode[K, V any]() *processorNode[KeyValue[K, Change[V]], V] {
	supplier := newTableToValueStreamSupplier[K, V]()
	return newProcessorNode[KeyValue[K, Change[V]], V](supplier)
}

func newTableToStreamNode[K, V any]() *processorNode[KeyValue[K, Change[V]], KeyValue[K, V]] {
	supplier := newTableToStreamSupplier[K, V]()
	return newProcessorNode[KeyValue[K, Change[V]], KeyValue[K, V]](supplier)
}

func addChild[T, TR, TRR any](parent *processorNode[T, TR], child *processorNode[TR, TRR]) {
	current := parent.forwards
	parent.forwards = func() []Processor[TR] {
		return append(current(), buildAndStart(child))
	}
	if !child.isSrc {
		child.rid = parent.rid
	}
}

func curryingAddChild[T, TR, TRR any](parent *processorNode[T, TR]) func(*processorNode[TR, TRR]) {
	return func(child *processorNode[TR, TRR]) {
		addChild(parent, child)
	}
}

func castAddChild[T, TR any](curriedAddChild func(*processorNode[T, T])) func(*processorNode[T, TR]) {
	return func(child *processorNode[T, TR]) {
		passNode := newFallThroughNode[T]()
		curriedAddChild(passNode)
		addChild(passNode, child)
	}
}

func buildAndStart[T, TR any](n *processorNode[T, TR]) Processor[T] {
	if n.processor == nil {
		n.processor = n.supplier.Processor(n.forwards()...)

		if n.isSrc {
			r := newRoutine(n.pipe, n.pool, n.processor)
			r.run(n.sctx.ctx, n.sctx.wg)
		}
	}

	return n.processor
}
