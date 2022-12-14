package gstream

type graphNode[T, TR any] struct {
	rid routineID

	processor Processor[T]
	supplier  ProcessorSupplier[T, TR]
	forwards  func() []Processor[TR]

	isSrc bool
	sctx  *streamContext
	pipe  <-chan T
	pool  int
}

func (p *graphNode[_, _]) RoutineId() routineID {
	return p.rid
}

func newProcessorNode[T, TR any](supplier ProcessorSupplier[T, TR]) *graphNode[T, TR] {
	return &graphNode[T, TR]{
		supplier: supplier,
		forwards: func() []Processor[TR] { return make([]Processor[TR], 0) },
	}
}

func newVoidNode[T, TR any]() *graphNode[T, TR] {
	return newProcessorNode[T, TR](newVoidSupplier[T, TR]())
}

func newFallThroughNode[T any]() *graphNode[T, T] {
	return newProcessorNode[T, T](newFallThroughSupplier[T]())
}

func newSourceNode[T any](rid routineID, sctx *streamContext, pipe <-chan T, pool int) *graphNode[T, T] {
	node := newFallThroughNode[T]()
	node.rid = rid
	node.isSrc = true
	node.sctx = sctx
	node.pipe = pipe
	node.pool = pool
	return node
}

func newStreamToTableNode[K, V any](supplier *streamToTableSupplier[K, V]) *graphNode[KeyValue[K, V], KeyValue[K, Change[V]]] {
	return newProcessorNode[KeyValue[K, V], KeyValue[K, Change[V]]](supplier)
}

func newTableToValueStreamNode[K, V any]() *graphNode[KeyValue[K, Change[V]], V] {
	supplier := newTableToValueStreamSupplier[K, V]()
	return newProcessorNode[KeyValue[K, Change[V]], V](supplier)
}

func newTableToStreamNode[K, V any]() *graphNode[KeyValue[K, Change[V]], KeyValue[K, V]] {
	supplier := newTableToStreamSupplier[K, V]()
	return newProcessorNode[KeyValue[K, Change[V]], KeyValue[K, V]](supplier)
}

func addChild[T, TR, TRR any](parent *graphNode[T, TR], child *graphNode[TR, TRR]) {
	current := parent.forwards
	parent.forwards = func() []Processor[TR] {
		return append(current(), buildAndStart(child))
	}
	if !child.isSrc {
		child.rid = parent.rid
	}
}

func curryingAddChild[T, TR, TRR any](parent *graphNode[T, TR]) func(*graphNode[TR, TRR]) {
	return func(child *graphNode[TR, TRR]) {
		addChild(parent, child)
	}
}

func castAddChild[T, TR any](curriedAddChild func(*graphNode[T, T])) func(*graphNode[T, TR]) {
	return func(child *graphNode[T, TR]) {
		passNode := newFallThroughNode[T]()
		curriedAddChild(passNode)
		addChild(passNode, child)
	}
}

func buildAndStart[T, TR any](n *graphNode[T, TR]) Processor[T] {
	if n.processor == nil {
		n.processor = n.supplier.Processor(n.forwards()...)

		if n.isSrc {
			r := newRoutine(n.rid, n.pipe, n.pool, n.processor)
			r.run(n.sctx)
		}
	}

	return n.processor
}
