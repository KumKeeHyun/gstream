package gstream

import "fmt"

type nextInt func() int

func newNextInt() nextInt {
	i := 0
	return func() int {
		i++
		return i
	}
}

type GStreamID string

const RootID = GStreamID("root")

type builder struct {
	nextInt   nextInt
	streamCtx *gstreamContext
	root      *processorNode[any, any]
}

func NewBuilder() *builder {
	return &builder{
		nextInt:   newNextInt(),
		streamCtx: newGStreamContext(),
		root:      newProcessorNode[any, any](newVoidProcessorSupplier[any, any]()),
	}
}

func (b *builder) getRoutineID() GStreamID {
	return GStreamID(fmt.Sprintf("routine-%d", b.nextInt()))
}

func (b *builder) getSinkID(rid GStreamID) GStreamID {
	return GStreamID(fmt.Sprintf("%s-sink-%d", rid, b.nextInt()))
}

func Stream[T any](b *builder) *streamBuilder[T] {
	return &streamBuilder[T]{
		b: b,
	}
}

type streamBuilder[T any] struct {
	b *builder
}

func (sb *streamBuilder[T]) From(source chan T) GStream[T] {
	voidNode := newProcessorNode[any, T](newVoidProcessorSupplier[any, T]())
	addChild(sb.b.root, voidNode)
	sourceNode := newSourceNode(sb.b.getRoutineID(), sb.b.streamCtx, source)
	addChild(voidNode, sourceNode)

	sb.b.streamCtx.addCloseChan(RootID,
		sourceNode.RoutineId(),
		func() { close(source) })

	return &gstream[T]{
		builder:   sb.b,
		routineID: sourceNode.RoutineId(),
		addChild:  curryingAddChild[T, T, T](sourceNode),
	}
}

func (tb *streamBuilder[T]) SliceSource(slice []T) GStream[T] {
	source := make(chan T, len(slice))
	for _, v := range slice {
		source <- v
	}
	// close(source)
	return tb.From(source)
}

func Table[K, V any](b *builder) *tableBuilder[K, V] {
	return &tableBuilder[K, V]{
		b: b,
	}
}

type tableBuilder[K, V any] struct {
	b *builder
}

func (tb *tableBuilder[K, V]) From(source chan V, selectKey func(V) K, materialized Materialized[K, V]) GTable[K, V] {
	stream := Stream[V](tb.b).From(source)
	return SelectKey(stream, selectKey).ToTable(materialized)
}

type StreamCloser func()

func (b *builder) BuildAndStart() StreamCloser {
	buildNode(b.root)
	return b.streamCtx.close
}
