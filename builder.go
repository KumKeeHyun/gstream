package gstream

import (
	"context"
	"fmt"
	"github.com/KumKeeHyun/gstream/state/materialized"
)

type nextInt func() int

func newNextInt() nextInt {
	i := 0
	return func() int {
		i++
		return i
	}
}

type GStreamID string

type builder struct {
	nextInt nextInt
	sctx    *streamContext
	root    *processorNode[any, any]
}

func NewBuilder() *builder {
	return &builder{
		nextInt: newNextInt(),
		sctx:    newStreamContext(),
		root:    newVoidProcessorNode[any](),
	}
}

func (b *builder) getRoutineID() GStreamID {
	return GStreamID(fmt.Sprintf("routine-%d", b.nextInt()))
}

func Stream[T any](b *builder) *streamBuilder[T] {
	return &streamBuilder[T]{
		b: b,
	}
}

type streamBuilder[T any] struct {
	b *builder
}

func (sb *streamBuilder[T]) From(pipe chan T) GStream[T] {
	voidNode := newProcessorNode[any, T](newVoidProcessorSupplier[any, T]())
	addChild(sb.b.root, voidNode)
	srcNode := newSourceNode(sb.b.getRoutineID(), sb.b.sctx, pipe, 1)
	addChild(voidNode, srcNode)

	sb.b.sctx.add(newPipeCloser(pipe))

	return &gstream[T]{
		builder:   sb.b,
		routineID: srcNode.RoutineId(),
		addChild:  curryingAddChild[T, T, T](srcNode),
	}
}

func (tb *streamBuilder[T]) SliceSource(slice []T) GStream[T] {
	source := make(chan T, len(slice))
	for _, v := range slice {
		source <- v
	}
	// close(pipe)
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

func (tb *tableBuilder[K, V]) From(source chan V, selectKey func(V) K, materialized materialized.Materialized[K, V]) GTable[K, V] {
	s := Stream[V](tb.b).From(source)
	return SelectKey(s, selectKey).ToTable(materialized)
}

func (b *builder) BuildAndStart(ctx context.Context) {
	b.sctx.ctx = ctx
	build(b.root)

	b.sctx.wg.Wait()
	b.sctx.cleanUp()
}
