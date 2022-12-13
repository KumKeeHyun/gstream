package gstream

import (
	"context"
	"fmt"
	"github.com/KumKeeHyun/gstream/options/source"
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

type routineID string

func NewBuilder() *builder {
	return &builder{
		nextInt: newNextInt(),
		sctx:    newStreamContext(),
		root:    newVoidNode[any, any](),
	}
}

type builder struct {
	nextInt nextInt
	sctx    *streamContext
	root    *processorNode[any, any]
}

func (b *builder) getRoutineID() routineID {
	return routineID(fmt.Sprintf("routine-%d", b.nextInt()))
}

func (b *builder) BuildAndStart(ctx context.Context) {
	b.sctx.ctx = ctx
	buildAndStart(b.root)

	b.sctx.wg.Wait()
	b.sctx.cleanUp()
}

func Stream[T any](b *builder) *streamBuilder[T] {
	return &streamBuilder[T]{
		b: b,
	}
}

type streamBuilder[T any] struct {
	b *builder
}

func (sb *streamBuilder[T]) From(pipe <-chan T, opts ...source.Option) GStream[T] {
	srcOpt := newSourceOption(opts...)

	voidNode := newVoidNode[any, T]()
	addChild(sb.b.root, voidNode)
	srcNode := newSourceNode(sb.b.getRoutineID(), sb.b.sctx, pipe, srcOpt.WorkerPool())
	addChild(voidNode, srcNode)

	return &gstream[T]{
		builder:  sb.b,
		rid:      srcNode.RoutineId(),
		addChild: curryingAddChild[T, T, T](srcNode),
	}
}

func Table[K, V any](b *builder) *tableBuilder[K, V] {
	return &tableBuilder[K, V]{
		b: b,
	}
}

type tableBuilder[K, V any] struct {
	b *builder
}

func (tb *tableBuilder[K, V]) From(pipe <-chan V, selectKey func(V) K, mater materialized.Materialized[K, V], opts ...source.Option) GTable[K, V] {
	s := Stream[V](tb.b).From(pipe, opts...)
	return SelectKey(s, selectKey).ToTable(mater)
}
