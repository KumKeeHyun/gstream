package gstream

import (
	"context"
	"fmt"
	"github.com/KumKeeHyun/gstream/options/source"
	"github.com/KumKeeHyun/gstream/state"
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

// NewBuilder create new builder.
// builder provides an entry point for the DSL.
//
//	b := gstream.NewBuilder()
//	gstream.Stream[int](b)
//	gstream.Table[int, string](b)
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
	root    *graphNode[any, any]
}

func (b *builder) newRoutineID() routineID {
	return routineID(fmt.Sprintf("routine-%d", b.nextInt()))
}

// BuildAndStart converts graph nodes to a processor
// and processing records received from the channel.
// it will block until all source channels are closed or context is cancelled.
func (b *builder) BuildAndStart(ctx context.Context) {
	b.sctx.ctx = ctx
	buildAndStart(b.root)
	b.sctx.wg.Wait()
	b.sctx.cleanUpStores()
}

// Stream is generics facilititators for create source stream.
func Stream[T any](b *builder) *streamBuilder[T] {
	return &streamBuilder[T]{
		b: b,
	}
}

type streamBuilder[T any] struct {
	b *builder
}

// From create new source stream from channel.
// The stream basically handle channel with single goroutine.
//
// If you want to handle channel with 3 goroutine,
//
//	gstream.Stream[int](b).From(ch, source.WithWorkerPool(3))
func (sb *streamBuilder[T]) From(pipe <-chan T, opts ...source.Option) GStream[T] {
	srcOpt := newSourceOption(opts...)

	voidNode := newVoidNode[any, T]()
	addChild(sb.b.root, voidNode)
	srcNode := newSourceNode(sb.b.newRoutineID(), sb.b.sctx, pipe, srcOpt.WorkerPool())
	addChild(voidNode, srcNode)

	return &gstream[T]{
		builder:  sb.b,
		rid:      srcNode.RoutineId(),
		addChild: curryingAddChild[T, T, T](srcNode),
	}
}

// Table is generics facilititators for create source table.
func Table[K, V any](b *builder) *tableBuilder[K, V] {
	return &tableBuilder[K, V]{
		b: b,
	}
}

type tableBuilder[K, V any] struct {
	b *builder
}

// From create new source table from channel.
// This function is the same as the following code
//
//	s := gstream.Stream[string](b).From(ch, opts)
//	gstream.SelectKey(s, keySelector).ToTable(sopt)
func (tb *tableBuilder[K, V]) From(pipe <-chan V, selectKey func(V) K, stateOpt state.Options[K, V], opts ...source.Option) GTable[K, V] {
	s := Stream[V](tb.b).From(pipe, opts...)
	return SelectKey(s, selectKey).ToTable(stateOpt)
}
