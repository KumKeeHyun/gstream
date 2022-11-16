package gstream

type Change[T any] struct {
	OldValue, NewValue T
}

func NewChange[T any](ov, nv T) Change[T] {
	return Change[T]{
		OldValue: ov,
		NewValue: nv,
	}
}