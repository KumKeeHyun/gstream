package gstream

type Fail[T any] struct {
	Arg T
	Err error
}

func NewFail[T any](v T, err error) Fail[T] {
	return Fail[T]{
		Arg: v,
		Err: err,
	}
}

type result[T, TR any] struct {
	arg T
	res TR
	err error
}

func (r *result[T, TR]) success() TR {
	return r.res
}

func (r *result[T, TR]) fail() Fail[T] {
	return NewFail(r.arg, r.err)
}
