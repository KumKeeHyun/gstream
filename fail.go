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

type Pair[T1, T2 any] struct {
	First  T1
	Second T2
}

type biResult[T1, T2, TR any] struct {
	args Pair[T1, T2]
	res  TR
	err  error
}

func (r *biResult[T1, T2, TR]) success() TR {
	return r.res
}

func (r *biResult[T1, T2, TR]) fail() Fail[Pair[T1, T2]] {
	return NewFail(r.args, r.err)
}

type Tuple[T1, T2, T3 any] struct {
	First  T1
	Second T2
	Third  T3
}

type triResult[T1, T2, T3, TR any] struct {
	args Tuple[T1, T2, T3]
	res  TR
	err  error
}

func (r *triResult[T1, T2, T3, TR]) success() TR {
	return r.res
}

func (r *triResult[T1, T2, T3, TR]) fail() Fail[Tuple[T1, T2, T3]] {
	return NewFail(r.args, r.err)
}
