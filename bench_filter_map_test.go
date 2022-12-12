package gstream

import (
	"context"
	"math/rand"
	"testing"
)

func genericFilterMap(s []int) []int {
	return genericMap(genericFilter(s, func(i int) bool {
		return i%2 == 0
	}), func(i int) int {
		return i * 2
	})
}

func genericFilterMap10Times(s []int) []int {
	res1 := genericMap(genericFilter(s, func(i int) bool {
		return i%2 == 0
	}), func(i int) int {
		return i * 2
	})
	res2 := genericMap(genericFilter(res1, func(i int) bool {
		return i%2 == 0
	}), func(i int) int {
		return i * 2
	})
	res3 := genericMap(genericFilter(res2, func(i int) bool {
		return i%2 == 0
	}), func(i int) int {
		return i * 2
	})
	res4 := genericMap(genericFilter(res3, func(i int) bool {
		return i%2 == 0
	}), func(i int) int {
		return i * 2
	})
	res5 := genericMap(genericFilter(res4, func(i int) bool {
		return i%2 == 0
	}), func(i int) int {
		return i * 2
	})
	res6 := genericMap(genericFilter(res5, func(i int) bool {
		return i%2 == 0
	}), func(i int) int {
		return i * 2
	})
	res7 := genericMap(genericFilter(res6, func(i int) bool {
		return i%2 == 0
	}), func(i int) int {
		return i * 2
	})
	res8 := genericMap(genericFilter(res7, func(i int) bool {
		return i%2 == 0
	}), func(i int) int {
		return i * 2
	})
	res9 := genericMap(genericFilter(res8, func(i int) bool {
		return i%2 == 0
	}), func(i int) int {
		return i * 2
	})
	res := genericMap(genericFilter(res9, func(i int) bool {
		return i%2 == 0
	}), func(i int) int {
		return i * 2
	})

	return res
}

func genericFilter[T any](s []T, filter func(T) bool) []T {
	res := make([]T, len(s))
	for _, v := range s {
		if filter(v) {
			res = append(res, v)
		}
	}
	return res
}

func genericMap[T, TR any](s []T, mapper func(T) TR) []TR {
	res := make([]TR, len(s))
	for i, v := range s {
		res[i] = mapper(v)
	}
	return res
}

func gstreamFilterMap(s []int) []int {
	input := make(chan int)
	res := make([]int, len(s))

	builder := NewBuilder()
	Stream[int](builder).
		From(input).
		Filter(func(i int) bool { return i%2 == 0 }).
		Map(func(_ context.Context, i int) int { return i * 2 }).
		Foreach(func(_ context.Context, i int) {
			res = append(res, i)
		})

	done := make(chan struct{})
	go func() {
		builder.BuildAndStart(context.Background())
		done <- struct{}{}
	}()

	for _, v := range s {
		input <- v
	}
	close(input)
	<-done
	return res
}

func gstreamFilterMap10Times(s []int) []int {
	input := make(chan int)
	res := make([]int, len(s))

	builder := NewBuilder()
	Stream[int](builder).
		From(input).
		Filter(func(i int) bool { return i%2 == 0 }).
		Map(func(_ context.Context, i int) int { return i * 2 }).
		Filter(func(i int) bool { return i%2 == 0 }).
		Map(func(_ context.Context, i int) int { return i * 2 }).
		Filter(func(i int) bool { return i%2 == 0 }).
		Map(func(_ context.Context, i int) int { return i * 2 }).
		Filter(func(i int) bool { return i%2 == 0 }).
		Map(func(_ context.Context, i int) int { return i * 2 }).
		Filter(func(i int) bool { return i%2 == 0 }).
		Map(func(_ context.Context, i int) int { return i * 2 }).
		Filter(func(i int) bool { return i%2 == 0 }).
		Map(func(_ context.Context, i int) int { return i * 2 }).
		Filter(func(i int) bool { return i%2 == 0 }).
		Map(func(_ context.Context, i int) int { return i * 2 }).
		Filter(func(i int) bool { return i%2 == 0 }).
		Map(func(_ context.Context, i int) int { return i * 2 }).
		Filter(func(i int) bool { return i%2 == 0 }).
		Map(func(_ context.Context, i int) int { return i * 2 }).
		Filter(func(i int) bool { return i%2 == 0 }).
		Foreach(func(_ context.Context, i int) {
			res = append(res, i)
		})

	done := make(chan struct{})
	go func() {
		builder.BuildAndStart(context.Background())
		done <- struct{}{}
	}()

	for _, v := range s {
		input <- v
	}
	close(input)
	<-done
	return res
}

func randIntSlice(size int) []int {
	s := make([]int, size)
	for i := range s {
		s[i] = rand.Int()
	}
	return s
}

func BenchmarkGenericSize1000(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		s := randIntSlice(1000)
		b.StartTimer()
		genericFilterMap(s)
	}
}

func BenchmarkGStreamSize1000(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		s := randIntSlice(1000)
		b.StartTimer()
		gstreamFilterMap(s)
	}
}

func BenchmarkGenericSize100000(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		s := randIntSlice(100000)
		b.StartTimer()
		genericFilterMap(s)
	}
}

func BenchmarkGStreamSize100000(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		s := randIntSlice(100000)
		b.StartTimer()
		gstreamFilterMap(s)
	}
}

func BenchmarkGeneric10TimesSize1000(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		s := randIntSlice(1000)
		b.StartTimer()
		genericFilterMap10Times(s)
	}
}

func BenchmarkGStream10TimesSize1000(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		s := randIntSlice(1000)
		b.StartTimer()
		gstreamFilterMap10Times(s)
	}
}

func BenchmarkGStream10TimesSize100000(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		s := randIntSlice(100000)
		b.StartTimer()
		gstreamFilterMap10Times(s)
	}
}
