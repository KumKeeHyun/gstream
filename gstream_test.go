package gstream

import (
	"context"
	"errors"
	"fmt"
	"github.com/KumKeeHyun/gstream/state/materialized"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"testing"
)

func TestMapErr(t *testing.T) {
	go goleak.VerifyNone(t)

	ch := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		ch <- i
	}
	close(ch)

	b := NewBuilder()
	src := Stream[int](b).From(ch)

	res := make([]int, 0, 4)
	mapped, _ := MapErr[int, int](src, func(_ context.Context, i int) (int, error) {
		if i == 3 {
			return 0, errors.New("mock error")
		}
		return i * i, nil
	})
	mapped.Foreach(func(_ context.Context, i int) {
		res = append(res, i)
	})

	b.BuildAndStart(context.Background())
	assert.ElementsMatch(t, []int{1, 4, 16, 25}, res)
}

func TestKeyValueGStream_MapErr(t *testing.T) {
	go goleak.VerifyNone(t)

	ch := make(chan int, 5)
	for i := 1; i <= 5; i++ {
		ch <- i
	}
	close(ch)

	b := NewBuilder()
	src := Stream[int](b).From(ch)

	res := make([]int, 0, 4)
	mapped, _ := SelectKey(src, func(i int) int { return i }).
		MapErr(func(_ context.Context, kv KeyValue[int, int]) (KeyValue[int, int], error) {
			if kv.Key == 3 {
				return kv, errors.New("mock error")
			}
			return NewKeyValue(kv.Key, kv.Value*kv.Value), nil
		})
	mapped.Foreach(func(_ context.Context, kv KeyValue[int, int]) {
		res = append(res, kv.Value)
	})

	b.BuildAndStart(context.Background())
	assert.ElementsMatch(t, []int{1, 4, 16, 25}, res)
}

func TestJoinedGStream_JoinTableErr(t *testing.T) {
	go goleak.VerifyNone(t)

	sc := make(chan int, 4)
	sc <- 1
	sc <- 2
	sc <- 3
	sc <- 4
	close(sc)

	tc := make(chan int, 4)
	tc <- 1
	tc <- 2
	tc <- 3
	tc <- 4
	close(tc)

	b := NewBuilder()
	ssrc := Stream[int](b).From(sc)

	selectKey := func(i int) int { return i }
	mater, _ := materialized.New(materialized.WithInMemory[int, int]())
	tsrc := Table[int, int](b).From(tc,
		selectKey,
		mater)


	joined, failed := JoinStreamTableErr[int, int, int, string](SelectKey[int, int](ssrc, selectKey), tsrc, func(k, v, vo int) (string, error) {
		if k%2 == 0 {
			return fmt.Sprintf("key: %d, value: %d, valueOut: %d", k, v, vo), nil
		} else {
			return "", errors.New("mock error")
		}
	})

	success := make([]string, 0)
	joined.Foreach(func(_ context.Context, kv KeyValue[int, string]) {
		success = append(success, kv.Value)
	})

	fail := make([]error, 0)
	failed.Foreach(func(_ context.Context, kv KeyValue[int, int], err error) {
		fail = append(fail, err)
	})

	b.BuildAndStart(context.Background())
	assert.Equal(t, 2, len(success))
	assert.Equal(t, 2, len(fail))
}
