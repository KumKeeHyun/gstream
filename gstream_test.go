package gstream

import (
	"context"
	"errors"
	"fmt"
	"github.com/KumKeeHyun/gstream/state/materialized"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMapErr(t *testing.T) {
	b := NewBuilder()

	ch := make(chan int, 5)
	ch <- 1
	ch <- 2
	ch <- 3
	ch <- 4
	ch <- 5
	close(ch)

	res := make([]int, 0, 4)
	src := Stream[int](b).From(ch)
	mapped, _ := MapErr[int, int](src, func(_ context.Context, i int) (int, error) {
		if i == 3 {
			return 0, errors.New("mock error")
		}
		return i * i, nil
	})
	mapped.Foreach(func(_ context.Context, i int) {
		res = append(res, i)
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		b.BuildAndStart(ctx)
		done <- struct{}{}
	}()

	time.Sleep(time.Millisecond)
	cancel()

	assert.ElementsMatch(t, []int{1, 4, 16, 25}, res)
}

func TestKeyValueGStream_MapErr(t *testing.T) {
	b := NewBuilder()

	ch := make(chan int, 5)
	ch <- 1
	ch <- 2
	ch <- 3
	ch <- 4
	ch <- 5
	close(ch)

	res := make([]int, 0, 4)
	src := Stream[int](b).From(ch)
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

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		b.BuildAndStart(ctx)
		done <- struct{}{}
	}()

	time.Sleep(time.Millisecond)
	cancel()

	assert.ElementsMatch(t, []int{1, 4, 16, 25}, res)
}

func TestJoinedGStream_JoinTableErr(t *testing.T) {
	b := NewBuilder()

	sc := make(chan int)
	ssrc := Stream[int](b).From(sc)

	tc := make(chan int)
	selectKey := func(i int) int { return i }
	mater, _ := materialized.New(materialized.WithInMemory[int, int]())
	tsrc := Table[int, int](b).From(tc, selectKey, mater)

	joined, failed := Joined[int, int, int, string](SelectKey[int, int](ssrc, selectKey)).
		JoinTableErr(tsrc, func(k, v, vo int) (string, error) {
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

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		b.BuildAndStart(ctx)
		done <- struct{}{}
	}()

	tc <- 1
	tc <- 2
	tc <- 3
	tc <- 4

	sc <- 1
	sc <- 2
	sc <- 3
	sc <- 4
	time.Sleep(time.Millisecond)
	cancel()

	assert.Equal(t, 2, len(success))
	assert.Equal(t, 2, len(fail))
}
