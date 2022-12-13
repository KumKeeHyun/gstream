package gstream

import (
	"context"
	"errors"
	"github.com/KumKeeHyun/gstream/state"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFallThroughProcessor(t *testing.T) {
	var shouldBeEqualToV int
	p := newFallThroughSupplier[int]().
		Processor(func(ctx context.Context, v int) {
			shouldBeEqualToV = v
		})

	ctx := context.Background()
	p(ctx, 5)
	assert.Equal(t, 5, shouldBeEqualToV)
	p(ctx, 100)
	assert.Equal(t, 100, shouldBeEqualToV)
	p(ctx, -1234)
	assert.Equal(t, -1234, shouldBeEqualToV)
}

// -------------------------------

func TestFilterProcessor(t *testing.T) {
	var trueIfBiggerThan10 bool
	p := newFilterSupplier(func(i int) bool {
		return i > 10
	}).Processor(func(ctx context.Context, v int) {
		trueIfBiggerThan10 = true
	})

	ctx := context.Background()
	p(ctx, 11)
	assert.True(t, trueIfBiggerThan10)

	trueIfBiggerThan10 = false
	p(ctx, 10)
	assert.False(t, trueIfBiggerThan10)
}

// -------------------------------

func TestMapProcessor(t *testing.T) {
	var shouldBeItoa string
	p := newMapSupplier(func(_ context.Context, d int) string {
		return strconv.Itoa(d)
	}).
		Processor(func(ctx context.Context, v string) {
			shouldBeItoa = v
		})

	ctx := context.Background()
	p(ctx, 10)
	assert.Equal(t, strconv.Itoa(10), shouldBeItoa)
	p(ctx, 10000)
	assert.Equal(t, strconv.Itoa(10000), shouldBeItoa)
	p(ctx, -1234)
	assert.Equal(t, strconv.Itoa(-1234), shouldBeItoa)
}

// -------------------------------

func TestFlatMapProcessor(t *testing.T) {
	var shouldBeInc []int
	p := newFlatMapSupplier(func(ctx context.Context, i int) []int {
		res := make([]int, 0, i)
		for n := 0; n < i; n++ {
			res = append(res, n)
		}
		return res
	}).Processor(func(ctx context.Context, v int) {
		shouldBeInc = append(shouldBeInc, v)
	})

	ctx := context.Background()
	p(ctx, 5)
	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4}, shouldBeInc)
}

// -------------------------------

type mockKvstore struct {
	store map[int]int
}

var _ state.KeyValueStore[int, int] = &mockKvstore{}

func (kvs *mockKvstore) Get(key int) (int, error) {
	v, ok := kvs.store[key]
	if ok {
		return v, nil
	}
	return 0, errors.New("mock error")
}

func (kvs *mockKvstore) Put(key int, value int) {
	kvs.store[key] = value
}

func (*mockKvstore) Delete(key int) {
	panic("unimplemented")
}

// -------------------------------

func TestStreamToTableProcessor(t *testing.T) {
	var shouldBeEqual KeyValue[int, Change[int]]

	mockKvs := &mockKvstore{map[int]int{}}
	p := newStreamToTableSupplier[int, int](mockKvs).
		Processor(func(ctx context.Context, kv KeyValue[int, Change[int]]) {
			shouldBeEqual = kv
		})

	ctx := context.Background()
	p(ctx, NewKeyValue(1, 1))
	assert.Equal(t, 1, shouldBeEqual.Key)
	assert.Equal(t, 1, shouldBeEqual.Value.NewValue)

	p(ctx, NewKeyValue(1, 2))
	assert.Equal(t, 1, shouldBeEqual.Key)
	assert.Equal(t, 1, shouldBeEqual.Value.OldValue)
	assert.Equal(t, 2, shouldBeEqual.Value.NewValue)

	p(ctx, NewKeyValue(1, 3))
	assert.Equal(t, 1, shouldBeEqual.Key)
	assert.Equal(t, 2, shouldBeEqual.Value.OldValue)
	assert.Equal(t, 3, shouldBeEqual.Value.NewValue)
}

// -------------------------------

func TestStreamTableJoinProcessor(t *testing.T) {
	var shouldBeEqual KeyValue[int, int]

	foundGetter := func(int) (int, error) { return 10, nil }
	notFoundGetter := func(int) (int, error) { return 0, errors.New("mock error") }
	joiner := func(k, v, vo int) int { return v + vo }
	p := newStreamTableJoinSupplier(foundGetter, joiner).
		Processor(func(ctx context.Context, kv KeyValue[int, int]) {
			shouldBeEqual = kv
		})

	ctx := context.Background()
	p(ctx, NewKeyValue(1, 1))
	assert.Equal(t, 1, shouldBeEqual.Key)
	assert.Equal(t, 11, shouldBeEqual.Value)

	p(ctx, NewKeyValue(2, 22))
	assert.Equal(t, 2, shouldBeEqual.Key)
	assert.Equal(t, 32, shouldBeEqual.Value)

	var trueIfProcessed bool

	p = newStreamTableJoinSupplier(notFoundGetter, joiner).
		Processor(func(ctx context.Context, kv KeyValue[int, int]) {
			trueIfProcessed = true
		})

	p(ctx, NewKeyValue(1, 1))
	assert.False(t, trueIfProcessed)

	p(ctx, NewKeyValue(2, 1))
	assert.False(t, trueIfProcessed)

}
