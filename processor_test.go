package gstream

import (
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFallThroughProcessor(t *testing.T) {
	var shouldBeEqualToV int
	p := newFallThroughProcessorSupplier[int]().
		Processor(func(v int) {
			shouldBeEqualToV = v
		})

	p(5)
	assert.Equal(t, 5, shouldBeEqualToV)
	p(100)
	assert.Equal(t, 100, shouldBeEqualToV)
	p(-1234)
	assert.Equal(t, -1234, shouldBeEqualToV)
}

// -------------------------------

func TestFilterProcessor(t *testing.T) {
	var trueIfBiggerThan10 bool
	p := newFilterProcessorSupplier(func(i int) bool {
		return i > 10
	}).Processor(func(v int) {
		trueIfBiggerThan10 = true
	})

	p(11)
	assert.True(t, trueIfBiggerThan10)

	trueIfBiggerThan10 = false
	p(10)
	assert.False(t, trueIfBiggerThan10)
}

// -------------------------------

func TestMapProcessor(t *testing.T) {
	var shouldBeItoa string
	p := newMapProcessorSupplier(strconv.Itoa).
		Processor(func(v string) {
			shouldBeItoa = v
		})

	p(10)
	assert.Equal(t, strconv.Itoa(10), shouldBeItoa)
	p(10000)
	assert.Equal(t, strconv.Itoa(10000), shouldBeItoa)
	p(-1234)
	assert.Equal(t, strconv.Itoa(-1234), shouldBeItoa)
}

// -------------------------------

func TestFlatMapProcessor(t *testing.T) {
	var shouldBeInc []int
	p := newFlatMapProcessorSupplier(func(i int) []int {
		res := make([]int, 0, i)
		for n := 0; n < i; n++ {
			res = append(res, n)
		}
		return res
	}).Processor(func(v int) {
		shouldBeInc = append(shouldBeInc, v)
	})

	p(5)
	assert.ElementsMatch(t, []int{0, 1, 2, 3, 4}, shouldBeInc)
}

// -------------------------------

type mockKvstore struct{
	store map[int]int
}

var _ KeyValueStore[int, int] = &mockKvstore{}

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
	p := newStreamToTableProcessorSupplier[int, int](mockKvs).
		Processor(func(kv KeyValue[int, Change[int]]) {
			shouldBeEqual = kv
		})

	p(NewKeyValue(1, 1))
	assert.Equal(t, 1, shouldBeEqual.Key)
	assert.Equal(t, 1, shouldBeEqual.Value.NewValue)

	p(NewKeyValue(1, 2))
	assert.Equal(t, 1, shouldBeEqual.Key)
	assert.Equal(t, 1, shouldBeEqual.Value.OldValue)
	assert.Equal(t, 2, shouldBeEqual.Value.NewValue)

	p(NewKeyValue(1, 3))
	assert.Equal(t, 1, shouldBeEqual.Key)
	assert.Equal(t, 2, shouldBeEqual.Value.OldValue)
	assert.Equal(t, 3, shouldBeEqual.Value.NewValue)
}

// -------------------------------

func TestStreamTableJoinProcessor(t *testing.T) {
	var shouldBeEqual KeyValue[int, int]

	foundGetter := func(int) (int, error) { return 10, nil }
	notFoundGetter := func(int) (int, error) { return 0, errors.New("mock error") }
	joiner := func(v, vo int) int { return v + vo}
	p := newStreamTableJoinProcessorSupplier(foundGetter, joiner).
		Processor(func(kv KeyValue[int, int]) {
			shouldBeEqual = kv
		})
	
	p(NewKeyValue(1, 1))
	assert.Equal(t, 1, shouldBeEqual.Key)
	assert.Equal(t, 11, shouldBeEqual.Value)

	p(NewKeyValue(2, 22))
	assert.Equal(t, 2, shouldBeEqual.Key)
	assert.Equal(t, 32, shouldBeEqual.Value)

	var trueIfProcessed bool

	p = newStreamTableJoinProcessorSupplier(notFoundGetter, joiner).
		Processor(func(kv KeyValue[int, int]) {
			trueIfProcessed = true
		})
	
	p(NewKeyValue(1, 1))
	assert.False(t, trueIfProcessed)

	p(NewKeyValue(2, 1))
	assert.False(t, trueIfProcessed)

}