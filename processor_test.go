package gstream

import (
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