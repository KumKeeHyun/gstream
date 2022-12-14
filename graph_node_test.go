package gstream

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockSupplier[V, VR any] struct{}

var _ ProcessorSupplier[any, any] = &mockSupplier[any, any]{}

func (*mockSupplier[V, VR]) Processor(_ ...Processor[VR]) Processor[V] {
	return func(ctx context.Context, v V) {}
}

func newMockNode[V, VR any]() *graphNode[V, VR] {
	return newProcessorNode[V, VR](&mockSupplier[V, VR]{})
}

func assertEqualPointer(t *testing.T, expected, actual any) {
	assert.Equal(t, reflect.ValueOf(expected).Pointer(), reflect.ValueOf(actual).Pointer())
}

func TestAddChildStraight(t *testing.T) {
	first := newMockNode[int, string]()
	second := newMockNode[string, int]()
	third := newMockNode[int, float64]()

	addChild(first, second)
	addChild(second, third)

	ffs := first.forwards()
	assert.Equal(t, 1, len(ffs))
	assertEqualPointer(t, second.processor, ffs[0])

	sfs := second.forwards()
	assert.Equal(t, 1, len(sfs))
	assertEqualPointer(t, third.processor, sfs[0])

	assert.Equal(t, 0, len(third.forwards()))
}

func TestAddChildSplit(t *testing.T) {
	first := newMockNode[int, string]()
	second := newMockNode[string, int]()
	third := newMockNode[string, float64]()

	addChild(first, second)
	addChild(first, third)

	ffs := first.forwards()
	assert.Equal(t, 2, len(ffs))
	assertEqualPointer(t, second.processor, ffs[0])
	assertEqualPointer(t, third.processor, ffs[1])

	assert.Equal(t, 0, len(second.forwards()))
	assert.Equal(t, 0, len(third.forwards()))
}

func TestAddChildMerge(t *testing.T) {
	first := newMockNode[int, string]()
	second := newMockNode[int, string]()
	third := newMockNode[string, float64]()

	addChild(first, third)
	addChild(second, third)

	ffs := first.forwards()
	assert.Equal(t, 1, len(ffs))
	assertEqualPointer(t, third.processor, ffs[0])

	sfs := second.forwards()
	assert.Equal(t, 1, len(sfs))
	assertEqualPointer(t, third.processor, sfs[0])

	assert.Equal(t, 0, len(third.forwards()))
}
