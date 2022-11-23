package gstream

import (
	"strconv"
	"testing"

	"go.uber.org/goleak"
)

type mockMaterialized struct {}

func (m *mockMaterialized) KeySerde() Serde[int] {
	return IntSerde
}

func (m *mockMaterialized) ValueSerde() Serde[string] {
	return nil
}

func (m *mockMaterialized) StoreType() StoreType {
	return MEMORY
}

func TestCloseStream(t *testing.T) {
	defer goleak.VerifyNone(t)

	builder := NewBuilder()

	intStream := Stream[int](builder).From(make(chan int))
	strStream := Stream[string](builder).From(make(chan string))
	Table[int, string](builder).From(make(chan string), 
		func(s string) int {return 0}, 
		&mockMaterialized{})
	mergedStream := Map(intStream, strconv.Itoa).Merge(strStream)
	mergedStream.Pipe()

	close := builder.BuildAndStart()
	close()
}
