package gstream

import (
	"github.com/KumKeeHyun/gstream/materialized"
	"strconv"
	"testing"

	"go.uber.org/goleak"
)

func TestCloseStream(t *testing.T) {
	defer goleak.VerifyNone(t)

	builder := NewBuilder()

	intStream := Stream[int](builder).From(make(chan int))
	strStream := Stream[string](builder).From(make(chan string))

	mater := materialized.New(
		materialized.WithKeySerde[int, string](materialized.IntSerde),
		materialized.WithInMemory[int, string](),
	)
	Table[int, string](builder).From(make(chan string),
		func(s string) int { return 0 },
		mater)
	mergedStream := Map(intStream, strconv.Itoa).Merge(strStream)
	mergedStream.Pipe()

	close := builder.BuildAndStart()
	close()
}
