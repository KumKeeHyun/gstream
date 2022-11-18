package gstream

import (
	"strconv"
	"testing"

	"go.uber.org/goleak"
)

func TestCloseStream(t *testing.T) {
	defer goleak.VerifyNone(t)

	builder := NewBuilder()

	intStream := Stream[int](builder).From(make(chan int))
	strStream := Stream[string](builder).From(make(chan string))
	Table[int, string](builder).From(make(chan string), func(s string) int {return 0}, IntSerde)
	mergedStream := Mapped[int, string](intStream).Map(strconv.Itoa).Merge(strStream)
	mergedStream.Pipe()

	close := builder.BuildAndStart()
	close()
}
