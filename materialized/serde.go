package materialized

import (
	"encoding/binary"
	"encoding/json"
)

type Serde[T any] interface {
	Serialize(T) []byte
	Deserialize([]byte) T
}

var (
	IntSerde Serde[int]    = &intSerde{}
	StrSerde Serde[string] = &stringSerde{}
)

type jsonSerde[T any] struct{}

var _ Serde[any] = &jsonSerde[any]{}

func (*jsonSerde[T]) Serialize(o T) []byte {
	res, _ := json.Marshal(o)
	return res
}

func (*jsonSerde[T]) Deserialize(b []byte) T {
	var res T
	json.Unmarshal(b, &res)
	return res
}

type intSerde struct{}

var _ Serde[int] = &intSerde{}

func (*intSerde) Serialize(i int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i))
	return b
}

func (*intSerde) Deserialize(b []byte) int {
	return int(binary.BigEndian.Uint64(b))
}

type stringSerde struct{}

var _ Serde[string] = &stringSerde{}

func (*stringSerde) Serialize(s string) []byte {
	return []byte(s)
}

func (*stringSerde) Deserialize(b []byte) string {
	return string(b)
}
