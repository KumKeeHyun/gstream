package state

import "encoding/json"

type Serde[T any] interface {
	Serialize(T) []byte
	Deserialize([]byte) T
}

type jsonSerde[T any] struct{}

var _ Serde[any] = &jsonSerde[any]{}

func (*jsonSerde[T]) Serialize(o T) []byte {
	res, _ := json.Marshal(o)
	return res
}

func (*jsonSerde[T]) Deserialize(b []byte) T {
	var res T
	_ = json.Unmarshal(b, &res)
	return res
}
