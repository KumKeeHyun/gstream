package gstream

import "encoding/binary"

type Serde[T any] interface {
	Serialize(T) []byte
	Deserialize([]byte) T
}

var (
	IntSerde Serde[int] = &intSerde{}
)

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
