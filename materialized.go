package gstream

type Materialized[K, V any] interface {
	KeySerde()   Serde[K]
	ValueSerde() Serde[V]
	StoreType()  StoreType
}

type StoreType int

const (
	MEMORY = iota
	BOLTDB
)
