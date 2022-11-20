package gstream

type StoreType int

const (
	MEMORY = iota
	BOLTDB
)

func newKeyValueStore[K, V any](m Materialized[K, V]) KeyValueStore[K, V] {
	switch m.StoreType() {
	// case MEMORY:
	// case BOLTDB:
	default:
		return newMemKeyValueStore[K, V](m.KeySerde())
	}
}
