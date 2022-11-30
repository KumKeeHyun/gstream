package state

import (
	"github.com/KumKeeHyun/gstream/materialized"
)

func NewKeyValueStore[K, V any](m materialized.Materialized[K, V]) KeyValueStore[K, V] {
	switch m.StoreType() {
	// case InMemory:
	// case BoltDB:
	default:
		return newMemKeyValueStore[K, V](m.KeySerde())
	}
}
