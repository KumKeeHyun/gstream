package state

import (
	"github.com/KumKeeHyun/gstream/state/materialized"
)

func NewKeyValueStore[K, V any](m materialized.Materialized[K, V]) KeyValueStore[K, V] {
	switch m.StoreType() {
	case materialized.InMemory:
		return newMemKeyValueStore[K, V](m)
	case materialized.BoltDB:
		return newBoltDBKeyValueStore(m)
	default:
		return newMemKeyValueStore[K, V](m)
	}
}
