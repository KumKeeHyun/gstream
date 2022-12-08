package state

import (
	materialized2 "github.com/KumKeeHyun/gstream/state/materialized"
)

func NewKeyValueStore[K, V any](m materialized2.Materialized[K, V]) KeyValueStore[K, V] {
	switch m.StoreType() {
	case materialized2.InMemory:
		return newMemKeyValueStore[K, V](m)
	case materialized2.BoltDB:
		return newBoltDBKeyValueStore(m)
	default:
		return newMemKeyValueStore[K, V](m)
	}
}
