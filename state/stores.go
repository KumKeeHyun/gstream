package state

import (
	"github.com/KumKeeHyun/gstream/materialized"
)

func NewKeyValueStore[K, V any](m materialized.Materialized[K, V]) KeyValueStore[K, V] {
	switch m.StoreType() {
	case materialized.InMemory:
		return newMemKeyValueStore[K, V](m.KeySerde())
	case materialized.BoltDB:
		return newBoltDBKeyValueStore(m.Name(), m.KeySerde(), m.ValueSerde())
	default:
		return newMemKeyValueStore[K, V](m.KeySerde())
	}
}
