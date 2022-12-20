package state

func NewKeyValueStore[K, V any](opts Options[K, V]) KeyValueStore[K, V] {
	switch opts.StoreType() {
	case InMemory:
		return newMemKeyValueStore(opts)
	case BoltDB:
		return newBoltDBKeyValueStore(opts)
	default:
		return newMemKeyValueStore(opts)
	}
}
