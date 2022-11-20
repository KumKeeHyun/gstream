package gstream

type ReadOnlyKeyValueStore[K, V any] interface {
	Get(key K) (V, error)
}

type KeyValueStore[K, V any] interface {
	ReadOnlyKeyValueStore[K, V]

	Put(key K, value V)
	Delete(key K)
}
