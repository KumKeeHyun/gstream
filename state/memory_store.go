package state

import (
	"errors"
	"github.com/KumKeeHyun/gstream/state/materialized"
	"sync"
)

func newMemKeyValueStore[K, V any](mater materialized.Materialized[K, V]) KeyValueStore[K, V] {
	return &memKeyValueStore[K, V]{
		store:    make(map[string]V, 100),
		keySerde: mater.KeySerde(),
		mu:       sync.Mutex{},
	}
}

type memKeyValueStore[K, V any] struct {
	store    map[string]V
	keySerde materialized.Serde[K]
	mu       sync.Mutex
}

var _ KeyValueStore[any, any] = &memKeyValueStore[any, any]{}

func (kvs *memKeyValueStore[K, V]) Get(key K) (V, error) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	keySer := kvs.keySerde.Serialize(key)
	v, exists := kvs.store[string(keySer)]
	if exists {
		return v, nil
	}
	return v, errors.New("cannot find value")
}

func (kvs *memKeyValueStore[K, V]) Put(key K, value V) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	keySer := kvs.keySerde.Serialize(key)
	kvs.store[string(keySer)] = value
}

func (kvs *memKeyValueStore[K, V]) Delete(key K) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	keySer := kvs.keySerde.Serialize(key)
	delete(kvs.store, string(keySer))
}
