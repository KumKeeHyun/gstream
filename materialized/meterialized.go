package materialized

import "github.com/KumKeeHyun/gstream"

func New[K, V any](opts ...Option[K, V]) *materialized[K, V] {
	m := &materialized[K, V]{
		storeType: gstream.MEMORY,
	}
	for _, opt := range opts {
		opt(m)
	}

	return m
}

type materialized[K, V any] struct {
	keySerde   gstream.Serde[K]
	valueSerde gstream.Serde[V]
	storeType  gstream.StoreType
}

var _ gstream.Materialized[any, any] = &materialized[any, any]{}

func (m *materialized[K, V]) KeySerde() gstream.Serde[K] {
	return m.keySerde
}

func (m *materialized[K, V]) ValueSerde() gstream.Serde[V] {
	return m.valueSerde
}

func (m *materialized[K, V]) StoreType() gstream.StoreType {
	return m.storeType
}

type Option[K, V any] func(*materialized[K, V])

func WithKeySerde[K, V any](keySerde gstream.Serde[K]) Option[K, V] {
	return func(m *materialized[K, V]) {
		m.keySerde = keySerde
	}
}

func WithValueSerde[K, V any](valueSerde gstream.Serde[V]) Option[K, V] {
	return func(m *materialized[K, V]) {
		m.valueSerde = valueSerde
	}
}

func WithInMemory[K, V any]() Option[K, V] {
	return func(m *materialized[K, V]) {
		m.storeType = gstream.MEMORY
	}
}

func WithBoltDB[K, V any]() Option[K, V] {
	return func(m *materialized[K, V]) {
		m.storeType = gstream.BOLTDB
	}
}
