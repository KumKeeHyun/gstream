package materialized

type Materialized[K, V any] interface {
	KeySerde() Serde[K]
	ValueSerde() Serde[V]
	StoreType() StoreType
}

func New[K, V any](opts ...Option[K, V]) Materialized[K, V] {
	m := &materialized[K, V]{
		storeType: InMemory,
	}
	for _, opt := range opts {
		opt(m)
	}

	return m
}

type materialized[K, V any] struct {
	keySerde   Serde[K]
	valueSerde Serde[V]
	storeType  StoreType
}

var _ Materialized[any, any] = &materialized[any, any]{}

func (m *materialized[K, V]) KeySerde() Serde[K] {
	return m.keySerde
}

func (m *materialized[K, V]) ValueSerde() Serde[V] {
	return m.valueSerde
}

func (m *materialized[K, V]) StoreType() StoreType {
	return m.storeType
}

type Option[K, V any] func(*materialized[K, V])

func WithKeySerde[K, V any](keySerde Serde[K]) Option[K, V] {
	return func(m *materialized[K, V]) {
		m.keySerde = keySerde
	}
}

func WithValueSerde[K, V any](valueSerde Serde[V]) Option[K, V] {
	return func(m *materialized[K, V]) {
		m.valueSerde = valueSerde
	}
}

func WithInMemory[K, V any]() Option[K, V] {
	return func(m *materialized[K, V]) {
		m.storeType = InMemory
	}
}

func WithBoltDB[K, V any]() Option[K, V] {
	return func(m *materialized[K, V]) {
		m.storeType = BoltDB
	}
}
