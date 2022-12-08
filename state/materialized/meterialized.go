package materialized

import "errors"

type StoreType int

const (
	InMemory = iota
	BoltDB
)

type Materialized[K, V any] interface {
	KeySerde() Serde[K]
	ValueSerde() Serde[V]
	StoreType() StoreType
	Name() string
	DirPath() string
}

func New[K, V any](opts ...Option[K, V]) (Materialized[K, V], error) {
	// default materialized
	// key, value will be serialized/deserialized by json
	// table will be stored in memory
	m := &materialized[K, V]{
		keySerde:   &jsonSerde[K]{},
		valueSerde: &jsonSerde[V]{},
		storeType:  InMemory,
	}
	for _, opt := range opts {
		if err := opt(m); err != nil {
			return nil, err
		}
	}

	return m, nil
}

type materialized[K, V any] struct {
	keySerde   Serde[K]
	valueSerde Serde[V]
	storeType  StoreType
	name       string
	dirPath    string
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

func (m *materialized[K, V]) Name() string {
	return m.name
}

func (m *materialized[K, V]) DirPath() string {
	return m.dirPath
}

type Option[K, V any] func(*materialized[K, V]) error

func WithKeySerde[K, V any](keySerde Serde[K]) Option[K, V] {
	return func(m *materialized[K, V]) error {
		if keySerde == nil {
			return errors.New("keySerde must not be nil")
		}
		m.keySerde = keySerde
		return nil
	}
}

func WithValueSerde[K, V any](valueSerde Serde[V]) Option[K, V] {
	return func(m *materialized[K, V]) error {
		if valueSerde == nil {
			return errors.New("valueSerde must not be nil")
		}
		m.valueSerde = valueSerde
		return nil
	}
}

func WithInMemory[K, V any]() Option[K, V] {
	return func(m *materialized[K, V]) error {
		m.storeType = InMemory
		return nil
	}
}

func WithBoltDB[K, V any](name string) Option[K, V] {
	return func(m *materialized[K, V]) error {
		m.storeType = BoltDB
		m.name = name
		return nil
	}
}

func WithDirPath[K, V any](dirPath string) Option[K, V] {
	return func(m *materialized[K, V]) error {
		m.dirPath = dirPath
		return nil
	}
}
