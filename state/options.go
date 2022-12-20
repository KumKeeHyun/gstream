package state

type StoreType int

const (
	InMemory = iota
	BoltDB
)

type Options[K, V any] interface {
	KeySerde() Serde[K]
	ValueSerde() Serde[V]
	StoreType() StoreType
	Name() string
	DirPath() string
}

func NewOptions[K, V any](opts ...Option[K, V]) Options[K, V] {
	// default options
	// key, value will be serialized/deserialized by json
	// table will be stored in memory
	o := &options[K, V]{
		keySerde:   &jsonSerde[K]{},
		valueSerde: &jsonSerde[V]{},
		storeType:  InMemory,
	}
	for _, opt := range opts {
		opt(o)
	}

	return o
}

type options[K, V any] struct {
	keySerde   Serde[K]
	valueSerde Serde[V]
	storeType  StoreType
	name       string
	dirPath    string
}

var _ Options[any, any] = &options[any, any]{}

func (o *options[K, V]) KeySerde() Serde[K] {
	return o.keySerde
}

func (o *options[K, V]) ValueSerde() Serde[V] {
	return o.valueSerde
}

func (o *options[K, V]) StoreType() StoreType {
	return o.storeType
}

func (o *options[K, V]) Name() string {
	return o.name
}

func (o *options[K, V]) DirPath() string {
	return o.dirPath
}

type Option[K, V any] func(*options[K, V])

func WithKeySerde[K, V any](keySerde Serde[K]) Option[K, V] {
	return func(m *options[K, V]) {
		if keySerde == nil {
			return
		}
		m.keySerde = keySerde
	}
}

func WithValueSerde[K, V any](valueSerde Serde[V]) Option[K, V] {
	return func(m *options[K, V]) {
		if valueSerde == nil {
			return
		}
		m.valueSerde = valueSerde
	}
}

func WithInMemory[K, V any]() Option[K, V] {
	return func(m *options[K, V]) {
		m.storeType = InMemory
	}
}

func WithBoltDB[K, V any](name string) Option[K, V] {
	return func(m *options[K, V]) {
		m.storeType = BoltDB
		m.name = name
	}
}

func WithDirPath[K, V any](dirPath string) Option[K, V] {
	return func(m *options[K, V]) {
		m.dirPath = dirPath
	}
}
