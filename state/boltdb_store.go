package state

import (
	"errors"
	"github.com/KumKeeHyun/gstream/materialized"
	bolt "go.etcd.io/bbolt"
	"os"
	"path"
	"path/filepath"
	"time"
)

var (
	dbFileDir = "boltdb"
)

func newBoltDBKeyValueStore[K, V any](name string, keySerde materialized.Serde[K], valSerde materialized.Serde[V]) KeyValueStore[K, V] {
	fPath := path.Join(dbFileDir, name+".db")
	if err := os.MkdirAll(filepath.Dir(fPath), os.ModePerm); err != nil {
		panic(err)
	}

	bopts := &bolt.Options{}
	bopts.Timeout = time.Second
	db, err := bolt.Open(fPath, 0600, bopts)
	if err != nil {
		// TODO: handle error
		panic(err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("gstream"))
		return err
	})
	if err != nil {
		// TODO: handle error
		panic(err)
	}

	return &boltDBKeyValueStore[K, V]{
		db:       db,
		keySerde: keySerde,
		valSerde: valSerde,
	}
}

type boltDBKeyValueStore[K, V any] struct {
	db       *bolt.DB
	keySerde materialized.Serde[K]
	valSerde materialized.Serde[V]
}

var _ KeyValueStore[any, any] = &boltDBKeyValueStore[any, any]{}

var _ StoreCloser = &boltDBKeyValueStore[any, any]{}

func (kvs boltDBKeyValueStore[K, V]) Get(key K) (v V, err error) {
	kvs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("gstream"))
		bv := b.Get(kvs.keySerde.Serialize(key))
		if bv == nil {
			err = errors.New("cannot find value")
		}
		v = kvs.valSerde.Deserialize(bv)
		return nil
	})
	return
}

func (kvs boltDBKeyValueStore[K, V]) Put(key K, value V) {
	kvs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("gstream"))
		return b.Put(kvs.keySerde.Serialize(key), kvs.valSerde.Serialize(value))
	})
}

func (kvs boltDBKeyValueStore[K, V]) Delete(key K) {
	kvs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("gstream"))
		return b.Delete(kvs.keySerde.Serialize(key))
	})
}

func (kvs boltDBKeyValueStore[K, V]) Close() error {
	return kvs.db.Close()
}
