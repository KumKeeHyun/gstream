package state

import (
	"errors"
	"github.com/KumKeeHyun/gstream/state/materialized"
	bolt "go.etcd.io/bbolt"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"
)

const (
	dbFile = "gstream.db"
)

var (
	dbs     = map[string]*bolt.DB{}
	dbslock = sync.Mutex{}
)

func getBoldDB(path string) *bolt.DB {
	dbslock.Lock()
	defer dbslock.Unlock()

	db, exists := dbs[path]
	if exists {
		return db
	}

	newDB := openBoltDB(path)
	dbs[path] = newDB
	return newDB
}

func openBoltDB(path string) *bolt.DB {
	bopts := &bolt.Options{}
	bopts.Timeout = time.Second

	db, err := bolt.Open(path, 0600, bopts)
	if err != nil {
		// TODO: handle error
		panic(err)
	}
	return db
}

func newBoltDBKeyValueStore[K, V any](mater materialized.Materialized[K, V]) KeyValueStore[K, V] {
	dbPath := path.Join(mater.DirPath(), dbFile)
	if err := os.MkdirAll(filepath.Dir(dbPath), os.ModePerm); err != nil {
		panic(err)
	}
	db := getBoldDB(dbPath)

	// Create new bucket with Materialized.Name().
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(mater.Name()))
		return err
	})
	if err != nil {
		// TODO: handle error
		panic(err)
	}

	return &boltDBKeyValueStore[K, V]{
		db:       db,
		bucket:   []byte(mater.Name()),
		keySerde: mater.KeySerde(),
		valSerde: mater.ValueSerde(),
	}
}

type boltDBKeyValueStore[K, V any] struct {
	db       *bolt.DB
	bucket   []byte
	keySerde materialized.Serde[K]
	valSerde materialized.Serde[V]
}

var _ KeyValueStore[any, any] = &boltDBKeyValueStore[any, any]{}

func (kvs boltDBKeyValueStore[K, V]) Get(key K) (v V, err error) {
	_ = kvs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(kvs.bucket)
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
	_ = kvs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(kvs.bucket)
		return b.Put(kvs.keySerde.Serialize(key), kvs.valSerde.Serialize(value))
	})
}

func (kvs boltDBKeyValueStore[K, V]) Delete(key K) {
	_ = kvs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(kvs.bucket)
		return b.Delete(kvs.keySerde.Serialize(key))
	})
}

func (kvs boltDBKeyValueStore[K, V]) Close() error {
	return kvs.db.Close()
}
