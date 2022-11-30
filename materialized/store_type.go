package materialized

type StoreType int

const (
	InMemory = iota
	BoltDB
)
