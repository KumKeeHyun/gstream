package pipe

type options interface {
	SetWorkerPool(pool int)
	SetBufferedChan(cap int)
}

type Option func(options)

func WithWorkerPool(pool int) Option {
	return func(o options) {
		o.SetWorkerPool(pool)
	}
}

func WithBufferedChan(cap int) Option {
	return func(o options) {
		o.SetBufferedChan(cap)
	}
}
