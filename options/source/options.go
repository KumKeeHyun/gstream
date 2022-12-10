package source

type options interface {
	SetWorkerPool(pool int)
}

type Option func(options)

func WithWorkerPool(pool int) Option {
	return func(o options) {
		o.SetWorkerPool(pool)
	}
}
