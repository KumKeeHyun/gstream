package sink

import (
	"time"
)

type options interface {
	SetBufferedChan(cap int)
	SetTimeout(t time.Duration)
}

type Option func(options)

func WithBufferedChan(cap int) Option {
	return func(o options) {
		o.SetBufferedChan(cap)
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(o options) {
		o.SetTimeout(timeout)
	}
}
