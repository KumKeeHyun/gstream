package gstream

func newRoutine[T any](rid routineID, pipe <-chan T, poolSize int, processor Processor[T]) *routine[T] {
	return &routine[T]{
		rid:      rid,
		pipe:     pipe,
		poolSize: poolSize,
		process:  processor,
	}
}

type routine[T any] struct {
	rid      routineID
	pipe     <-chan T
	poolSize int
	process  Processor[T]
}

func (r *routine[T]) run(sctx *streamContext) {
	for i := 0; i < r.poolSize; i++ {
		sctx.startRoutine(r.rid)
		go worker(r.rid, sctx, r.pipe, r.process)
	}
}

func worker[T any](rid routineID, sctx *streamContext, pipe <-chan T, process Processor[T]) {
	defer func() {
		sctx.doneRoutine(rid)
	}()

	ctx := sctx.ctx
	for {
		select {
		case d, ok := <-pipe:
			if !ok {
				return
			}

			process(ctx, d)
		case <-ctx.Done():
			return
		}
	}
}
