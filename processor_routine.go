package gstream

func newProcessorRoutine[T any](routineID GStreamID, input chan T, composedProcessor Processor[T]) *processorRoutine[T] {
	return &processorRoutine[T]{
		routineID: routineID,
		input:     input,
		process:   composedProcessor,
	}
}

type processorRoutine[T any] struct {
	routineID GStreamID
	input     chan T
	process   Processor[T]
}

func (pr *processorRoutine[T]) start(ctx *gstreamContext) {
	ctx.addProcessorRoutine()

	go func() {
		defer func() {
			ctx.tryCloseChans(pr.routineID)
			ctx.closeStores(pr.routineID)
			ctx.doneProcessorRoutine()
		}()

		for in := range pr.input {
			pr.process(in)
		}
	}()
}
