package gstream

import (
	"github.com/KumKeeHyun/gstream/state"
	"sync"
)

func newGStreamContext() *gstreamContext {
	return &gstreamContext{
		routineGroup: sync.WaitGroup{},
		chanContext:  newChanContext(),
		storeContext: newStoreContext(),
	}
}

type gstreamContext struct {
	chanContext
	storeContext
	routineGroup sync.WaitGroup
}

func (c *gstreamContext) close() {
	c.tryCloseChans(RootID)
	c.routineGroup.Wait()
}

func (c *gstreamContext) addProcessorRoutine() {
	c.routineGroup.Add(1)
}

func (c *gstreamContext) doneProcessorRoutine() {
	c.routineGroup.Done()
}

func newChanContext() chanContext {
	return chanContext{
		mu:        sync.Mutex{},
		closers:   map[GStreamID]chanCloser{},
		refers:    map[GStreamID][]GStreamID{},
		referCnts: map[GStreamID]int{},
	}
}

type chanCloser func()

func safeChanCloser[T any](ch chan T) chanCloser {
	return func() {
		defer func() {
			if r := recover(); r != nil {
				// TODO: logging
			}
		}()
		close(ch)
	}
}

type chanContext struct {
	mu        sync.Mutex
	closers   map[GStreamID]chanCloser
	refers    map[GStreamID][]GStreamID
	referCnts map[GStreamID]int
}

func (c *chanContext) addChanCloser(parentID, childID GStreamID, closer chanCloser) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closers[childID] = closer
	c.refers[parentID] = append(c.refers[parentID], childID)
	cnt := c.referCnts[childID]
	c.referCnts[childID] = cnt + 1
}

func (c *chanContext) tryCloseChans(routineID GStreamID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	refers, exists := c.refers[routineID]
	if !exists {
		return
	}
	for _, childID := range refers {
		cnt := c.referCnts[childID]
		if cnt <= 1 {
			if closeChan, exists := c.closers[childID]; exists {
				closeChan()
			}
			delete(c.closers, childID)
		}
		c.referCnts[childID] = cnt - 1
	}
}

func newStoreContext() storeContext {
	return storeContext{
		mu:     sync.Mutex{},
		stores: map[GStreamID][]state.StoreCloser{},
	}
}

type storeContext struct {
	mu     sync.Mutex
	stores map[GStreamID][]state.StoreCloser
}

func (sc *storeContext) addStore(routineID GStreamID, store state.StoreCloser) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.stores[routineID] = append(sc.stores[routineID], store)
}

func (sc *storeContext) closeStores(routineID GStreamID) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	stores, exists := sc.stores[routineID]
	if !exists {
		return
	}
	for _, store := range stores {
		store.Close()
	}
}
