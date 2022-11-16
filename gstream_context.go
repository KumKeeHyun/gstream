package gstream

import (
	"sync"
)

func newGStreamContext() *gstreamContext {
	return &gstreamContext{
		routineGroup:     sync.WaitGroup{},
		processorContext: newProcessorContext(),
	}
}

type gstreamContext struct {
	processorContext
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

func newProcessorContext() processorContext {
	return processorContext{
		mu:         sync.Mutex{},
		closeChans: map[GStreamID]closeChanFunc{},
		refers:     map[GStreamID][]GStreamID{},
		referCnts:  map[GStreamID]int{},
	}
}

type closeChanFunc func()

type processorContext struct {
	mu         sync.Mutex
	closeChans map[GStreamID]closeChanFunc
	refers     map[GStreamID][]GStreamID
	referCnts  map[GStreamID]int
}

func (c *processorContext) addCloseChan(routineID, childID GStreamID, closeChan closeChanFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closeChans[childID] = closeChan
	c.refers[routineID] = append(c.refers[routineID], childID)
	cnt := c.referCnts[childID]
	c.referCnts[childID] = cnt + 1
}

func (c *processorContext) tryCloseChans(routineID GStreamID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	refers, exists := c.refers[routineID]
	if !exists {
		return
	}
	for _, childID := range refers {
		cnt := c.referCnts[childID]
		if cnt <= 1 {
			if closeChan, exists := c.closeChans[childID]; exists {
				closeChan()
			}
			delete(c.closeChans, childID)
		}
		c.referCnts[childID] = cnt - 1
	}
}
