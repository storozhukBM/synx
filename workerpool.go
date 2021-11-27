package synx

import (
	"math"
	"sync/atomic"
	"time"
)

type WorkerPool struct {
	adaptationTicker      <-chan time.Time
	adaptationPeriod      time.Duration
	minWorkersInPool      int32
	maxWorkersInPool      int32
	taskQueue             chan func()
	_                     [64 - 8 - 8 - 4 - 4 - 8]byte // padding to avoid false sharing
	targetWorkersInPool   int32
	_                     [64 - 4]byte // padding to avoid false sharing
	maxConcurrencyInCycle int32
	_                     [64 - 4]byte // padding to avoid false sharing
	workersInPool         int32
	_                     [64 - 4]byte // padding to avoid false sharing
	currentRunningWorkers int32
	_                     [64 - 4]byte // padding to avoid false sharing
}

func NewWorkerPool(minWorkersInPool int, maxWorkersInPool int, adaptationPeriod time.Duration) *WorkerPool {
	switch {
	case minWorkersInPool < 0 && minWorkersInPool < math.MaxInt32:
		panic("synx: maxWorkersInPool should be between 0 and math.MaxInt32")
	case maxWorkersInPool < 1 && maxWorkersInPool < math.MaxInt32:
		panic("synx: maxWorkersInPool should be between 1 and math.MaxInt32")
	case adaptationPeriod.Nanoseconds()/time.Millisecond.Nanoseconds() < 1:
		panic("synx: adaptationPeriod should be at least 1 millisecond")
	}

	wp := &WorkerPool{
		adaptationTicker:    time.NewTicker(adaptationPeriod).C,
		adaptationPeriod:    adaptationPeriod,
		minWorkersInPool:    int32(minWorkersInPool),
		maxWorkersInPool:    int32(maxWorkersInPool),
		taskQueue:           make(chan func()),
		targetWorkersInPool: int32(maxWorkersInPool),
	}
	return wp
}

func (wp *WorkerPool) Do(task func()) {
	select {
	case wp.taskQueue <- task:
	default:
		go wp.startWorker(task)
	}
}

func (wp *WorkerPool) WorkersInPool() int {
	return int(atomic.LoadInt32(&wp.workersInPool))
}

func (wp *WorkerPool) CurrentRunningWorkers() int {
	return int(atomic.LoadInt32(&wp.currentRunningWorkers))
}

func (wp *WorkerPool) MaxConcurrencyObservedInCycle() int {
	return int(atomic.LoadInt32(&wp.maxConcurrencyInCycle))
}

func (wp *WorkerPool) TargetWorkersInPool() int {
	return int(atomic.LoadInt32(&wp.targetWorkersInPool))
}

func (wp *WorkerPool) startWorker(task func()) {
	wp.run(task) // do the given task

	if ok := wp.tryToEnterPool(); !ok {
		return
	}
	workerTick := time.NewTicker(wp.adaptationPeriod)
	defer workerTick.Stop()
	for {
		wp.executeOneCycleOfWorkerLoop(workerTick)
		if ok := wp.tryToLeavePool(); ok {
			return
		}
	}
}

func (wp *WorkerPool) run(task func()) {
	runningWorkers := atomic.AddInt32(&wp.currentRunningWorkers, 1)
	defer atomic.AddInt32(&wp.currentRunningWorkers, -1)
	for {
		maxWorkersRunning := atomic.LoadInt32(&wp.maxConcurrencyInCycle)
		if runningWorkers <= maxWorkersRunning {
			break
		}
		ok := atomic.CompareAndSwapInt32(&wp.maxConcurrencyInCycle, maxWorkersRunning, runningWorkers)
		if ok {
			break
		}
	}
	task()
}

func (wp *WorkerPool) tryToEnterPool() bool {
	for {
		workersInPool := atomic.LoadInt32(&wp.workersInPool)
		targetWorkersInPool := atomic.LoadInt32(&wp.targetWorkersInPool)
		if workersInPool == 0 {
			// if there are no workers in the pool, maybe there is no one to run the adaptation routine
			select {
			case <-wp.adaptationTicker:
				wp.runAdaptation()
			default:
			}
		}
		if workersInPool+1 > wp.maxWorkersInPool || workersInPool+1 > targetWorkersInPool {
			return false // too many workers already waiting
		}
		ok := atomic.CompareAndSwapInt32(&wp.workersInPool, workersInPool, workersInPool+1)
		if ok {
			return true // we are OK to stay in a pool
		}
	}
}

func (wp *WorkerPool) tryToLeavePool() bool {
	for {
		target := atomic.LoadInt32(&wp.targetWorkersInPool)
		workersInPool := atomic.LoadInt32(&wp.workersInPool)
		if workersInPool-1 < wp.minWorkersInPool || workersInPool-1 < target {
			return false // we should stay and wait for more tasks
		}
		ok := atomic.CompareAndSwapInt32(&wp.workersInPool, workersInPool, workersInPool-1)
		if ok {
			return true // we are OK to leave
		}
	}
}

func (wp *WorkerPool) executeOneCycleOfWorkerLoop(workerTick *time.Ticker) {
	select {
	case t := <-wp.taskQueue:
		wp.run(t)
		return
	default:
	}

	workerTick.Reset(wp.adaptationPeriod)
	select {
	case t := <-wp.taskQueue:
		wp.run(t)
	case <-wp.adaptationTicker:
		wp.runAdaptation()
	case <-workerTick.C:
	}
}

func (wp *WorkerPool) runAdaptation() {
	prevTargetOfWorkersInPool := atomic.LoadInt32(&wp.targetWorkersInPool)
	maxConcurrencyInCycle := atomic.SwapInt32(&wp.maxConcurrencyInCycle, 0)
	if maxConcurrencyInCycle < prevTargetOfWorkersInPool {
		// if we decrease the target, we want to do it not at once but linearly every cycle
		maxConcurrencyInCycle = (maxConcurrencyInCycle + prevTargetOfWorkersInPool) / 2
	}
	if maxConcurrencyInCycle < wp.minWorkersInPool {
		maxConcurrencyInCycle = wp.minWorkersInPool
	}
	atomic.StoreInt32(&wp.targetWorkersInPool, maxConcurrencyInCycle)
}
