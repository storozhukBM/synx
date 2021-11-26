package synx

import (
	"math"
	"sync/atomic"
	"time"
)

type WorkerPool struct {
	minWaitingWorkers             int32
	maxWaitingWorkers             int32
	reConfigurationPeriodDuration time.Duration
	taskQueue                     chan func()

	_                                       [64]byte // padding to avoid false sharing
	maxConcurrentWorkersWithinCurrentPeriod int32
	targetWaitingWorkers                    int32

	_             [64]byte // padding to avoid false sharing
	workersInPool int32

	_                     [64]byte // padding to avoid false sharing
	currentRunningWorkers int32
}

func NewWorkerPool(minWaitingWorkers int, maxWaitingWorkers int, reConfigurationPeriodDuration time.Duration) *WorkerPool {
	switch {
	case minWaitingWorkers < 0 && minWaitingWorkers < math.MaxInt32:
		panic("synx: maxWaitingWorkers should be between 0 and math.MaxInt32")
	case maxWaitingWorkers < 1 && maxWaitingWorkers < math.MaxInt32:
		panic("synx: maxWaitingWorkers should be between 1 and math.MaxInt32")
	case reConfigurationPeriodDuration.Nanoseconds()/time.Millisecond.Nanoseconds() < 1:
		panic("synx: reConfigurationPeriodDuration should be at least 1 millisecond")
	}

	wp := &WorkerPool{
		minWaitingWorkers:             int32(minWaitingWorkers),
		maxWaitingWorkers:             int32(maxWaitingWorkers),
		reConfigurationPeriodDuration: reConfigurationPeriodDuration,
		taskQueue:                     make(chan func()),
		targetWaitingWorkers:          int32(maxWaitingWorkers),
	}
	go func() {
		for {
			//TODO: use context here to free resources
			time.Sleep(reConfigurationPeriodDuration)
			prevTarget := atomic.LoadInt32(&wp.targetWaitingWorkers)
			maxConcurrentWorkers := atomic.SwapInt32(&wp.maxConcurrentWorkersWithinCurrentPeriod, 0)
			if maxConcurrentWorkers < prevTarget {
				maxConcurrentWorkers = (maxConcurrentWorkers + prevTarget) / 2
			}
			if maxConcurrentWorkers < int32(minWaitingWorkers) {
				maxConcurrentWorkers = int32(minWaitingWorkers)
			}
			atomic.StoreInt32(&wp.targetWaitingWorkers, maxConcurrentWorkers)
		}
	}()
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

func (wp *WorkerPool) MaxConcurrentWorkersWithinCurrentPeriod() int {
	return int(atomic.LoadInt32(&wp.maxConcurrentWorkersWithinCurrentPeriod))
}

func (wp *WorkerPool) TargetWaitingWorkers() int {
	return int(atomic.LoadInt32(&wp.targetWaitingWorkers))
}

func (wp *WorkerPool) run(task func()) {
	runningWorkers := atomic.AddInt32(&wp.currentRunningWorkers, 1)
	defer atomic.AddInt32(&wp.currentRunningWorkers, -1)
	for {
		maxWorkersRunning := atomic.LoadInt32(&wp.maxConcurrentWorkersWithinCurrentPeriod)
		if runningWorkers <= maxWorkersRunning {
			break
		}
		ok := atomic.CompareAndSwapInt32(&wp.maxConcurrentWorkersWithinCurrentPeriod, maxWorkersRunning, runningWorkers)
		if ok {
			break
		}
	}
	task()
}

func (wp *WorkerPool) startWorker(task func()) {
	wp.run(task) // do the given task

	tick := time.NewTicker(wp.reConfigurationPeriodDuration)
	defer tick.Stop()

	workerLoop := func() {
		// try to get task without blocking
		select {
		case t := <-wp.taskQueue:
			wp.run(t)
			return
		default:
		}

		tick.Reset(wp.reConfigurationPeriodDuration)
		select {
		case t := <-wp.taskQueue:
			wp.run(t)
			return
		case <-tick.C:
			return
		}
	}

	for {
		currentWaitingWorkers := atomic.LoadInt32(&wp.workersInPool)
		if currentWaitingWorkers+1 > wp.maxWaitingWorkers || currentWaitingWorkers+1 > atomic.LoadInt32(&wp.targetWaitingWorkers) {
			return // too many workers already waiting
		}
		ok := atomic.CompareAndSwapInt32(&wp.workersInPool, currentWaitingWorkers, currentWaitingWorkers+1)
		if ok {
			break
		}
	}

loop:
	for {
		workerLoop()

		target := atomic.LoadInt32(&wp.targetWaitingWorkers)
		for {
			ww := atomic.LoadInt32(&wp.workersInPool)
			if ww-1 < wp.minWaitingWorkers || ww-1 < target {
				continue loop
			}
			ok := atomic.CompareAndSwapInt32(&wp.workersInPool, ww, ww-1)
			if ok {
				break loop
			}
		}
	}
}
