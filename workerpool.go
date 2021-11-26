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

	_                     [64]byte // padding to avoid false sharing
	currentWaitingWorkers int32

	_                     [64]byte // padding to avoid false sharing
	currentRunningWorkers int32
}

func NewWorkerPool(minWaitingWorkers int, maxWaitingWorkers int, reConfigurationPeriodDuration time.Duration) *WorkerPool {
	switch {
	case minWaitingWorkers < 0 && minWaitingWorkers < math.MaxInt32:
		panic("synx: maxWaitingWorkers should be between 0 and math.MaxInt32")
	case maxWaitingWorkers < 1 && maxWaitingWorkers < math.MaxInt32:
		panic("synx: maxWaitingWorkers should be between 1 and math.MaxInt32")
	case reConfigurationPeriodDuration.Nanoseconds()/time.Second.Nanoseconds() < 1:
		panic("synx: reConfigurationPeriodDuration should be at least 1 second")
	}

	wp := &WorkerPool{
		minWaitingWorkers:             int32(minWaitingWorkers),
		maxWaitingWorkers:             int32(maxWaitingWorkers),
		reConfigurationPeriodDuration: reConfigurationPeriodDuration,
		taskQueue:                     make(chan func()),
		targetWaitingWorkers:          int32(maxWaitingWorkers),
	}
	go func() {
		time.Sleep(reConfigurationPeriodDuration)
		maxConcurrentWorkers := atomic.SwapInt32(&wp.maxConcurrentWorkersWithinCurrentPeriod, 0)
		atomic.StoreInt32(&wp.targetWaitingWorkers, maxConcurrentWorkers)
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

func (wp *WorkerPool) CurrentWaitingWorkers() int {
	return int(atomic.LoadInt32(&wp.currentWaitingWorkers))
}

func (wp *WorkerPool) run(task func()) {
	task()
}

func (wp *WorkerPool) startWorker(task func()) {
	wp.run(task) // do the given task

	tick := time.NewTicker(wp.reConfigurationPeriodDuration)
	defer tick.Stop()

	workerLoop := func() (shouldContinue bool) {
		currentWaitingWorkers := atomic.AddInt32(&wp.currentWaitingWorkers, 1)
		defer atomic.AddInt32(&wp.currentWaitingWorkers, -1)

		if currentWaitingWorkers > wp.maxWaitingWorkers || currentWaitingWorkers > atomic.LoadInt32(&wp.targetWaitingWorkers) {
			return false // too many workers already waiting
		}

		// try to get task without blocking
		select {
		case t := <-wp.taskQueue:
			wp.run(t)
			return true
		default:
		}

		tick.Reset(wp.reConfigurationPeriodDuration)
		select {
		case t := <-wp.taskQueue:
			wp.run(t)
			return true
		case <-tick.C:
			if atomic.LoadInt32(&wp.currentWaitingWorkers) <= wp.minWaitingWorkers {
				return true
			}
			return false
		}
	}

	for {
		if shouldContinue := workerLoop(); !shouldContinue {
			return
		}
	}
}
