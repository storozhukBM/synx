package synx

import (
	"math"
	"sync/atomic"
	"time"
)

type WorkerPool struct {
	maxWaitingWorkers     int32
	maxWaitingTime        time.Duration
	taskQueue             chan func()
	currentWaitingWorkers int32
}

func NewWorkerPool(maxWaitingWorkers int, maxWaitingTime time.Duration) *WorkerPool {
	switch {
	case maxWaitingWorkers < 1 && maxWaitingWorkers < math.MaxInt32:
		panic("synx: maxWaitingWorkers should be between 1 and math.MaxInt32")
	case maxWaitingTime < 1:
		panic("synx: maxWaitingTime should be greater than zero")
	}

	return &WorkerPool{
		maxWaitingWorkers: int32(maxWaitingWorkers),
		maxWaitingTime:    maxWaitingTime,
		taskQueue:         make(chan func()),
	}
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

func (wp *WorkerPool) startWorker(task func()) {
	task() // do the given task

	tick := time.NewTicker(wp.maxWaitingTime)
	defer tick.Stop()

	workerLoop := func() (shouldContinue bool) {
		currentWaitingWorkers := atomic.AddInt32(&wp.currentWaitingWorkers, 1)
		defer atomic.AddInt32(&wp.currentWaitingWorkers, -1)

		if currentWaitingWorkers > wp.maxWaitingWorkers {
			return false // too many workers already waiting
		}

		// try to get task without blocking
		select {
		case t := <-wp.taskQueue:
			t()
			return true
		default:
		}

		// wait for task, but the more workers we have - the shorter wait time gets
		tick.Reset(time.Duration(wp.maxWaitingTime.Nanoseconds() / (int64(currentWaitingWorkers) * int64(currentWaitingWorkers))))
		select {
		case t := <-wp.taskQueue:
			t()
			return true
		case <-tick.C:
			return false
		}
	}

	for {
		if shouldContinue := workerLoop(); !shouldContinue {
			return
		}
	}
}
