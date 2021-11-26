package synx

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type taskQueue struct {
	_                       [64]byte
	taskPointer             unsafe.Pointer
	_                       [64]byte
	taskWaitingMutex        *sync.Mutex
	taskCond                *sync.Cond
	schedulerCondition      *sync.Cond
	currentlyWaitingWorkers int
}

func newQueue(maxWaiting time.Duration) *taskQueue {
	taskWaitingMutex := &sync.Mutex{}
	result := &taskQueue{taskWaitingMutex: taskWaitingMutex, taskCond: sync.NewCond(taskWaitingMutex), schedulerCondition: sync.NewCond(taskWaitingMutex)}
	result.scheduleWakeupPeriods(maxWaiting)
	return result
}

func (tq *taskQueue) scheduleWakeupPeriods(maxWaiting time.Duration) {
	go func() {
		for {
			fmt.Println("wakeup scheduler is working")
			time.Sleep(maxWaiting / 1000)
			tq.taskCond.Broadcast()

			tq.taskWaitingMutex.Lock()
			if tq.currentlyWaitingWorkers == 0 {
				tq.schedulerCondition.Wait()
			}
			tq.taskWaitingMutex.Unlock()
		}
	}()
}

func (tq *taskQueue) put(f func()) bool {
	ok := atomic.CompareAndSwapPointer(&tq.taskPointer, nil, unsafe.Pointer(&f))
	if ok {
		tq.taskCond.Signal()
		return true
	}
	return false
}

func (tq *taskQueue) pull(maxWaitingTime time.Duration) (func(), bool) {
	pullFinish := time.Now().Add(maxWaitingTime)
	for time.Now().Before(pullFinish) {
		taskPointer := atomic.SwapPointer(&tq.taskPointer, nil)
		if taskPointer != nil {
			typedTaskPointer := (*func())(taskPointer)
			return *typedTaskPointer, true
		}

		tq.taskWaitingMutex.Lock()
		tq.currentlyWaitingWorkers++
		tq.schedulerCondition.Signal()
		tq.taskCond.Wait()
		tq.currentlyWaitingWorkers--
		tq.taskWaitingMutex.Unlock()
	}
	return nil, false
}

type WorkerPool struct {
	maxWaitingWorkers     int32
	maxWaitingTime        time.Duration
	taskQueue             *taskQueue
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
		taskQueue:         newQueue(maxWaitingTime),
	}
}

func (wp *WorkerPool) Do(task func()) {
	ok := wp.taskQueue.put(task)
	if ok {
		return
	}
	go wp.startWorker(task)
}

func (wp *WorkerPool) CurrentWaitingWorkers() int {
	return int(atomic.LoadInt32(&wp.currentWaitingWorkers))
}

func (wp *WorkerPool) startWorker(task func()) {
	task() // do the given task

	workerLoop := func() (shouldContinue bool) {
		currentWaitingWorkers := atomic.AddInt32(&wp.currentWaitingWorkers, 1)
		defer atomic.AddInt32(&wp.currentWaitingWorkers, -1)

		if currentWaitingWorkers > wp.maxWaitingWorkers {
			return false // too many workers already waiting
		}

		task, ok := wp.taskQueue.pull(wp.maxWaitingTime)
		if ok {
			task()
			return true
		}
		return false
	}

	for {
		if shouldContinue := workerLoop(); !shouldContinue {
			return
		}
	}
}
