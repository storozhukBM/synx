package synx

import (
	"math"
	"sync/atomic"
	"time"
)

// WorkerPool allows you to execute any function in a separate goroutine
// it is designed to act exactly like 'go' operator, with the only difference that
// goroutines after they finish the task, will stay for a bit and wait for additional tasks.
//
// This instrument allows you to reduce the overhead of creating new goroutines
// if your tasks are so small that such overhead can become noticeable.
//
// Another scenario where WorkerPool can help is when your typical task triggers goroutine stack growth,
// and you want to avoid creation and stack expansion repeated over and over.
//
// This WorkerPool is also adaptive. It tracks how many concurrent tasks it runs during adaptationPeriod
// and tries to keep at least that amount of workers ready within the next adaptationPeriod.
//
// WorkerPool has a few knobs that you can configure:
//  - minWorkersInPool - min amount goroutines to keep waiting for new tasks, after its creation
//  - maxWorkersInPool - max amount goroutines to keep waiting for new tasks, after its creation,
//                       note that when you reach this maximum, WorkerPool still accepts new tasks,
//                       but goroutines that work on those tasks won't stay in a pool after task is finished
//  - adaptationPeriod - specifies how often WorkerPool measures the maximum of concurrent tasks and
//                       reconfigures itself for a newly measured targetWorkersInPool
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

// NewWorkerPool creates new WorkerPool with fully adaptive workers size and default adaptationPeriod of 10 seconds
func NewWorkerPool() *WorkerPool {
	return NewWorkerPoolWithOptions(0, math.MaxInt32-1, 10*time.Second)
}

// NewWorkerPoolWithOptions creates new WorkerPool with the specified params:
//  - minWorkersInPool - min amount goroutines to keep waiting for new tasks, after its creation
//  - maxWorkersInPool - max amount goroutines to keep waiting for new tasks, after its creation,
//                       note that when you reach this maximum, WorkerPool still accepts new tasks,
//                       but goroutines that work on those tasks won't stay in a pool after task is finished
//  - adaptationPeriod - specifies how often WorkerPool measures the maximum of concurrent tasks and
//                       reconfigures itself for a newly measured targetWorkersInPool
func NewWorkerPoolWithOptions(minWorkersInPool int, maxWorkersInPool int, adaptationPeriod time.Duration) *WorkerPool {
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

// Do is an equivalent of `go` operator, it never blocks and executes the specified task
// on some already waiting goroutine from a pool or creates a new one
func (wp *WorkerPool) Do(task func()) {
	select {
	case wp.taskQueue <- task:
	default:
		go wp.startWorker(task)
	}
}

// WorkersInPool returns a number of goroutines that are currently members of a pool.
// This number can be higher than CurrentRunningWorkers, because some workers can just execute the task
// and die without joining the pool, if maxWorkersInPool or targetWorkersInPool is already reached
func (wp *WorkerPool) WorkersInPool() int {
	return int(atomic.LoadInt32(&wp.workersInPool))
}

// CurrentRunningWorkers returns a number of workers that execute some task right now
func (wp *WorkerPool) CurrentRunningWorkers() int {
	return int(atomic.LoadInt32(&wp.currentRunningWorkers))
}

// MaxConcurrencyObservedInCycle returns a maximum CurrentRunningWorkers number observed within current adaptation period
func (wp *WorkerPool) MaxConcurrencyObservedInCycle() int {
	return int(atomic.LoadInt32(&wp.maxConcurrencyInCycle))
}

// TargetWorkersInPool returns a current desired number of goroutines in a pool,
// this number gets re-evaluated every adaptation period
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
