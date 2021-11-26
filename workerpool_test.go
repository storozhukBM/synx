package synx

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestWorkerPool(t *testing.T) {
	const statsRefreshPeriod = time.Second / 3
	const shortDelay = 100 * time.Millisecond
	const workers = 3
	wp := NewWorkerPool(0, workers, statsRefreshPeriod)

	// submit tasks and grow till `workers` goroutines
	startG := runtime.NumGoroutine()

	// create twice more workers than expected
	for i := 1; i <= 2*workers; i++ {
		wp.Do(func() {
			time.Sleep(shortDelay)
		})

		if wp.WorkersInPool() != 0 {
			t.Fatalf("some workers already waiting")
		}

		if n := runtime.NumGoroutine() - startG; n != i {
			t.Fatalf("want %v, got %v", i, n)
		}
	}

	// wait for tasks to finish and check that only max waiting workers are still waiting for new tasks
	time.Sleep(time.Duration(float32(shortDelay.Nanoseconds()) * 1.2))
	waitingWorkers := wp.WorkersInPool()
	if waitingWorkers != workers {
		t.Fatalf("want %v, got %v", workers, waitingWorkers)
	}
	if n := runtime.NumGoroutine() - startG; n != workers {
		t.Fatalf("want %v, got %v", workers, n)
	}

	// wait for workers to give up and exit
	time.Sleep(4 * statsRefreshPeriod)
	waitingWorkers = wp.WorkersInPool()
	if waitingWorkers != 0 {
		t.Fatalf("want %v, got %v", 0, waitingWorkers)
	}
	if n := runtime.NumGoroutine() - startG; n != 0 {
		t.Fatalf("want %v, got %v", 0, n)
	}
}

func TestExponentialDeclineOfWaitingTime(t *testing.T) {
	wp := NewWorkerPool(5, 128, 20*time.Millisecond)
	for i := 0; i < 200; i++ {
		wp.Do(func() {
			time.Sleep(5 * time.Millisecond)
		})
	}
	go func() {
		for {
			wp.Do(func() {
				time.Sleep(1 * time.Millisecond)
			})
			time.Sleep(1000 * time.Microsecond)
		}
	}()
	for i := 0; i < 1000; i++ {
		bar := strings.Repeat("*", wp.CurrentRunningWorkers())
		workersWaiting := wp.WorkersInPool() - wp.CurrentRunningWorkers()
		if workersWaiting > 0 {
			bar = bar + strings.Repeat("-", workersWaiting)
		}
		fmt.Printf(
			"%.4d ms: [%.4d/%.4d] (%.4d|%.4d) %s\n",
			i, wp.CurrentRunningWorkers(), wp.WorkersInPool(), wp.MaxConcurrentWorkersWithinCurrentPeriod(), wp.TargetWaitingWorkers(),
			bar,
		)
		time.Sleep(time.Millisecond)
	}
}
