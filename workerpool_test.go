package synx

import (
	"fmt"
	"math"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestWorkerPool(t *testing.T) {
	const statsRefreshPeriod = time.Second / 3
	const shortDelay = 100 * time.Millisecond
	const workers = 3
	wp := NewWorkerPoolWithOptions(0, workers, statsRefreshPeriod)

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
	wp := NewWorkerPoolWithOptions(5, 128, 10*time.Millisecond)
	go func() {
		for {
			wp.Do(func() {
				time.Sleep(1 * time.Millisecond)
			})
			verticalShift := 20_000.0
			amplitude := 17_000.0
			periodLength := 75.0
			d := time.Duration(
				verticalShift +
					amplitude*
						math.Sin(
							float64(time.Now().UnixMilli())*(2*math.Pi/periodLength),
						),
			)
			time.Sleep(d)
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
			i, wp.CurrentRunningWorkers(), wp.WorkersInPool(), wp.MaxConcurrencyObservedInCycle(), wp.TargetWorkersInPool(),
			bar,
		)
		time.Sleep(time.Millisecond)
	}
}
