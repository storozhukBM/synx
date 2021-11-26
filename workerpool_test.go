package synx

import (
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerPool(t *testing.T) {
	const maxWaitingTime = time.Second
	const shortDelay = 100 * time.Millisecond
	const workers = 3
	wp := NewWorkerPool(workers, maxWaitingTime)

	// submit tasks and grow till `workers` goroutines
	startG := runtime.NumGoroutine()

	// create twice more workers than expected
	for i := 1; i <= 2*workers; i++ {
		wp.Do(func() {
			time.Sleep(shortDelay)
		})

		if wp.CurrentWaitingWorkers() != 0 {
			t.Fatalf("some workers already waiting")
		}

		if n := runtime.NumGoroutine() - startG; n != i {
			t.Fatalf("want %v, got %v", i, n)
		}
	}

	// wait for tasks to finish and check that only max waiting workers are still waiting for new tasks
	time.Sleep(time.Duration(float32(shortDelay.Nanoseconds()) * 1.2))
	waitingWorkers := wp.CurrentWaitingWorkers()
	if waitingWorkers != workers {
		t.Fatalf("want %v, got %v", workers, waitingWorkers)
	}
	if n := runtime.NumGoroutine() - startG; n != workers {
		t.Fatalf("want %v, got %v", workers, n)
	}

	// wait for workers to give up and exit
	time.Sleep(maxWaitingTime)
	waitingWorkers = wp.CurrentWaitingWorkers()
	if waitingWorkers != 0 {
		t.Fatalf("want %v, got %v", 0, waitingWorkers)
	}
	if n := runtime.NumGoroutine() - startG; n != 0 {
		t.Fatalf("want %v, got %v", 0, n)
	}
}

func TestExponentialDeclineOfWaitingTime(t *testing.T) {
	currentlyWorking := int64(0)
	wp := NewWorkerPool(128, 1000*time.Millisecond)
	for i := 0; i < 200; i++ {
		wp.Do(func() {
			atomic.AddInt64(&currentlyWorking, 1)
			defer atomic.AddInt64(&currentlyWorking, -1)
			time.Sleep(5 * time.Millisecond)
		})
	}

	go func() {
		for {
			wp.Do(func() {
				atomic.AddInt64(&currentlyWorking, 1)
				defer atomic.AddInt64(&currentlyWorking, -1)
				time.Sleep(time.Millisecond)
			})
			time.Sleep(5_000 * time.Microsecond)
		}
	}()

	for i := 0; i < 10000; i++ {
		fmt.Printf(
			"%.4d ms: %.4d/%.4d %s\n",
			i, atomic.LoadInt64(&currentlyWorking), wp.CurrentWaitingWorkers(),
			strings.Repeat("*", int(atomic.LoadInt64(&currentlyWorking)))+strings.Repeat("-", (wp.CurrentWaitingWorkers())),
		)
		time.Sleep(time.Millisecond)
	}
}
