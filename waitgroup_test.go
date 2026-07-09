package syncx_test

import (
	"testing"
	"testing/synctest"

	"github.com/kamiertop/syncx"
)

func TestWaitGroupCounter(t *testing.T) {
	var wg syncx.WaitGroup

	if got := wg.Counter(); got != 0 {
		t.Fatalf("Counter() = %d, want 0", got)
	}

	wg.Add(2)
	if got := wg.Counter(); got != 2 {
		t.Fatalf("Counter() after Add(2) = %d, want 2", got)
	}

	wg.Done()
	if got := wg.Counter(); got != 1 {
		t.Fatalf("Counter() after Done() = %d, want 1", got)
	}

	wg.Done()
	wg.Wait()
	if got := wg.Counter(); got != 0 {
		t.Fatalf("Counter() after Wait() = %d, want 0", got)
	}
}

func TestWaitGroupWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var wg syncx.WaitGroup
		const waiters = 3

		wg.Add(1)
		done := make(chan struct{}, waiters)
		for i := 0; i < waiters; i++ {
			go func() {
				wg.Wait()
				done <- struct{}{}
			}()
		}

		synctest.Wait()
		if got := wg.Waiter(); got != waiters {
			t.Fatalf("Waiter() = %d, want %d", got, waiters)
		}

		if got := wg.Counter(); got != 1 {
			t.Fatalf("Counter() while waiters blocked = %d, want 1", got)
		}

		wg.Done()
		synctest.Wait()
		if gotDone := drain(done); gotDone != waiters {
			t.Fatalf("Wait returned %d times, want %d", gotDone, waiters)
		}

		if got := wg.Waiter(); got != 0 {
			t.Fatalf("Waiter() after release = %d, want 0", got)
		}
	})
}

func drain(ch <-chan struct{}) int {
	n := 0
	for {
		select {
		case <-ch:
			n++
		default:
			return n
		}
	}
}
