// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Pool is allowed to drop values under the race detector, so these tests mirror
// the standard library's sync.Pool tests and run only without -race.
//
//go:build !race

package syncx_test

import (
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/kamiertop/syncx"
)

func TestPool(t *testing.T) {
	defer debug.SetGCPercent(debug.SetGCPercent(-1))

	var p Pool[string]
	if got := p.Get(); got != "" {
		t.Fatalf("got %#v; want empty string", got)
	}

	func() {
		// Make sure the goroutine doesn't migrate to another P
		// between Put and Get calls.
		Runtime_procPin()
		defer Runtime_procUnpin()

		p.Put("a")
		p.Put("b")
		if got := p.Get(); got != "a" {
			t.Fatalf("got %#v; want a", got)
		}
		if got := p.Get(); got != "b" {
			t.Fatalf("got %#v; want b", got)
		}
		if got := p.Get(); got != "" {
			t.Fatalf("got %#v; want empty string", got)
		}
	}()

	// Put in enough objects that they spill into stealable space.
	for i := 0; i < 100; i++ {
		p.Put("c")
	}
	// After one GC, the victim cache should keep them alive.
	runtime.GC()
	if got := p.Get(); got != "c" {
		t.Fatalf("got %#v; want c after GC", got)
	}
	// A second GC should drop the victim cache.
	runtime.GC()
	if got := p.Get(); got != "" {
		t.Fatalf("got %#v; want empty string after second GC", got)
	}
}

func TestPoolNew(t *testing.T) {
	defer debug.SetGCPercent(debug.SetGCPercent(-1))

	i := 0
	p := Pool[int]{
		New: func() int {
			i++
			return i
		},
	}
	if got := p.Get(); got != 1 {
		t.Fatalf("got %v; want 1", got)
	}
	if got := p.Get(); got != 2 {
		t.Fatalf("got %v; want 2", got)
	}

	func() {
		// Make sure the goroutine doesn't migrate to another P
		// between Put and Get calls.
		Runtime_procPin()
		defer Runtime_procUnpin()

		p.Put(42)
		if got := p.Get(); got != 42 {
			t.Fatalf("got %v; want 42", got)
		}
	}()

	if got := p.Get(); got != 3 {
		t.Fatalf("got %v; want 3", got)
	}
}

func TestPoolZeroValue(t *testing.T) {
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	calls := 0
	p := Pool[int]{
		New: func() int {
			calls++
			return 99
		},
	}

	p.Put(0)
	if got := p.Get(); got != 0 {
		t.Fatalf("got %v; want pooled zero value", got)
	}
	if calls != 0 {
		t.Fatalf("New called %d times; want 0", calls)
	}

	if got := p.Get(); got != 99 {
		t.Fatalf("got %v; want New value", got)
	}
	if calls != 1 {
		t.Fatalf("New called %d times; want 1", calls)
	}
}

func TestPoolDifferentTypesSurviveGC(t *testing.T) {
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	var ints Pool[int]
	var strings Pool[string]
	ints.Put(1)
	strings.Put("one")

	runtime.GC()
	if got := ints.Get(); got != 1 {
		t.Fatalf("int pool got %v; want 1 after GC", got)
	}
	if got := strings.Get(); got != "one" {
		t.Fatalf("string pool got %q; want one after GC", got)
	}
}

func TestPoolGC(t *testing.T) {
	testPoolFinalizers(t, true)
}

func TestPoolRelease(t *testing.T) {
	testPoolFinalizers(t, false)
}

type poolFinalizerValue struct {
	id int
}

func testPoolFinalizers(t *testing.T, drain bool) {
	if drain {
		defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	}

	var p Pool[*poolFinalizerValue]
	const n = 100
	for try := 0; try < 3; try++ {
		if try == 1 && testing.Short() {
			break
		}

		var finalized atomic.Int32
		for i := 0; i < n; i++ {
			v := &poolFinalizerValue{id: i}
			runtime.SetFinalizer(v, func(*poolFinalizerValue) {
				finalized.Add(1)
			})
			p.Put(v)
		}
		if drain {
			for i := 0; i < n; i++ {
				p.Get()
			}
		} else {
			// Move primary cache to victim cache before the collection loop
			// drops it, matching the standard library release test.
			runtime.GC()
		}

		deadline := time.Now().Add(5 * time.Second)
		for finalized.Load() < n-1 && time.Now().Before(deadline) {
			runtime.GC()
			time.Sleep(10 * time.Millisecond)
		}
		if got := finalized.Load(); got < n-1 {
			t.Fatalf("only %d out of %d resources were finalized on try %d", got, n, try)
		}
	}
}

func TestPoolStress(t *testing.T) {
	const goroutines = 10
	n := 100000
	if testing.Short() {
		n = 1000
	}

	var p Pool[int]
	done := make(chan bool)
	for i := 0; i < goroutines; i++ {
		go func() {
			v := 0
			for j := 0; j < n; j++ {
				p.Put(v)
				v = p.Get()
				if v != 0 {
					t.Errorf("got %v; want 0", v)
					break
				}
			}
			done <- true
		}()
	}
	for i := 0; i < goroutines; i++ {
		<-done
	}
}

func TestPoolDequeue(t *testing.T) {
	testPoolDequeue(t, NewPoolDequeue[int](16))
}

func TestPoolChain(t *testing.T) {
	testPoolDequeue(t, NewPoolChain[int]())
}

func testPoolDequeue(t *testing.T, d PoolDequeue[int]) {
	const goroutines = 10
	n := 100000
	if testing.Short() {
		n = 1000
	}

	have := make([]int32, n)
	var stop atomic.Int32
	var wg sync.WaitGroup
	record := func(val int) {
		if val < 0 || val >= n {
			t.Errorf("got out-of-range value %d", val)
			stop.Store(1)
			return
		}
		atomic.AddInt32(&have[val], 1)
		if val == n-1 {
			stop.Store(1)
		}
	}

	// Start consumers.
	for i := 1; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fail := 0
			for stop.Load() == 0 {
				val, ok := d.PopTail()
				if ok {
					fail = 0
					record(val)
					continue
				}
				if fail++; fail%100 == 0 {
					runtime.Gosched()
				}
			}
		}()
	}

	nPopHead := atomic.Int32{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < n; j++ {
			for !d.PushHead(j) {
				runtime.Gosched()
			}
			if j%10 == 0 {
				val, ok := d.PopHead()
				if ok {
					nPopHead.Add(1)
					record(val)
				}
			}
		}
	}()
	wg.Wait()

	for i, count := range have {
		if count != 1 {
			t.Errorf("have[%d] = %d; want 1", i, count)
		}
	}
	if !testing.Short() && nPopHead.Load() == 0 {
		t.Error("popHead never succeeded")
	}
}

func TestPoolDequeueZeroValue(t *testing.T) {
	d := NewPoolDequeue[int](2)
	if !d.PushHead(0) {
		t.Fatal("pushHead failed")
	}
	if got, ok := d.PopTail(); !ok || got != 0 {
		t.Fatalf("popTail = %v, %v; want 0, true", got, ok)
	}
	if got, ok := d.PopTail(); ok || got != 0 {
		t.Fatalf("empty popTail = %v, %v; want 0, false", got, ok)
	}
}

func TestNilPool(t *testing.T) {
	catch := func() {
		if recover() == nil {
			t.Error("expected panic")
		}
	}

	var p *Pool[string]
	t.Run("Get", func(t *testing.T) {
		defer catch()
		_ = p.Get()
		t.Error("should have panicked already")
	})
	t.Run("Put", func(t *testing.T) {
		defer catch()
		p.Put("a")
		t.Error("should have panicked already")
	})
}

func BenchmarkPool(b *testing.B) {
	var p Pool[int]
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p.Put(1)
			p.Get()
		}
	})
}

func BenchmarkPoolOverflow(b *testing.B) {
	var p Pool[int]
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := 0; i < 100; i++ {
				p.Put(1)
			}
			for i := 0; i < 100; i++ {
				p.Get()
			}
		}
	})
}
