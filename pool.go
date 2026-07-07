// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package syncx

import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// A Pool is a set of temporary objects that may be individually saved and
// retrieved.
//
// Any item stored in the Pool may be removed automatically at any time without
// notification. If the Pool holds the only reference when this happens, the
// item might be deallocated.
//
// A Pool is safe for use by multiple goroutines simultaneously.
//
// Pool's purpose is to cache allocated but unused items for later reuse,
// relieving pressure on the garbage collector. That is, it makes it easy to
// build efficient, thread-safe free lists. However, it is not suitable for all
// free lists.
//
// An appropriate use of a Pool is to manage a group of temporary items
// silently shared among and potentially reused by concurrent independent
// clients of a package. Pool provides a way to amortize allocation overhead
// across many clients.
//
// An example of good use of a Pool is in the fmt package, which maintains a
// dynamically-sized store of temporary output buffers. The store scales under
// load (when many goroutines are actively printing) and shrinks when
// quiescent.
//
// On the other hand, a free list maintained as part of a short-lived object is
// not a suitable use for a Pool, since the overhead does not amortize well in
// that scenario. It is more efficient to have such objects implement their own
// free list.
//
// A Pool must not be copied after first use.
//
// In the terminology of [the Go memory model], a call to Put(x) “synchronizes before”
// a call to [Pool.Get] returning that same value x.
// Similarly, a call to New returning x “synchronizes before”
// a call to Get returning that same value x.
//
// [the Go memory model]: https://go.dev/ref/mem
type Pool[T any] struct {
	noCopy noCopy

	local     unsafe.Pointer // local fixed-size per-P pool, actual type is [P]poolLocal[T]
	localSize uintptr        // size of the local array

	victim     unsafe.Pointer // local from previous cycle
	victimSize uintptr        // size of victims array

	// New optionally specifies a function to generate
	// a value when Get would otherwise return the zero value.
	// It may not be changed concurrently with calls to Get.
	New func() T
}

// Local per-P Pool appendix.
type poolLocalInternal[T any] struct {
	private    T            // Can be used only by the respective P.
	privateSet bool         // Reports whether private contains a pooled value.
	shared     poolChain[T] // Local P can pushHead/popHead; any P can popTail.
}

type poolLocal[T any] struct {
	poolLocalInternal[T]

	// Reduce false sharing between per-P shards. The exact size of
	// poolLocalInternal depends on T, so keep a full cache-line pad.
	pad [128]byte
}

// Put adds x to the pool.
func (p *Pool[T]) Put(x T) {
	l, _ := p.pin()
	if !l.privateSet {
		l.private = x
		l.privateSet = true
	} else {
		l.shared.pushHead(x)
	}
	runtime_procUnpin()
}

// Get selects an arbitrary item from the [Pool], removes it from the
// Pool, and returns it to the caller.
// Get may choose to ignore the pool and treat it as empty.
// Callers should not assume any relation between values passed to [Pool.Put] and
// the values returned by Get.
//
// If Get would otherwise return the zero value and p.New is non-nil, Get returns
// the result of calling p.New.
func (p *Pool[T]) Get() T {
	l, pid := p.pin()
	x := l.private
	ok := l.privateSet
	if ok {
		var zero T
		l.private = zero
		l.privateSet = false
	} else {
		// Try to pop the head of the local shard. We prefer
		// the head over the tail for temporal locality of
		// reuse.
		x, ok = l.shared.popHead()
		if !ok {
			x, ok = p.getSlow(pid)
		}
	}
	runtime_procUnpin()
	if !ok && p.New != nil {
		x = p.New()
	}

	return x
}

func (p *Pool[T]) getSlow(pid int) (T, bool) {
	// See the comment in pin regarding ordering of the loads.
	size := runtime_LoadAcquintptr(&p.localSize) // load-acquire
	locals := p.local                            // load-consume
	// Try to steal one element from other procs.
	for i := 0; i < int(size); i++ {
		l := indexLocal[T](locals, (pid+i+1)%int(size))
		if x, ok := l.shared.popTail(); ok {
			return x, true
		}
	}

	// Try the victim cache. We do this after attempting to steal
	// from all primary caches because we want objects in the
	// victim cache to age out if at all possible.
	size = atomic.LoadUintptr(&p.victimSize)
	if uintptr(pid) >= size {
		var zero T
		return zero, false
	}
	locals = p.victim
	l := indexLocal[T](locals, pid)
	if l.privateSet {
		x := l.private
		var zero T
		l.private = zero
		l.privateSet = false
		return x, true
	}
	for i := 0; i < int(size); i++ {
		l := indexLocal[T](locals, (pid+i)%int(size))
		if x, ok := l.shared.popTail(); ok {
			return x, true
		}
	}

	// Mark the victim cache as empty for future gets don't bother
	// with it.
	atomic.StoreUintptr(&p.victimSize, 0)

	var zero T

	return zero, false
}

// pin pins the current goroutine to P, disables preemption and
// returns poolLocal pool for the P and the P's id.
// Caller must call runtime_procUnpin() when done with the pool.
func (p *Pool[T]) pin() (*poolLocal[T], int) {
	// Check whether p is nil to get a panic.
	// Otherwise the nil dereference happens while the m is pinned,
	// causing a fatal error rather than a panic.
	if p == nil {
		panic("nil Pool")
	}

	pid := runtime_procPin()
	// In pinSlow we store to local and then to localSize, here we load in opposite order.
	// Since we've disabled preemption, GC cannot happen in between.
	// Thus here we must observe local at least as large localSize.
	// We can observe a newer/larger local, it is fine (we must observe its zero-initialized-ness).
	s := runtime_LoadAcquintptr(&p.localSize) // load-acquire
	l := p.local                              // load-consume
	if uintptr(pid) < s {
		return indexLocal[T](l, pid), pid
	}

	return p.pinSlow()
}

func (p *Pool[T]) pinSlow() (*poolLocal[T], int) {
	// Retry under the mutex.
	// Can not lock the mutex while pinned.
	runtime_procUnpin()
	allPoolsMu.Lock()
	defer allPoolsMu.Unlock()
	pid := runtime_procPin()
	// poolCleanup won't be called while we are pinned.
	s := p.localSize
	l := p.local
	if uintptr(pid) < s {
		return indexLocal[T](l, pid), pid
	}
	if p.local == nil {
		allPools = append(allPools, p)
	}
	// If GOMAXPROCS changes between GCs, we re-allocate the array and lose the old one.
	size := runtime.GOMAXPROCS(0)
	local := make([]poolLocal[T], size)
	atomic.StorePointer(&p.local, unsafe.Pointer(&local[0])) // store-release
	runtime_StoreReluintptr(&p.localSize, uintptr(size))     // store-release

	return &local[pid], pid
}

// poolCleanup should be an internal detail,
// but widely used packages access it using linkname.
// Notable members of the hall of shame include:
//   - github.com/bytedance/gopkg
//   - github.com/songzhibin97/gkit
//
// Do not remove or change the type signature.
// See go.dev/issue/67401.
//
//go:linkname poolCleanup
func poolCleanup() {
	// This function is called with the world stopped, at the beginning of a garbage collection.
	// It must not allocate and probably should not call any runtime functions.

	// Because the world is stopped, no pool user can be in a
	// pinned section (in effect, this has all Ps pinned).

	// Drop victim caches from all pools.
	for _, p := range oldPools {
		p.dropVictim()
	}

	// Move primary cache to victim cache.
	for _, p := range allPools {
		p.movePrimaryToVictim()
	}

	// The pools with non-empty primary caches now have non-empty
	// victim caches and no pools have primary caches.
	oldPools, allPools = allPools, nil
}

type poolCleanupable interface {
	dropVictim()
	movePrimaryToVictim()
}

func (p *Pool[T]) dropVictim() {
	p.victim = nil
	p.victimSize = 0
}

func (p *Pool[T]) movePrimaryToVictim() {
	p.victim = p.local
	p.victimSize = p.localSize
	p.local = nil
	p.localSize = 0
}

var (
	allPoolsMu sync.Mutex

	// allPools is the set of pools that have non-empty primary
	// caches. Protected by either 1) allPoolsMu and pinning or 2)
	// STW.
	allPools []poolCleanupable

	// oldPools is the set of pools that may have non-empty victim
	// caches. Protected by STW.
	oldPools []poolCleanupable
)

func init() {
	runtime_registerPoolCleanup(poolCleanup)
}

func indexLocal[T any](l unsafe.Pointer, i int) *poolLocal[T] {
	lp := unsafe.Pointer(uintptr(l) + uintptr(i)*unsafe.Sizeof(poolLocal[T]{}))

	return (*poolLocal[T])(lp)
}

// Implemented in runtime.
//
//go:linkname runtime_registerPoolCleanup sync.runtime_registerPoolCleanup
func runtime_registerPoolCleanup(cleanup func())

//go:linkname runtime_procPin runtime.procPin
func runtime_procPin() int

//go:linkname runtime_procUnpin runtime.procUnpin
func runtime_procUnpin()

// The below are implemented in internal/runtime/atomic and the
// compiler also knows to intrinsify the symbol we linkname into this
// package.

//go:linkname runtime_LoadAcquintptr internal/runtime/atomic.LoadAcquintptr
func runtime_LoadAcquintptr(ptr *uintptr) uintptr

//go:linkname runtime_StoreReluintptr internal/runtime/atomic.StoreReluintptr
func runtime_StoreReluintptr(ptr *uintptr, val uintptr)
