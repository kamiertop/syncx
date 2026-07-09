package syncx

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

type WaitGroup struct {
	sync.WaitGroup
}

// NewWaitGroup returns a new WaitGroup instance, this is a wrapper around the standard library's WaitGroup.
func NewWaitGroup() *WaitGroup {
	return &WaitGroup{}
}

type waitGroupFields struct {
	_     noCopy
	state atomic.Uint64
	sema  uint32
}

func (wg *WaitGroup) state() uint64 {
	return (*waitGroupFields)(unsafe.Pointer(&wg.WaitGroup)).state.Load()
}

// Waiter return the number of the Waiter goroutine
func (wg *WaitGroup) Waiter() int {
	return int(wg.state() & 0x7fffffff)
}

// Counter return the number of the WaitGroup counter
func (wg *WaitGroup) Counter() int {
	return int(int32(wg.state() >> 32))
}
