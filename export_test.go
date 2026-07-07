// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package syncx

// Export for testing.
var Runtime_procPin = runtime_procPin
var Runtime_procUnpin = runtime_procUnpin

// PoolDequeue exports an interface for poolDequeue testing.
type PoolDequeue[T any] interface {
	PushHead(val T) bool
	PopHead() (T, bool)
	PopTail() (T, bool)
}

func NewPoolDequeue[T any](n int) PoolDequeue[T] {
	d := &poolDequeue[T]{
		vals: make([]poolDequeueSlot[T], n),
	}
	// For testing purposes, set the head and tail indexes close
	// to wrapping around.
	d.headTail.Store(d.pack(1<<dequeueBits-500, 1<<dequeueBits-500))
	return d
}

func (d *poolDequeue[T]) PushHead(val T) bool {
	return d.pushHead(val)
}

func (d *poolDequeue[T]) PopHead() (T, bool) {
	return d.popHead()
}

func (d *poolDequeue[T]) PopTail() (T, bool) {
	return d.popTail()
}

func NewPoolChain[T any]() PoolDequeue[T] {
	return new(poolChain[T])
}

func (c *poolChain[T]) PushHead(val T) bool {
	c.pushHead(val)
	return true
}

func (c *poolChain[T]) PopHead() (T, bool) {
	return c.popHead()
}

func (c *poolChain[T]) PopTail() (T, bool) {
	return c.popTail()
}
