package syncx

import "sync"

// A Pool is a type-safe wrapper around sync.Pool.
//
// Any item stored in the Pool may be removed automatically at any time without
// notification. If the Pool holds the only reference when this happens, the
// item might be deallocated.
//
// A Pool is safe for use by multiple goroutines simultaneously.
//
// A Pool must not be copied after first use.
type Pool[T any] struct {
	_ noCopy

	p sync.Pool

	// New optionally specifies a function to generate a value when Get would
	// otherwise return the zero value. It may not be changed concurrently with
	// calls to Get.
	New func() T
}

type poolItem[T any] struct {
	value T
}

// Put adds x to the pool.
func (p *Pool[T]) Put(x T) {
	if p == nil {
		panic("nil Pool")
	}
	p.p.Put(poolItem[T]{value: x})
}

// Get selects an arbitrary item from the Pool, removes it from the Pool, and
// returns it to the caller.
//
// Get may choose to ignore the pool and treat it as empty. Callers should not
// assume any relation between values passed to Put and the values returned by
// Get.
//
// If Get would otherwise return the zero value and p.New is non-nil, Get returns
// the result of calling p.New.
func (p *Pool[T]) Get() T {
	if p == nil {
		panic("nil Pool")
	}
	if x := p.p.Get(); x != nil {
		return x.(poolItem[T]).value
	}
	if p.New != nil {
		return p.New()
	}
	var zero T
	return zero
}
