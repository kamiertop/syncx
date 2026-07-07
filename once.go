package syncx

import (
	"sync"
	"sync/atomic"
)

type Once struct {
	_    noCopy
	done atomic.Bool
	m    sync.Mutex
}

func (o *Once) Do(f func()) {
	if !o.done.Load() {
		o.doSlow(f)
	}
}

func (o *Once) doSlow(f func()) {
	o.m.Lock()
	defer o.m.Unlock()

	if !o.done.Load() {
		defer o.done.Store(true)
		f()
	}
}

func (o *Once) Done() bool {
	return o.done.Load()
}

func (o *Once) DoE(f func() error) error {
	if !o.done.Load() {
		return o.doSlowE(f)
	}

	return nil
}

func (o *Once) doSlowE(f func() error) error {
	o.m.Lock()
	defer o.m.Unlock()

	if o.done.Load() {
		return nil
	}

	if err := f(); err != nil {
		return err
	}

	o.done.Store(true)

	return nil
}

type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
