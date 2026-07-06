// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package syncx_test

import (
	"errors"
	"sync/atomic"
	"testing"

	. "github.com/kamiertop/syncx"
)

type one int

func (o *one) Increment() {
	*o++
}

func run(t *testing.T, once *Once, o *one, c chan bool) {
	once.Do(func() { o.Increment() })
	if v := *o; v != 1 {
		t.Errorf("once failed inside run: %d is not 1", v)
	}
	c <- true
}

func TestOnce(t *testing.T) {
	o := new(one)
	once := new(Once)
	c := make(chan bool)
	const N = 10
	for i := 0; i < N; i++ {
		go run(t, once, o, c)
	}
	for i := 0; i < N; i++ {
		<-c
	}
	if *o != 1 {
		t.Errorf("once failed outside run: %d is not 1", *o)
	}
}

func TestOncePanic(t *testing.T) {
	var once Once
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("Once.Do did not panic")
			}
		}()
		once.Do(func() {
			panic("failed")
		})
	}()

	once.Do(func() {
		t.Fatalf("Once.Do called twice")
	})
}

func TestOnceDone(t *testing.T) {
	var once Once
	if once.Done() {
		t.Fatal("new Once is done")
	}

	once.Do(func() {})
	if !once.Done() {
		t.Fatal("Once is not done after Do")
	}
}

func TestOnceDoneWhileDoRunning(t *testing.T) {
	var once Once
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})

	go func() {
		once.Do(func() {
			close(started)
			<-release
		})
		close(done)
	}()

	<-started
	if once.Done() {
		t.Fatal("Once is done while Do is still running")
	}

	close(release)
	<-done
	if !once.Done() {
		t.Fatal("Once is not done after Do completed")
	}
}

func TestOnceDoEDone(t *testing.T) {
	var once Once
	if once.Done() {
		t.Fatal("new Once is done")
	}

	if err := once.DoE(func() error { return nil }); err != nil {
		t.Fatalf("DoE returned error: %v", err)
	}
	if !once.Done() {
		t.Fatal("Once is not done after successful DoE")
	}
}

func TestOnceDoEDoneWhileRunning(t *testing.T) {
	var once Once
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error)

	go func() {
		done <- once.DoE(func() error {
			close(started)
			<-release
			return nil
		})
	}()

	<-started
	if once.Done() {
		t.Fatal("Once is done while DoE is still running")
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("DoE returned error: %v", err)
	}
	if !once.Done() {
		t.Fatal("Once is not done after DoE completed")
	}
}

func TestOnceDoEErrorRetries(t *testing.T) {
	var once Once
	var calls int
	wantErr := errors.New("failed")

	if err := once.DoE(func() error {
		calls++
		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("DoE error = %v, want %v", err, wantErr)
	}
	if once.Done() {
		t.Fatal("Once is done after failed DoE")
	}

	if err := once.DoE(func() error {
		calls++
		return nil
	}); err != nil {
		t.Fatalf("DoE retry returned error: %v", err)
	}
	if !once.Done() {
		t.Fatal("Once is not done after successful DoE retry")
	}
	if calls != 2 {
		t.Fatalf("DoE calls = %d, want 2", calls)
	}
}

func TestOnceDoESuccessRunsOnce(t *testing.T) {
	var once Once
	var calls int

	if err := once.DoE(func() error {
		calls++
		return nil
	}); err != nil {
		t.Fatalf("DoE returned error: %v", err)
	}
	if err := once.DoE(func() error {
		calls++
		return errors.New("should not run")
	}); err != nil {
		t.Fatalf("second DoE returned error: %v", err)
	}

	if calls != 1 {
		t.Fatalf("DoE calls = %d, want 1", calls)
	}
}

func TestOnceDoEConcurrent(t *testing.T) {
	var once Once
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 2)

	go func() {
		done <- once.DoE(func() error {
			calls.Add(1)
			close(started)
			<-release
			return nil
		})
	}()

	<-started
	go func() {
		done <- once.DoE(func() error {
			calls.Add(1)
			return errors.New("should not run")
		})
	}()

	close(release)
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatalf("DoE returned error: %v", err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("DoE calls = %d, want 1", got)
	}
	if !once.Done() {
		t.Fatal("Once is not done after concurrent DoE")
	}
}

func BenchmarkOnce(b *testing.B) {
	var once Once
	f := func() {}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			once.Do(f)
		}
	})
}
