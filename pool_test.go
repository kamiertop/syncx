package syncx_test

import (
	"sync"
	"testing"

	. "github.com/kamiertop/syncx"
)

func TestPool(t *testing.T) {
	var p Pool[string]
	if got := p.Get(); got != "" {
		t.Fatalf("got %#v; want empty string", got)
	}

	p.Put("a")
	if got := p.Get(); got != "a" {
		t.Fatalf("got %#v; want a", got)
	}
}

func TestPoolNew(t *testing.T) {
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

	p.Put(42)
	if got := p.Get(); got != 42 {
		t.Fatalf("got %v; want 42", got)
	}

	if got := p.Get(); got != 2 {
		t.Fatalf("got %v; want 2", got)
	}
}

func TestPoolZeroValue(t *testing.T) {
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

func TestPoolNilPointerValue(t *testing.T) {
	calls := 0
	value := 1
	p := Pool[*int]{
		New: func() *int {
			calls++
			return &value
		},
	}

	p.Put(nil)
	if got := p.Get(); got != nil {
		t.Fatalf("got %v; want pooled nil pointer", got)
	}
	if calls != 0 {
		t.Fatalf("New called %d times; want 0", calls)
	}
}

func TestPoolStress(t *testing.T) {
	const goroutines = 10
	n := 100000
	if testing.Short() {
		n = 1000
	}

	var p Pool[int]
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < n; j++ {
				p.Put(j)
				_ = p.Get()
			}
		}()
	}
	wg.Wait()
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
