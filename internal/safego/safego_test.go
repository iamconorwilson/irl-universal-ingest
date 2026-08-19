package safego

import (
	"testing"
	"time"
)

func TestGoRecoversPanic(t *testing.T) {
	done := make(chan struct{})

	Go("test.panicker", func() {
		defer close(done)
		panic("boom")
	})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("goroutine never completed; panic likely propagated instead of being recovered")
	}
}

func TestGoRunsNormally(t *testing.T) {
	result := make(chan int, 1)

	Go("test.normal", func() {
		result <- 42
	})

	select {
	case v := <-result:
		if v != 42 {
			t.Errorf("expected 42, got %d", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("goroutine never ran")
	}
}
