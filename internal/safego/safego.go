// Package safego runs goroutines with panic recovery so one bad stream can't crash the process.
package safego

import (
	"log"
	"runtime/debug"
)

// Go runs fn in a new goroutine, recovering and logging any panic instead of crashing.
func Go(label string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[panic] recovered in %s: %v\n%s", label, r, debug.Stack())
			}
		}()
		fn()
	}()
}
