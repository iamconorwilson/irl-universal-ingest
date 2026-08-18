// Package health aggregates component-level failure state so /healthz can report real status.
package health

import "sync"

// Tracker aggregates named component health flags.
type Tracker struct {
	mu     sync.RWMutex
	errors map[string]string
}

// NewTracker creates an empty (healthy) Tracker.
func NewTracker() *Tracker {
	return &Tracker{errors: make(map[string]string)}
}

// SetUnhealthy records a component-level failure with a human-readable reason.
func (t *Tracker) SetUnhealthy(component, reason string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.errors[component] = reason
}

// ClearUnhealthy removes a previously recorded failure once the component recovers.
func (t *Tracker) ClearUnhealthy(component string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.errors, component)
}

// Problems returns a snapshot of currently unhealthy components; an empty map means healthy.
func (t *Tracker) Problems() map[string]string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[string]string, len(t.errors))
	for k, v := range t.errors {
		out[k] = v
	}
	return out
}
