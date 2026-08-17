package arbitration

import (
	"strings"
	"sync"
	"time"
)

// SlotInfo contains details about the active ingest session.
type SlotInfo struct {
	Protocol    string
	Path        string
	StartedAt   time.Time
	LastSeen    time.Time
	BitrateKbps int64
	RTT         any
}

// Manager controls single-source arbitration across all ingest protocols.
type Manager struct {
	mu            sync.RWMutex
	sourceTimeout time.Duration
	activeSlot    *SlotInfo
	timer         *time.Timer
	onSlotRelease func(protocol, path string)

	// Bitrate tracking
	lastBytesCheck time.Time
	bytesInPeriod  uint64
}

// NewManager creates an arbitration manager with the specified silence timeout.
func NewManager(sourceTimeout time.Duration, onSlotRelease func(protocol, path string)) *Manager {
	return &Manager{
		sourceTimeout: sourceTimeout,
		onSlotRelease: onSlotRelease,
	}
}

// TryAcquire attempts to claim the active ingest slot for a protocol and path.
// Returns whether acquisition was successful and a release callback function.
func (m *Manager) TryAcquire(protocol, path string) (bool, func()) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeSlot != nil {
		return false, nil
	}

	upperProto := strings.ToUpper(protocol)
	now := time.Now()
	slot := &SlotInfo{
		Protocol:    upperProto,
		Path:        path,
		StartedAt:   now,
		LastSeen:    now,
		BitrateKbps: 0,
		RTT:         false,
	}
	m.activeSlot = slot
	m.lastBytesCheck = now
	m.bytesInPeriod = 0

	if m.sourceTimeout > 0 {
		m.timer = time.AfterFunc(m.sourceTimeout, func() {
			m.handleTimeout(upperProto, path)
		})
	}

	var once sync.Once
	release := func() {
		once.Do(func() {
			m.releaseSlot(upperProto, path)
		})
	}

	return true, release
}

// Touch refreshes the activity timestamp and resets the inactivity watchdog timer.
func (m *Manager) Touch() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeSlot == nil {
		return
	}

	m.activeSlot.LastSeen = time.Now()
	if m.timer != nil {
		m.timer.Reset(m.sourceTimeout)
	}
}

// AddBytes accumulates received byte counts for rolling bitrate computation.
func (m *Manager) AddBytes(n uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeSlot == nil {
		return
	}

	m.bytesInPeriod += n
	now := time.Now()
	elapsed := now.Sub(m.lastBytesCheck)

	if elapsed >= 1*time.Second {
		bits := float64(m.bytesInPeriod * 8)
		seconds := elapsed.Seconds()
		m.activeSlot.BitrateKbps = int64(bits / (1000.0 * seconds))

		m.bytesInPeriod = 0
		m.lastBytesCheck = now
	}
}

// SetRTT updates the current round-trip time metric for protocols that provide it.
func (m *Manager) SetRTT(rtt any) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeSlot == nil {
		return
	}
	m.activeSlot.RTT = rtt
}

// SetBitrate directly sets the current bitrate in Kbps.
func (m *Manager) SetBitrate(kbps int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeSlot == nil {
		return
	}
	m.activeSlot.BitrateKbps = kbps
}

// ActiveInfo returns snapshot information about the currently active slot.
func (m *Manager) ActiveInfo() (SlotInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.activeSlot == nil {
		return SlotInfo{}, false
	}
	return *m.activeSlot, true
}

// releaseSlot clears the active slot if it matches the releasing protocol and path.
func (m *Manager) releaseSlot(protocol, path string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeSlot == nil {
		return
	}

	upperProto := strings.ToUpper(protocol)
	if m.activeSlot.Protocol == upperProto && m.activeSlot.Path == path {
		if m.timer != nil {
			m.timer.Stop()
			m.timer = nil
		}
		m.activeSlot = nil

		if m.onSlotRelease != nil {
			go m.onSlotRelease(upperProto, path)
		}
	}
}

// handleTimeout clears an inactive slot after silence exceeds sourceTimeout.
func (m *Manager) handleTimeout(protocol, path string) {
	m.releaseSlot(protocol, path)
}
