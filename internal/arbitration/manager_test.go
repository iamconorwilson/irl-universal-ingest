package arbitration

import (
	"testing"
	"time"
)

func TestManagerAcquireAndRelease(t *testing.T) {
	mgr := NewManager(5*time.Second, nil)

	acquired, release := mgr.TryAcquire("rtmp", "/live")
	if !acquired {
		t.Fatalf("expected initial acquire to succeed")
	}

	info, ok := mgr.ActiveInfo()
	if !ok || info.Protocol != "RTMP" || info.Path != "/live" {
		t.Fatalf("unexpected active info: %+v", info)
	}

	secondAcquire, _ := mgr.TryAcquire("srt", "/live")
	if secondAcquire {
		t.Fatalf("expected secondary acquire to be rejected while slot is held")
	}

	release()

	_, ok = mgr.ActiveInfo()
	if ok {
		t.Fatalf("expected slot to be empty after release")
	}

	thirdAcquire, releaseThird := mgr.TryAcquire("srt", "/live")
	if !thirdAcquire {
		t.Fatalf("expected acquire after release to succeed")
	}
	releaseThird()
}

func TestManagerTimeout(t *testing.T) {
	timeoutChan := make(chan string, 1)
	mgr := NewManager(50*time.Millisecond, func(proto, path string) {
		timeoutChan <- proto
	})

	acquired, _ := mgr.TryAcquire("rtmp", "/live")
	if !acquired {
		t.Fatalf("expected acquire to succeed")
	}

	select {
	case proto := <-timeoutChan:
		if proto != "RTMP" {
			t.Errorf("expected RTMP timeout notification, got %s", proto)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for manager slot release")
	}

	_, ok := mgr.ActiveInfo()
	if ok {
		t.Fatalf("expected slot to be released on timeout")
	}
}

func TestManagerBitrateCalculation(t *testing.T) {
	mgr := NewManager(5*time.Second, nil)
	acquired, release := mgr.TryAcquire("rtmp", "/live")
	if !acquired {
		t.Fatalf("expected acquire to succeed")
	}
	defer release()

	mgr.lastBytesCheck = time.Now().Add(-1100 * time.Millisecond)
	// 125,000 bytes in 1.1s = 1,000,000 bits / 1.1s ≈ 909 kbps
	mgr.AddBytes(125000)

	info, _ := mgr.ActiveInfo()
	if info.BitrateKbps <= 0 {
		t.Errorf("expected bitrate > 0, got %d", info.BitrateKbps)
	}
}

func TestManagerReleaseBlocksAcquireUntilCallbackCompletes(t *testing.T) {
	// A slow onSlotRelease callback must block TryAcquire until it completes.
	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})

	mgr := NewManager(5*time.Second, func(proto, path string) {
		close(callbackStarted)
		<-releaseCallback
	})

	_, release := mgr.TryAcquire("rtmp", "/live")

	go release()
	<-callbackStarted

	acquireResult := make(chan bool, 1)
	go func() {
		acquired, _ := mgr.TryAcquire("srt", "/live")
		acquireResult <- acquired
	}()

	select {
	case <-acquireResult:
		t.Fatalf("expected TryAcquire to block while the release callback is still running")
	case <-time.After(100 * time.Millisecond):
		// expected: still blocked
	}

	close(releaseCallback)

	select {
	case acquired := <-acquireResult:
		if !acquired {
			t.Fatalf("expected TryAcquire to succeed once the release callback completed")
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for TryAcquire to unblock")
	}
}

func TestManagerSetBitrate(t *testing.T) {
	mgr := NewManager(5*time.Second, nil)
	acquired, release := mgr.TryAcquire("srt", "/live")
	if !acquired {
		t.Fatalf("expected acquire to succeed")
	}
	defer release()

	mgr.SetBitrate(4500)
	info, ok := mgr.ActiveInfo()
	if !ok || info.BitrateKbps != 4500 {
		t.Errorf("expected BitrateKbps 4500, got %d", info.BitrateKbps)
	}
}
