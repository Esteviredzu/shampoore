package vp8

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

func newTestTrack(t *testing.T) *webrtc.TrackLocalStaticSample {
	t.Helper()
	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8},
		"test-id", "test-stream",
	)
	if err != nil {
		t.Fatalf("new track: %v", err)
	}
	return track
}

func TestReportErrorFollowsGeneration(t *testing.T) {
	tun, err := AttachTunnel(nil, nil, nil)
	if err != nil {
		t.Fatalf("attach tunnel: %v", err)
	}
	defer func() {
		if err := tun.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	gen := tun.gen.Load()
	tun.reportError(gen, errors.New("track dead"))

	select {
	case err := <-tun.Errors():
		if err == nil || err.Error() != "track dead" {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("track error was not reported")
	}

	tun.reportError(gen-1, errors.New("stale generation"))
	select {
	case err := <-tun.Errors():
		t.Fatalf("stale generation leaked: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	tun.InvalidateSession()
	tun.reportError(gen, errors.New("stale after invalidate"))
	select {
	case err := <-tun.Errors():
		t.Fatalf("stale error leaked after invalidate: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWriteWithoutTracksReturnsError(t *testing.T) {
	tun, err := AttachTunnel(nil, nil, nil)
	if err != nil {
		t.Fatalf("attach tunnel: %v", err)
	}
	defer func() {
		if err := tun.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	if err := tun.WriteRawVP8([]byte{0x11}); err == nil {
		t.Fatal("expected an error when the publisher track is missing")
	}
	if err := tun.WriteSharingVP8([]byte{0x11}); err == nil {
		t.Fatal("expected an error when the sharing track is missing")
	}
}

func TestRebindOnClosedTunnelFails(t *testing.T) {
	tun, err := AttachTunnel(nil, nil, nil)
	if err != nil {
		t.Fatalf("attach tunnel: %v", err)
	}
	track := newTestTrack(t)

	if err := tun.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := tun.Rebind(nil, track); err == nil {
		t.Fatal("expected rebind on a closed tunnel to fail")
	}
	if err := tun.Rebind(nil, nil); err == nil {
		t.Fatal("expected rebind without a track to fail")
	}
}

func TestRTPSequenceGapDetection(t *testing.T) {
	tests := []struct {
		prev, cur uint16
		missing   uint16
		gap       bool
	}{
		{prev: 100, cur: 101, gap: false},
		{prev: 100, cur: 100, gap: false},
		{prev: 100, cur: 99, gap: false},
		{prev: 100, cur: 105, missing: 4, gap: true},
		{prev: 65535, cur: 1, missing: 1, gap: true},
		{prev: 100, cur: 100 + uint16(pliMaxGapPackets), missing: uint16(pliMaxGapPackets - 1), gap: true},
		{prev: 100, cur: 100 + uint16(pliMaxGapPackets) + 1, gap: false},
	}
	for _, tc := range tests {
		missing, gap := rtpSequenceGap(tc.prev, tc.cur)
		if gap != tc.gap || missing != tc.missing {
			t.Fatalf("rtpSequenceGap(%d, %d) = (%d, %v), want (%d, %v)",
				tc.prev, tc.cur, missing, gap, tc.missing, tc.gap)
		}
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}

func TestPLIOnlyOnGapWithBackoff(t *testing.T) {
	tun, err := AttachTunnel(nil, nil, nil)
	if err != nil {
		t.Fatalf("attach tunnel: %v", err)
	}
	defer func() {
		if err := tun.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	tun.pliBase = 20 * time.Millisecond
	tun.pliMax = 40 * time.Millisecond
	tun.pliReset = 200 * time.Millisecond

	var calls atomic.Int32
	tun.pliHook = func(webrtc.SSRC) error {
		calls.Add(1)
		return nil
	}

	go tun.pliLoop(42, nil, tun.gen.Load())

	time.Sleep(3 * tun.pliBase)
	if got := calls.Load(); got != 0 {
		t.Fatalf("PLI sent without an RTP gap: %d", got)
	}

	tun.requestPLI()
	waitFor(t, time.Second, func() bool { return calls.Load() >= 1 })
	if got := calls.Load(); got != 1 {
		t.Fatalf("PLI calls after first gap = %d, want 1", got)
	}

	tun.requestPLI()
	tun.requestPLI()
	time.Sleep(5 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("PLI storm during backoff: %d calls", got)
	}

	time.Sleep(2 * tun.pliBase)
	tun.requestPLI()
	waitFor(t, time.Second, func() bool { return calls.Load() >= 2 })

	before := calls.Load()
	tun.requestPLI()
	time.Sleep(5 * time.Millisecond)
	if got := calls.Load(); got != before {
		t.Fatalf("PLI backoff did not suppress a burst: %d -> %d", before, got)
	}
}

func TestRebindConcurrentAccess(t *testing.T) {
	track := newTestTrack(t)

	tun, err := AttachTunnel(nil, nil, track)
	if err != nil {
		t.Fatalf("attach tunnel: %v", err)
	}
	defer func() {
		if err := tun.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	stop := make(chan struct{})
	var wg sync.WaitGroup

	run := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					fn()
				}
			}
		}()
	}

	run(func() {
		if err := tun.Rebind(nil, track); err != nil {
			t.Errorf("rebind: %v", err)
		}
	})
	run(func() {
		if err := tun.WriteRawVP8([]byte{0x11, 0x22}); err != nil {
			t.Errorf("write raw: %v", err)
		}
	})
	run(func() {
		tun.SetSharingTrack(track)
		if err := tun.WriteSharingVP8([]byte{0x11, 0x22}); err != nil {
			t.Errorf("write sharing: %v", err)
		}
	})
	run(func() {
		tun.InvalidateSession()
		select {
		case <-tun.Errors():
		default:
		}
	})

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}
