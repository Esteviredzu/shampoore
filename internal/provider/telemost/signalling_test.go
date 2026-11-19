package telemost

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

func newTestConfig() *Config {
	cfg := DefaultConfig()
	cfg.LocalSignalingURL = "ws://127.0.0.1:1"
	cfg.ConferenceLink = "https://telemost.yandex.ru/j/abcdef123456"
	return &cfg
}

func newTestSignaling(t *testing.T) *signaling {
	t.Helper()

	cfg := newTestConfig()
	s := newSignaling(cfg, fakeBootstrap(cfg.LocalSignalingURL), testLogger{})

	api, err := newWebRTCAPI()
	if err != nil {
		t.Fatalf("create webrtc api: %v", err)
	}
	pubPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create publisher pc: %v", err)
	}
	subPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		_ = pubPC.Close()
		t.Fatalf("create subscriber pc: %v", err)
	}

	s.pcMu.Lock()
	s.pubPC = pubPC
	s.subPC = subPC
	s.pcMu.Unlock()

	t.Cleanup(s.Close)
	return s
}

func TestConnectedIPsConcurrentWithClose(t *testing.T) {
	s := newTestSignaling(t)

	cfg := newTestConfig()
	p := NewPipe(cfg, fakeBootstrap(cfg.LocalSignalingURL), testLogger{})
	p.mu.Lock()
	p.sig = s
	p.mu.Unlock()

	const readers = 8
	start := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 300; j++ {
				_ = s.ConnectedIPs()
				_ = p.ConnectedIPs()
			}
		}()
	}

	closerDone := make(chan struct{})
	go func() {
		defer close(closerDone)
		<-start
		for i := 0; i < 16; i++ {
			s.Close()
			_ = p.Stop(context.Background())
		}
	}()

	close(start)
	wg.Wait()
	<-closerDone

	if ips := p.ConnectedIPs(); ips != nil {
		t.Fatalf("expected no IPs after stop, got %v", ips)
	}
}

func TestHandlePCStateNotifiesSessionLoss(t *testing.T) {
	s := newTestSignaling(t)

	var lost atomic.Int64
	s.setSessionLostCallback(func(string) { lost.Add(1) })

	s.handlePCState("publisher", nil, webrtc.PeerConnectionStateFailed)
	s.handlePCState("subscriber", nil, webrtc.PeerConnectionStateFailed)
	s.handlePCState("publisher", nil, webrtc.PeerConnectionStateClosed)

	if got := lost.Load(); got != 1 {
		t.Fatalf("expected exactly one session lost notification, got %d", got)
	}

	s.Close()
	s.handlePCState("publisher", nil, webrtc.PeerConnectionStateFailed)
	if got := lost.Load(); got != 1 {
		t.Fatalf("session lost notification fired after close: %d", got)
	}
}

func TestDisconnectedArmsRestartTimer(t *testing.T) {
	s := newTestSignaling(t)
	s.disconnectWait = 20 * time.Millisecond

	lost := make(chan string, 1)
	s.setSessionLostCallback(func(reason string) {
		select {
		case lost <- reason:
		default:
		}
	})

	s.handlePCState("publisher", nil, webrtc.PeerConnectionStateDisconnected)

	select {
	case reason := <-lost:
		if reason == "" {
			t.Fatal("empty session lost reason")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected a session restart request after the disconnect grace")
	}
}

func TestConnectedCancelsRestartTimer(t *testing.T) {
	s := newTestSignaling(t)
	s.disconnectWait = 80 * time.Millisecond

	var lost atomic.Int64
	s.setSessionLostCallback(func(string) { lost.Add(1) })

	s.handlePCState("publisher", nil, webrtc.PeerConnectionStateDisconnected)
	s.handlePCState("publisher", nil, webrtc.PeerConnectionStateConnected)

	time.Sleep(200 * time.Millisecond)
	if got := lost.Load(); got != 0 {
		t.Fatalf("connected state did not cancel the restart timer (%d notifications)", got)
	}
}

func TestStartFailureCleansUp(t *testing.T) {
	cfg := newTestConfig()
	p := NewPipe(cfg, fakeBootstrap(cfg.LocalSignalingURL), testLogger{})
	p.publishWait = 200 * time.Millisecond
	p.subscribeWait = 50 * time.Millisecond

	started := time.Now()
	err := p.Start(context.Background())
	if err == nil {
		t.Fatal("expected start to fail against an unreachable signalling server")
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("start took too long to fail: %s", elapsed)
	}
	if tunnel := p.Tunnel(); tunnel != nil {
		t.Fatal("tunnel must not be published after a failed start")
	}
	if got := p.ConnectedIPs(); got != nil {
		t.Fatalf("expected no connected IPs after a failed start, got %v", got)
	}

	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("stop after failed start: %v", err)
	}
	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("second stop: %v", err)
	}
	if err := p.Start(context.Background()); err == nil {
		t.Fatal("expected start on a stopped pipe to fail")
	}
}

func TestStartAbortsHangingDial(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() {
		if err := ln.Close(); err != nil {
			t.Logf("close listener: %v", err)
		}
	}()

	stop := make(chan struct{})
	defer close(stop)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				<-stop
				if err := c.Close(); err != nil {
					return
				}
			}(conn)
		}
	}()

	cfg := DefaultConfig()
	cfg.LocalSignalingURL = "ws://" + ln.Addr().String()
	cfg.ConferenceLink = "https://telemost.yandex.ru/j/abcdef123456"

	p := NewPipe(&cfg, fakeBootstrap(cfg.LocalSignalingURL), testLogger{})
	p.publishWait = 200 * time.Millisecond
	p.subscribeWait = 50 * time.Millisecond

	started := time.Now()
	if err := p.Start(context.Background()); err == nil {
		t.Fatal("expected start to fail when the server never answers")
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("start blocked on the handshake for %s", elapsed)
	}
	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
}
