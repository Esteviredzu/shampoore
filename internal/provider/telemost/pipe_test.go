package telemost

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/core/channel/vp8"
)

func makeFakeSession(tunnel *vp8.Tunnel) (*session, error) {
	if tunnel == nil {
		var err error
		tunnel, err = vp8.AttachTunnel(nil, nil, nil)
		if err != nil {
			return nil, err
		}
	}
	sess := &session{
		tunnel: tunnel,
		lost:   make(chan struct{}),
		done:   make(chan struct{}),
	}

	close(sess.done)
	return sess, nil
}

type sessionLog struct {
	mu   sync.Mutex
	list []*session
}

func (l *sessionLog) add(s *session) {
	l.mu.Lock()
	l.list = append(l.list, s)
	l.mu.Unlock()
}

func (l *sessionLog) get(n int) *session {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.list) < n {
		return nil
	}
	return l.list[n-1]
}

func (l *sessionLog) snapshot() []*session {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]*session, len(l.list))
	copy(out, l.list)
	return out
}

func (l *sessionLog) waitFor(t *testing.T, n int, timeout time.Duration) *session {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if sess := l.get(n); sess != nil {
			return sess
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("session #%d was not created in %s", n, timeout)
	return nil
}

func fastRetryPipe(t *testing.T) *Pipe {
	t.Helper()
	cfg := newTestConfig()
	p := NewPipe(cfg, fakeBootstrap(cfg.LocalSignalingURL), testLogger{})
	p.publishWait = time.Second
	p.subscribeWait = time.Second
	p.retryMin = 5 * time.Millisecond
	p.retryMax = 25 * time.Millisecond
	return p
}

func TestSupervisorRestartsSessionAfterLoss(t *testing.T) {
	p := fastRetryPipe(t)

	var log sessionLog
	p.openSessionFn = func(
		_ context.Context,
		_ *bootstrapResult,
		reuse *vp8.Tunnel,
	) (*session, error) {
		sess, err := makeFakeSession(reuse)
		if err != nil {
			return nil, err
		}
		log.add(sess)
		return sess, nil
	}

	var resets atomic.Int64
	p.SetTransportResetFunc(func() { resets.Add(1) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := p.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	first := log.waitFor(t, 1, time.Second)

	first.markLost()

	second := log.waitFor(t, 2, 3*time.Second)
	if second.tunnel != first.tunnel {
		t.Fatal("restarted session must reuse the existing tunnel")
	}

	deadline := time.Now().Add(3 * time.Second)
	for resets.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if resets.Load() == 0 {
		t.Fatal("transport reset was not signalled upwards")
	}

	if !first.isClosing() {
		t.Fatal("lost session was not closed before the restart")
	}

	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}

	for _, sess := range log.snapshot() {
		if !sess.isClosing() {
			t.Fatal("session left open after stop")
		}
	}
}

func TestSupervisorStopDuringBackoff(t *testing.T) {
	p := fastRetryPipe(t)
	p.retryMin = 100 * time.Millisecond
	p.retryMax = 300 * time.Millisecond

	var calls atomic.Int64
	var first *session
	p.openSessionFn = func(
		_ context.Context,
		_ *bootstrapResult,
		reuse *vp8.Tunnel,
	) (*session, error) {
		if calls.Add(1) == 1 {
			sess, err := makeFakeSession(reuse)
			if err != nil {
				return nil, err
			}
			first = sess
			return sess, nil
		}
		return nil, errors.New("restart failed")
	}

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	first.markLost()

	deadline := time.Now().Add(3 * time.Second)
	for calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if calls.Load() < 2 {
		t.Fatal("supervisor did not retry the restart")
	}

	stopped := make(chan error, 1)
	go func() { stopped <- p.Stop(context.Background()) }()

	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("stop: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stop blocked inside the retry backoff")
	}

	if !first.isClosing() {
		t.Fatal("session was not closed on stop")
	}
}

func TestSupervisorKeepsRetryingTunnelDeath(t *testing.T) {
	p := fastRetryPipe(t)

	var log sessionLog
	p.openSessionFn = func(
		_ context.Context,
		_ *bootstrapResult,
		reuse *vp8.Tunnel,
	) (*session, error) {
		sess, err := makeFakeSession(reuse)
		if err != nil {
			return nil, err
		}
		log.add(sess)
		return sess, nil
	}

	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		if err := p.Stop(context.Background()); err != nil {
			t.Errorf("stop: %v", err)
		}
	}()

	for i := 1; i <= 3; i++ {
		sess := log.waitFor(t, i, 3*time.Second)
		sess.markLost()
	}
	log.waitFor(t, 4, 3*time.Second)
}
