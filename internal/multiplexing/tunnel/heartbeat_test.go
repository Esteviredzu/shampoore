package tunnel

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

type linkedTransport struct {
	incoming <-chan []byte
	outgoing chan<- []byte
}

func (l *linkedTransport) Start(context.Context) error { return nil }
func (l *linkedTransport) Stop(context.Context) error  { return nil }
func (l *linkedTransport) Send(ctx context.Context, data []byte) error {
	cp := append([]byte(nil), data...)
	select {
	case l.outgoing <- cp:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (l *linkedTransport) Receive(ctx context.Context) ([]byte, error) {
	select {
	case data := <-l.incoming:
		return data, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (l *linkedTransport) MaxMessageSize() int   { return 1 << 20 }
func (l *linkedTransport) BytesDelivered() int64 { return 0 }
func (l *linkedTransport) BytesAcked() int64     { return 0 }

func linkedPair() (client, server *linkedTransport) {
	a2b := make(chan []byte, 256)
	b2a := make(chan []byte, 256)
	return &linkedTransport{incoming: b2a, outgoing: a2b},
		&linkedTransport{incoming: a2b, outgoing: b2a}
}

func linkedConfig() Config {
	return Config{
		FlushThreshold: 4096,
		FlushInterval:  2 * time.Millisecond,
		PingInterval:   10 * time.Millisecond,
		PingTimeout:    200 * time.Millisecond,
	}
}

func waitPeerModern(t *testing.T, m *TunnelMultiplexer, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !m.peerModern.Load() {
		if time.Now().After(deadline) {
			t.Fatal("peer did not confirm the control protocol")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPingPongEnablesModernPeerAndAckedOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientTr, serverTr := linkedPair()
	cfg := linkedConfig()

	server := New(serverTr, testLogger{}, cfg)
	if err := server.Start(ctx); err != nil {
		t.Fatalf("server start: %v", err)
	}
	defer func() { _ = server.Stop(context.Background()) }()

	client := New(clientTr, testLogger{}, cfg)
	if err := client.Start(ctx); err != nil {
		t.Fatalf("client start: %v", err)
	}
	defer func() { _ = client.Stop(context.Background()) }()

	waitPeerModern(t, client, time.Second)

	accepted := make(chan error, 1)
	go func() {
		stream, err := server.AcceptStream(ctx)
		if err != nil {
			accepted <- err
			return
		}
		defer stream.Close()
		buf := make([]byte, 4)
		if _, err := io.ReadFull(stream, buf); err != nil {
			accepted <- err
			return
		}
		_, err = stream.Write([]byte("pong"))
		accepted <- err
	}()

	stream, err := client.OpenStream(ctx, map[string]any{"host": "example.com", "port": 443})
	if err != nil {
		t.Fatalf("open stream with OPEN-ACK: %v", err)
	}
	defer stream.Close()
	if _, err := stream.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}

	reply := make([]byte, 4)
	if _, err := io.ReadFull(stream, reply); err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if string(reply) != "pong" {
		t.Fatalf("reply = %q", reply)
	}
	if err := <-accepted; err != nil {
		t.Fatalf("server handler: %v", err)
	}
}

func TestOpenStreamRetriesAndFailsWithoutAck(t *testing.T) {
	prev := openAckTimeout
	openAckTimeout = 30 * time.Millisecond
	t.Cleanup(func() { openAckTimeout = prev })

	tr := newFakeTransport()
	m := New(tr, testLogger{}, testConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = m.Stop(context.Background()) }()

	m.peerModern.Store(true)

	if _, err := m.OpenStream(ctx, map[string]any{"host": "example.com", "port": 443}); !errors.Is(err, errOpenAckTimeout) {
		t.Fatalf("open error = %v, want errOpenAckTimeout", err)
	}

	opens := 0
	for _, frame := range tr.sentFrames(t) {
		if frame.Type == FrameOpen {
			opens++
		}
	}
	if opens != 1+maxOpenRetries {
		t.Fatalf("OPEN frames sent = %d, want %d", opens, 1+maxOpenRetries)
	}

	m.mu.Lock()
	live := len(m.streams)
	m.mu.Unlock()
	if live != 0 {
		t.Fatalf("%d unconfirmed streams left behind", live)
	}
}

func TestOpenAckWindowCoversSlowTransports(t *testing.T) {
	prev := openAckTimeout
	openAckTimeout = defaultOpenAckTimeout
	t.Cleanup(func() { openAckTimeout = prev })

	tr := newFakeTransport()
	m := New(tr, testLogger{}, testConfig())

	if got := m.openAckWindow(); got != defaultOpenAckTimeout {
		t.Fatalf("default open-ack window = %s, want %s", got, defaultOpenAckTimeout)
	}

	m.cfg.FlushInterval = time.Second
	if got, want := m.openAckWindow(), openAckFlushWindows*time.Second; got != want {
		t.Fatalf("adaptive open-ack window = %s, want %s", got, want)
	}

	openAckTimeout = 25 * time.Millisecond
	if got := m.openAckWindow(); got != openAckTimeout {
		t.Fatalf("overridden open-ack window = %s, want %s", got, openAckTimeout)
	}
}

func TestOpenStreamToleratesSlowOpenAck(t *testing.T) {

	const ackDelay = 1700 * time.Millisecond

	prev := openAckTimeout
	openAckTimeout = defaultOpenAckTimeout
	t.Cleanup(func() { openAckTimeout = prev })

	tr := newFakeTransport()
	ackCtx, ackCancel := context.WithCancel(context.Background())
	defer ackCancel()

	tr.setSendFunc(func(_ context.Context, data []byte) error {
		frames, err := NewFrameDecoder().Feed(append([]byte(nil), data...))
		if err != nil {
			return nil
		}
		for _, frame := range frames {
			if frame.Type != FrameOpen {
				continue
			}
			ack, err := EncodeFrame(&TunnelFrame{
				StreamID: frame.StreamID,
				Type:     FrameOpenAck,
				Epoch:    frame.Epoch,
			})
			if err != nil {
				continue
			}
			go func(ack []byte) {
				timer := time.NewTimer(ackDelay)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-ackCtx.Done():
					return
				}
				select {
				case tr.recvCh <- ack:
				case <-ackCtx.Done():
				}
			}(ack)
		}
		return nil
	})

	m := New(tr, testLogger{}, testConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = m.Stop(context.Background()) }()

	m.peerModern.Store(true)

	start := time.Now()
	stream, err := m.OpenStream(ctx, map[string]any{"host": "example.com", "port": 443})
	if err != nil {
		t.Fatalf("open stream with a slow OPEN-ACK: %v", err)
	}
	defer stream.Close()
	if elapsed := time.Since(start); elapsed < ackDelay/2 {
		t.Fatalf("stream opened in %s, expected to wait for the delayed ack", elapsed)
	}
}

func TestPingWatchdogFailsSilentSession(t *testing.T) {
	tr := newFakeTransport()
	failed := make(chan error, 1)
	cfg := Config{
		FlushThreshold: 4096,
		FlushInterval:  2 * time.Millisecond,
		PingInterval:   10 * time.Millisecond,
		PingTimeout:    20 * time.Millisecond,
		OnSessionError: func(err error) {
			select {
			case failed <- err:
			default:
			}
		},
	}
	m := New(tr, testLogger{}, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = m.Stop(context.Background()) }()

	m.peerModern.Store(true)

	select {
	case err := <-failed:
		if err == nil {
			t.Fatal("watchdog reported a nil error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("silent session was not failed by the ping watchdog")
	}
}
