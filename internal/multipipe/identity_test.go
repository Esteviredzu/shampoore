package multipipe

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/Esteviredzu/shampoore/internal/app"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/multiplexing/tunnel"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

const testCarrierKind = "multipipe_test_carrier"

type frameRecorder struct {
	mu     sync.Mutex
	frames [][]byte
}

func (r *frameRecorder) add(frame []byte) {
	if len(frame) == 0 {
		return
	}
	cp := append([]byte(nil), frame...)
	r.mu.Lock()
	r.frames = append(r.frames, cp)
	r.mu.Unlock()
}

func (r *frameRecorder) snapshot() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][]byte, len(r.frames))
	copy(out, r.frames)
	return out
}

var activeRecorder struct {
	mu sync.Mutex
	r  *frameRecorder
}

func setRecorder(r *frameRecorder) {
	activeRecorder.mu.Lock()
	activeRecorder.r = r
	activeRecorder.mu.Unlock()
}

func currentRecorder() *frameRecorder {
	activeRecorder.mu.Lock()
	defer activeRecorder.mu.Unlock()
	return activeRecorder.r
}

type recordingTransport struct {
	rec *frameRecorder
}

func (r *recordingTransport) Start(context.Context) error { return nil }
func (r *recordingTransport) Stop(context.Context) error  { return nil }
func (r *recordingTransport) Send(_ context.Context, data []byte) error {
	if r.rec != nil {
		r.rec.add(data)
	}
	return nil
}
func (r *recordingTransport) Receive(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (r *recordingTransport) MaxMessageSize() int   { return 1 << 20 }
func (r *recordingTransport) BytesDelivered() int64 { return 0 }
func (r *recordingTransport) BytesAcked() int64     { return 0 }

var registerCarrier sync.Once

func registerTestCarrier() {
	registerCarrier.Do(func() {
		err := app.RegisterDescriptor(app.Descriptor{
			Kind: testCarrierKind,
			Build: func(context.Context, *app.Settings, string, logging.ContextLogger, app.Values) (*app.BuildResult, error) {
				return &app.BuildResult{Transport: &recordingTransport{rec: currentRecorder()}}, nil
			},
			Defaults: app.NewValues(),
		})
		if err != nil {
			panic(err)
		}
	})
}

func multiSettings(t *testing.T, strategy string) *app.Settings {
	t.Helper()
	profile := map[string]any{
		"type":         "multi",
		"user_id":      "alice",
		"exit_node_id": "42",
		"transport": map[string]any{
			"strategy": strategy,
			"transports": []map[string]any{
				{"type": testCarrierKind},
				{"type": testCarrierKind},
			},
		},
	}
	raw, err := json.Marshal(profile)
	if err != nil {
		t.Fatal(err)
	}
	settings, err := app.ParseSource(string(raw))
	if err != nil {
		t.Fatalf("parse subscription profile: %v", err)
	}
	return settings
}

func waitForHello(t *testing.T, frames func() [][]byte, payload func([][]byte) []byte) *tunnel.TunnelFrame {
	t.Helper()
	dec := tunnel.NewFrameDecoder()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		dec.Reset()
		got, err := dec.Feed(payload(frames()))
		if err != nil {
			t.Fatalf("decode frames: %v", err)
		}
		for _, frame := range got {
			if frame.Type == tunnel.FrameHello && frame.Hello != nil {
				return frame
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("tunnel hello was not observed")
	return nil
}

func TestFlowMuxCarriesSubscriptionIdentity(t *testing.T) {
	registerTestCarrier()
	rec := &frameRecorder{}
	setRecorder(rec)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt, err := app.BuildRuntime(ctx, multiSettings(t, "flowmux"), "client", logging.New("multipipe-test", false))
	if err != nil {
		t.Fatalf("build runtime: %v", err)
	}
	if rt.StreamMux == nil {
		t.Fatal("flowmux runtime has no stream multiplexer")
	}
	if err := rt.StreamMux.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	hello := waitForHello(t, rec.snapshot, func(frames [][]byte) []byte {
		var out []byte
		for _, f := range frames {
			out = append(out, f...)
		}
		return out
	})
	if hello.Hello.UserID != "alice" || hello.Hello.ExitNodeID != "42" {
		t.Fatalf("hello identity = %+v", hello.Hello)
	}
	if err := rt.StreamMux.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

func TestBondCarriesSubscriptionIdentity(t *testing.T) {
	registerTestCarrier()
	rec := &frameRecorder{}
	setRecorder(rec)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rt, err := app.BuildRuntime(ctx, multiSettings(t, "bond"), "client", logging.New("multipipe-test", false))
	if err != nil {
		t.Fatalf("build runtime: %v", err)
	}
	if rt.StreamMux == nil {
		t.Fatal("bond runtime has no stream multiplexer")
	}
	if err := rt.StreamMux.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	hello := waitForHello(t, rec.snapshot, func(frames [][]byte) []byte {
		type bondFrame struct {
			seq  uint32
			data []byte
		}
		var decoded []bondFrame
		for _, raw := range frames {
			if len(raw) < 8 {
				continue
			}
			seq := binary.BigEndian.Uint32(raw[0:4])
			size := int(binary.BigEndian.Uint32(raw[4:8]))
			if size < 0 || 8+size > len(raw) {
				continue
			}
			decoded = append(decoded, bondFrame{seq: seq, data: raw[8 : 8+size]})
		}
		for i := 0; i < len(decoded); i++ {
			for j := i + 1; j < len(decoded); j++ {
				if decoded[j].seq < decoded[i].seq {
					decoded[i], decoded[j] = decoded[j], decoded[i]
				}
			}
		}
		var out []byte
		next := uint32(0)
		for _, f := range decoded {
			if f.seq != next {
				return nil
			}
			out = append(out, f.data...)
			next++
		}
		return out
	})
	if hello.Hello.UserID != "alice" || hello.Hello.ExitNodeID != "42" {
		t.Fatalf("hello identity = %+v", hello.Hello)
	}
	if err := rt.StreamMux.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

var _ transport.Transport = (*recordingTransport)(nil)
