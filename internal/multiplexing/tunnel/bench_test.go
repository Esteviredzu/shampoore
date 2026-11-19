package tunnel

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

func BenchmarkEncodeDecodeFrame(b *testing.B) {
	id := StreamID(uuid.New())
	payload := make([]byte, 12*1024)
	frame := &TunnelFrame{
		StreamID: id,
		Type:     FrameData,
		Epoch:    42,
		Data:     payload,
	}

	b.Run("encode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(payload)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := EncodeFrame(frame); err != nil {
				b.Fatalf("encode: %v", err)
			}
		}
	})

	const framesPerBatch = 32
	batch := make([]byte, 0, framesPerBatch*(FrameHeaderSize+len(payload)))
	for i := 0; i < framesPerBatch; i++ {
		encoded, err := EncodeFrame(frame)
		if err != nil {
			b.Fatalf("encode batch frame: %v", err)
		}
		batch = append(batch, encoded...)
	}

	b.Run("decode_batch", func(b *testing.B) {
		decoder := NewFrameDecoder()
		b.ReportAllocs()
		b.SetBytes(int64(len(batch)))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			decoder.Reset()
			frames, err := decoder.Feed(batch)
			if err != nil {
				b.Fatalf("decode: %v", err)
			}
			if len(frames) != framesPerBatch {
				b.Fatalf("expected %d frames, got %d", framesPerBatch, len(frames))
			}
		}
	})
}

func BenchmarkMultiplexerThroughput(b *testing.B) {
	for _, chunkSize := range []int{1 << 10, 8 << 10, 32 << 10} {
		b.Run(fmt.Sprintf("chunk_%d", chunkSize), func(b *testing.B) {
			tr := newFakeTransport()
			var sent atomic.Int64
			tr.setSendFunc(func(_ context.Context, data []byte) error {
				sent.Add(int64(len(data)))
				return nil
			})

			cfg := Config{
				FlushThreshold: chunkSize,
				FlushInterval:  time.Millisecond,
			}
			m := New(tr, testLogger{}, cfg)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := m.Start(ctx); err != nil {
				b.Fatalf("start: %v", err)
			}
			defer func() {
				if err := m.Stop(context.Background()); err != nil {
					b.Errorf("stop: %v", err)
				}
			}()

			stream, err := m.OpenStream(ctx, map[string]any{"host": "bench", "port": 0})
			if err != nil {
				b.Fatalf("open stream: %v", err)
			}

			buf := make([]byte, chunkSize)
			b.ReportAllocs()
			b.SetBytes(int64(chunkSize))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if _, err := stream.Write(buf); err != nil {
					b.Fatalf("write: %v", err)
				}
			}

			b.StopTimer()
			if err := m.flushOutbound(context.Background()); err != nil {
				b.Fatalf("flush: %v", err)
			}
			if got := sent.Load(); got < int64(b.N*chunkSize) {
				b.Fatalf("sent %d bytes, expected at least %d", got, b.N*chunkSize)
			}
			if err := stream.Close(); err != nil {
				b.Fatalf("close stream: %v", err)
			}
		})
	}
}
