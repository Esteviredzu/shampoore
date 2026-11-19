package tunnel

import (
	"bytes"
	"testing"

	"github.com/google/uuid"
)

func TestFrameRoundTripWithEpoch(t *testing.T) {
	id := StreamID(uuid.New())

	cases := []struct {
		name  string
		frame *TunnelFrame
	}{
		{
			name: "open",
			frame: &TunnelFrame{
				StreamID: id,
				Type:     FrameOpen,
				Epoch:    42,
				Metadata: map[string]any{"host": "example.com", "port": float64(443)},
			},
		},
		{
			name:  "data",
			frame: &TunnelFrame{StreamID: id, Type: FrameData, Epoch: 0xdeadbeef, Data: []byte("hello")},
		},
		{
			name:  "close",
			frame: &TunnelFrame{StreamID: id, Type: FrameClose, Epoch: 7},
		},
		{
			name:  "reset",
			frame: &TunnelFrame{StreamID: StreamID{}, Type: FrameReset, Epoch: 99},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := EncodeFrame(tc.frame)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if len(encoded) < FrameHeaderSize {
				t.Fatalf("encoded frame is shorter than header: %d", len(encoded))
			}
			if tc.frame.Type != FrameOpen && len(encoded) != FrameHeaderSize+len(tc.frame.Data) {
				t.Fatalf("unexpected encoded size: %d", len(encoded))
			}

			decoder := NewFrameDecoder()
			frames, err := decoder.Feed(encoded)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if len(frames) != 1 {
				t.Fatalf("expected 1 frame, got %d", len(frames))
			}

			got := frames[0]
			if got.StreamID != tc.frame.StreamID {
				t.Fatalf("stream id mismatch: %q != %q", got.StreamID, tc.frame.StreamID)
			}
			if got.Type != tc.frame.Type {
				t.Fatalf("type mismatch: %d != %d", got.Type, tc.frame.Type)
			}
			if got.Epoch != tc.frame.Epoch {
				t.Fatalf("epoch mismatch: %d != %d", got.Epoch, tc.frame.Epoch)
			}
			if !bytes.Equal(got.Data, tc.frame.Data) {
				t.Fatalf("data mismatch: %q != %q", got.Data, tc.frame.Data)
			}
			if tc.frame.Type == FrameOpen {
				if got.Metadata["host"] != "example.com" {
					t.Fatalf("metadata host mismatch: %v", got.Metadata["host"])
				}
				if got.Metadata["port"] != float64(443) {
					t.Fatalf("metadata port mismatch: %v", got.Metadata["port"])
				}
			}
		})
	}
}

func TestHelloAndErrorFramesRoundTrip(t *testing.T) {
	cases := []*TunnelFrame{
		{
			Type:  FrameHello,
			Epoch: 7,
			Hello: &Hello{UserID: "alice", ExitNodeID: "exit-1"},
		},
		{
			Type:  FrameError,
			Epoch: 8,
			Error: &ErrorFrame{Code: "exit_forbidden", Message: "not allowed"},
		},
	}

	for _, frame := range cases {
		encoded, err := EncodeFrame(frame)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		decoder := NewFrameDecoder()
		frames, err := decoder.Feed(encoded)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(frames) != 1 {
			t.Fatalf("expected 1 frame, got %d", len(frames))
		}
		got := frames[0]
		if got.Type != frame.Type || got.Epoch != frame.Epoch || got.StreamID != frame.StreamID {
			t.Fatalf("frame mismatch: %+v != %+v", got, frame)
		}
		switch frame.Type {
		case FrameHello:
			if got.Hello == nil || got.Hello.UserID != frame.Hello.UserID || got.Hello.ExitNodeID != frame.Hello.ExitNodeID {
				t.Fatalf("hello mismatch: %+v", got.Hello)
			}
		case FrameError:
			if got.Error == nil || got.Error.Code != frame.Error.Code || got.Error.Message != frame.Error.Message {
				t.Fatalf("error mismatch: %+v", got.Error)
			}
		}
	}
}

func TestHelloFrameRejectsDataAndMetadata(t *testing.T) {
	if _, err := EncodeFrame(&TunnelFrame{Type: FrameHello, Data: []byte("x")}); err == nil {
		t.Fatal("hello frame with data was accepted")
	}
	if _, err := EncodeFrame(&TunnelFrame{Type: FrameError, Metadata: map[string]any{"x": 1}}); err == nil {
		t.Fatal("error frame with metadata was accepted")
	}
}

func TestFrameDecoderSplitFeed(t *testing.T) {
	id := StreamID(uuid.New())
	first, err := EncodeFrame(&TunnelFrame{
		StreamID: id,
		Type:     FrameOpen,
		Epoch:    11,
		Metadata: map[string]any{"host": "a"},
	})
	if err != nil {
		t.Fatalf("encode open: %v", err)
	}
	second, err := EncodeFrame(&TunnelFrame{
		StreamID: id,
		Type:     FrameData,
		Epoch:    11,
		Data:     []byte("payload"),
	})
	if err != nil {
		t.Fatalf("encode data: %v", err)
	}

	stream := append(append([]byte(nil), first...), second...)
	decoder := NewFrameDecoder()

	var frames []*TunnelFrame
	for i := range stream {
		got, err := decoder.Feed(stream[i : i+1])
		if err != nil {
			t.Fatalf("feed byte %d: %v", i, err)
		}
		frames = append(frames, got...)
	}

	if len(frames) != 2 {
		t.Fatalf("expected 2 frames, got %d", len(frames))
	}
	if frames[0].Type != FrameOpen || frames[1].Type != FrameData {
		t.Fatalf("unexpected frame types: %d, %d", frames[0].Type, frames[1].Type)
	}
	if string(frames[1].Data) != "payload" {
		t.Fatalf("unexpected payload: %q", frames[1].Data)
	}
}

func TestFrameDecoderBatch(t *testing.T) {
	id := StreamID(uuid.New())
	const count = 32

	var stream []byte
	want := make([][]byte, count)
	for i := 0; i < count; i++ {
		payload := bytes.Repeat([]byte{byte(i)}, 64+i)
		want[i] = payload
		encoded, err := EncodeFrame(&TunnelFrame{
			StreamID: id,
			Type:     FrameData,
			Epoch:    uint32(i + 1),
			Data:     payload,
		})
		if err != nil {
			t.Fatalf("encode frame %d: %v", i, err)
		}
		stream = append(stream, encoded...)
	}

	decoder := NewFrameDecoder()
	split := len(stream) / 2
	frames, err := decoder.Feed(stream[:split])
	if err != nil {
		t.Fatalf("feed first half: %v", err)
	}
	rest, err := decoder.Feed(stream[split:])
	if err != nil {
		t.Fatalf("feed second half: %v", err)
	}
	frames = append(frames, rest...)

	if len(frames) != count {
		t.Fatalf("expected %d frames, got %d", count, len(frames))
	}
	for i, frame := range frames {
		if frame.Type != FrameData {
			t.Fatalf("frame %d: unexpected type %d", i, frame.Type)
		}
		if frame.Epoch != uint32(i+1) {
			t.Fatalf("frame %d: epoch %d != %d", i, frame.Epoch, i+1)
		}
		if !bytes.Equal(frame.Data, want[i]) {
			t.Fatalf("frame %d: payload mismatch", i)
		}
	}

	extra, err := EncodeFrame(&TunnelFrame{
		StreamID: id,
		Type:     FrameData,
		Epoch:    uint32(count + 1),
		Data:     bytes.Repeat([]byte{0xaa}, 4096),
	})
	if err != nil {
		t.Fatalf("encode extra: %v", err)
	}
	if _, err := decoder.Feed(extra); err != nil {
		t.Fatalf("feed extra: %v", err)
	}
	for i, frame := range frames {
		if !bytes.Equal(frame.Data, want[i]) {
			t.Fatalf("frame %d payload changed after subsequent feed", i)
		}
	}
}
func TestFrameDecoderRejectsOldVersion(t *testing.T) {

	encoded, err := EncodeFrame(&TunnelFrame{
		StreamID: StreamID(uuid.New()),
		Type:     FrameClose,
		Epoch:    1,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	encoded[frameOffVersion] = frameVersion - 1

	decoder := NewFrameDecoder()
	if _, err := decoder.Feed(encoded); err == nil {
		t.Fatal("expected unsupported version error")
	}
}
