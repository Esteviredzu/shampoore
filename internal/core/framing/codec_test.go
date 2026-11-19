package framing

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeFrameYUVInto(t *testing.T) {
	p, err := NewPalette(4)
	if err != nil {
		t.Fatalf("palette: %v", err)
	}

	const (
		width    = 96
		height   = 80
		cols     = 8
		rows     = 8
		cellSize = 8
	)
	payloadBytes := (cols*rows*p.BitsPerCell + 7) / 8
	payload := make([]byte, payloadBytes)
	for i := range payload {
		payload[i] = byte(i*37 + 5)
	}

	yuv := bytes.Repeat([]byte{0xFF}, YUVSize(width, height))
	EncodeFrameYUV(yuv, width, height, cols, rows, cellSize, payload, p)

	activeW := cols * cellSize
	activeH := rows * cellSize
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			if x < activeW && y < activeH {
				continue
			}
			if got := yuv[y*width+x]; got != p.IdleBGY {
				t.Fatalf("margin pixel (%d,%d) = %d, want idle %d",
					x, y, got, p.IdleBGY)
			}
		}
	}

	dst := make([]byte, payloadBytes)
	decoded := DecodeFrameYUVInto(dst, yuv, width, cols, rows, cellSize, payloadBytes, p)
	if decoded == nil {
		t.Fatal("decode returned nil")
	}
	if !bytes.Equal(decoded, payload) {
		t.Fatalf("decoded payload mismatch:\n got %x\nwant %x", decoded, payload)
	}

	idleYUV := bytes.Repeat([]byte{p.IdleBGY}, YUVSize(width, height))
	decoded = DecodeFrameYUVInto(dst, idleYUV, width, cols, rows, cellSize, payloadBytes, p)
	if decoded == nil {
		t.Fatal("idle decode returned nil")
	}
	if !bytes.Equal(decoded, make([]byte, payloadBytes)) {
		t.Fatalf("idle decode left stale bytes: %x", decoded)
	}
}

func TestEncodeFrameYUVShortPayloadPaintsIdle(t *testing.T) {
	p, err := NewPalette(8)
	if err != nil {
		t.Fatalf("palette: %v", err)
	}

	const (
		width    = 32
		height   = 32
		cols     = 4
		rows     = 4
		cellSize = 8
	)
	yuv := bytes.Repeat([]byte{0xFF}, YUVSize(width, height))
	EncodeFrameYUV(yuv, width, height, cols, rows, cellSize, []byte{0x00}, p)

	for i := 0; i < width*height; i++ {
		if yuv[i] != p.IdleBGY {
			t.Fatalf("pixel %d = %d, want idle %d", i, yuv[i], p.IdleBGY)
		}
	}
}
