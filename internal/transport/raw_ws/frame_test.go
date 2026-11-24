package raw_ws

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeFrameRoundTrip(t *testing.T) {
	msgs := [][]byte{
		[]byte("hello"),
		bytes.Repeat([]byte{0x42}, 1024),
		[]byte("world"),
	}

	for _, batched := range []bool{false, true} {
		for _, padding := range []bool{false, true} {
			in := msgs
			if !batched {
				in = msgs[:1]
			}
			frame := encodeFrame(in, batched, padding, shapeDefaults.Padding)
			if len(frame) < frameHeaderSize {
				t.Fatalf("batched=%v padding=%v: short frame %d", batched, padding, len(frame))
			}
			got, ok := decodeFrame(frame)
			if !ok {
				t.Fatalf("batched=%v padding=%v: decode failed", batched, padding)
			}
			if len(got) != len(in) {
				t.Fatalf("batched=%v padding=%v: decoded %d messages, want %d",
					batched, padding, len(got), len(in))
			}
			for i := range in {
				if !bytes.Equal(got[i], in[i]) {
					t.Fatalf("batched=%v padding=%v: message %d mismatch", batched, padding, i)
				}
			}
		}
	}
}

func TestRandomPadLenBounds(t *testing.T) {
	for i := 0; i < 1000; i++ {
		pad := randomPadLen(10)
		if pad < minPadTotal-10 || pad > minPadTotal+maxPadExtra-10 {
			t.Fatalf("pad length %d is out of bounds", pad)
		}
	}
}
