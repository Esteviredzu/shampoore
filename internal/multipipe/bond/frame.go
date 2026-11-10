package bond

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	frameHeaderSize = 8
	maxFramePayload = 64 * 1024
)

func writeFrame(w io.Writer, seq uint32, data []byte) error {
	if len(data) > maxFramePayload {
		return fmt.Errorf("bond: frame too large: %d", len(data))
	}
	hdr := make([]byte, frameHeaderSize)
	binary.BigEndian.PutUint32(hdr[0:4], seq)
	binary.BigEndian.PutUint32(hdr[4:8], uint32(len(data)))
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

func readFrame(r io.Reader) (uint32, []byte, error) {
	hdr := make([]byte, frameHeaderSize)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return 0, nil, err
	}
	seq := binary.BigEndian.Uint32(hdr[0:4])
	n := binary.BigEndian.Uint32(hdr[4:8])
	if n > maxFramePayload {
		return 0, nil, fmt.Errorf("bond: bad frame len: %d", n)
	}
	data := make([]byte, n)
	if _, err := io.ReadFull(r, data); err != nil {
		return 0, nil, err
	}
	return seq, data, nil
}
