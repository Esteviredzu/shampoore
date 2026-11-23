package tunnel

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
)

var frameMagic = [4]byte{'T', 'U', 'N', '1'}

const (
	frameVersion byte = 2

	frameTypeOpen  byte = 1
	frameTypeData  byte = 2
	frameTypeClose byte = 3
	frameTypeReset byte = 4
	frameTypeHello byte = 5
	frameTypeError byte = 6

	frameTypePing    byte = 7
	frameTypePong    byte = 8
	frameTypeOpenAck byte = 9

	FrameHeaderSize = 30

	frameOffMagic    = 0
	frameOffVersion  = 4
	frameOffType     = 5
	frameOffStreamID = 6
	frameOffLength   = 22
	frameOffEpoch    = 26
)

type StreamID [16]byte

type FrameType byte

const (
	FrameOpen  FrameType = FrameType(frameTypeOpen)
	FrameData  FrameType = FrameType(frameTypeData)
	FrameClose FrameType = FrameType(frameTypeClose)

	FrameReset FrameType = FrameType(frameTypeReset)
	FrameHello FrameType = FrameType(frameTypeHello)
	FrameError FrameType = FrameType(frameTypeError)

	FramePing    FrameType = FrameType(frameTypePing)
	FramePong    FrameType = FrameType(frameTypePong)
	FrameOpenAck FrameType = FrameType(frameTypeOpenAck)
)

type Hello struct {
	UserID     string `json:"user_id,omitempty"`
	ExitNodeID string `json:"exit_node_id,omitempty"`
}

type ErrorFrame struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type TunnelFrame struct {
	StreamID StreamID
	Type     FrameType
	Data     []byte
	Metadata map[string]any

	Hello *Hello
	Error *ErrorFrame

	Epoch uint32
}

func EncodeFrame(f *TunnelFrame) ([]byte, error) {
	var payload []byte

	switch f.Type {
	case FrameOpen:
		if len(f.Data) > 0 {
			return nil, fmt.Errorf("tunnel: open frame cannot contain data")
		}
		meta := f.Metadata
		if meta == nil {
			meta = map[string]any{}
		}
		b, err := json.Marshal(meta)
		if err != nil {
			return nil, fmt.Errorf("tunnel: marshal open metadata: %w", err)
		}
		payload = b

	case FrameData:
		payload = f.Data

	case FrameClose:
		if len(f.Data) > 0 {
			return nil, fmt.Errorf("tunnel: close frame cannot contain data")
		}
		if len(f.Metadata) > 0 {
			return nil, fmt.Errorf("tunnel: close frame cannot contain metadata")
		}
		payload = nil

	case FrameReset:
		if len(f.Data) > 0 {
			return nil, fmt.Errorf("tunnel: reset frame cannot contain data")
		}
		if len(f.Metadata) > 0 {
			return nil, fmt.Errorf("tunnel: reset frame cannot contain metadata")
		}
		payload = nil

	case FrameHello:
		if len(f.Data) > 0 {
			return nil, fmt.Errorf("tunnel: hello frame cannot contain data")
		}
		if len(f.Metadata) > 0 {
			return nil, fmt.Errorf("tunnel: hello frame cannot contain metadata")
		}
		hello := f.Hello
		if hello == nil {
			hello = &Hello{}
		}
		b, err := json.Marshal(hello)
		if err != nil {
			return nil, fmt.Errorf("tunnel: marshal hello: %w", err)
		}
		payload = b

	case FrameError:
		if len(f.Data) > 0 {
			return nil, fmt.Errorf("tunnel: error frame cannot contain data")
		}
		if len(f.Metadata) > 0 {
			return nil, fmt.Errorf("tunnel: error frame cannot contain metadata")
		}
		errInfo := f.Error
		if errInfo == nil {
			errInfo = &ErrorFrame{}
		}
		b, err := json.Marshal(errInfo)
		if err != nil {
			return nil, fmt.Errorf("tunnel: marshal error frame: %w", err)
		}
		payload = b

	case FramePing, FramePong, FrameOpenAck:
		if len(f.Data) > 0 {
			return nil, fmt.Errorf("tunnel: control frame cannot contain data")
		}
		if len(f.Metadata) > 0 {
			return nil, fmt.Errorf("tunnel: control frame cannot contain metadata")
		}
		payload = nil

	default:
		return nil, fmt.Errorf("tunnel: unknown frame type: %d", byte(f.Type))
	}

	out := make([]byte, FrameHeaderSize+len(payload))
	copy(out[frameOffMagic:frameOffMagic+4], frameMagic[:])
	out[frameOffVersion] = frameVersion
	out[frameOffType] = byte(f.Type)
	copy(out[frameOffStreamID:frameOffStreamID+len(f.StreamID)], f.StreamID[:])
	binary.BigEndian.PutUint32(out[frameOffLength:frameOffLength+4], uint32(len(payload)))
	binary.BigEndian.PutUint32(out[frameOffEpoch:frameOffEpoch+4], f.Epoch)
	copy(out[FrameHeaderSize:], payload)
	return out, nil
}

type FrameDecoder struct {
	mu  sync.Mutex
	buf []byte
	off int
}

func NewFrameDecoder() *FrameDecoder {
	return &FrameDecoder{buf: make([]byte, 0, 4096)}
}

func (d *FrameDecoder) Reset() {
	d.mu.Lock()
	d.buf = d.buf[:0]
	d.off = 0
	d.mu.Unlock()
}

func (d *FrameDecoder) Feed(data []byte) ([]*TunnelFrame, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.off > 0 {
		n := copy(d.buf, d.buf[d.off:])
		d.buf = d.buf[:n]
		d.off = 0
	}
	d.buf = append(d.buf, data...)

	var frames []*TunnelFrame

	for {
		avail := len(d.buf) - d.off
		if avail < FrameHeaderSize {
			break
		}

		base := d.off
		hdr := d.buf[base : base+FrameHeaderSize]

		if !bytes.Equal(hdr[frameOffMagic:frameOffMagic+4], frameMagic[:]) {
			return frames, fmt.Errorf("tunnel: bad magic: %x", hdr[frameOffMagic:frameOffMagic+4])
		}
		if hdr[frameOffVersion] != frameVersion {
			return frames, fmt.Errorf("tunnel: unsupported version: %d", hdr[frameOffVersion])
		}

		ftype := hdr[frameOffType]
		var streamID StreamID
		copy(streamID[:], hdr[frameOffStreamID:frameOffStreamID+len(streamID)])
		payloadSize := binary.BigEndian.Uint32(hdr[frameOffLength : frameOffLength+4])
		epoch := binary.BigEndian.Uint32(hdr[frameOffEpoch : frameOffEpoch+4])

		total := FrameHeaderSize + int(payloadSize)
		if avail < total {
			break
		}

		frame := &TunnelFrame{
			StreamID: streamID,
			Type:     FrameType(ftype),
			Epoch:    epoch,
		}

		payload := d.buf[base+FrameHeaderSize : base+total]

		switch FrameType(ftype) {
		case FrameOpen:
			var meta map[string]any
			if len(payload) > 0 {
				if err := json.Unmarshal(payload, &meta); err != nil {
					return frames, fmt.Errorf("tunnel: decode open metadata: %w", err)
				}
			}
			if meta == nil {
				meta = map[string]any{}
			}
			frame.Metadata = meta

		case FrameData:
			if len(payload) > 0 {

				frame.Data = append([]byte(nil), payload...)
			}

		case FrameClose:
			if len(payload) > 0 {
				return frames, fmt.Errorf("tunnel: close frame has payload")
			}

		case FrameReset:
			if len(payload) > 0 {
				return frames, fmt.Errorf("tunnel: reset frame has payload")
			}

		case FrameHello:
			var hello Hello
			if len(payload) > 0 {
				if err := json.Unmarshal(payload, &hello); err != nil {
					return frames, fmt.Errorf("tunnel: decode hello: %w", err)
				}
			}
			frame.Hello = &hello

		case FrameError:
			var errInfo ErrorFrame
			if len(payload) > 0 {
				if err := json.Unmarshal(payload, &errInfo); err != nil {
					return frames, fmt.Errorf("tunnel: decode error frame: %w", err)
				}
			}
			frame.Error = &errInfo

		case FramePing, FramePong, FrameOpenAck:
			if len(payload) > 0 {
				return frames, fmt.Errorf("tunnel: control frame has payload")
			}

		default:

			d.off = base + total
			continue
		}

		frames = append(frames, frame)
		d.off = base + total
	}

	if d.off > 0 {
		n := copy(d.buf, d.buf[d.off:])
		d.buf = d.buf[:n]
		d.off = 0
	}

	return frames, nil
}
