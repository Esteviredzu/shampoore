package vp8

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

const (
	frameDur = 33 * time.Millisecond

	writeSampleTimeout = 5 * time.Second
	vp8MaxPayload      = (1 << 19) - 1 - 7

	incomingBufferSize = 1024

	pliInterval  = 500 * time.Millisecond
	reconnectGap = 3 * time.Second
)

type Tunnel struct {
	pubTrack     *webrtc.TrackLocalStaticSample
	sharingTrack *webrtc.TrackLocalStaticSample

	remoteMu     sync.Mutex
	remoteTracks map[webrtc.SSRC]bool

	incoming chan []byte
	errors   chan error
	closed   chan struct{}

	writeMu     sync.Mutex
	lastWriteAt atomic.Int64

	reconnectMu sync.RWMutex
	onReconnect func()

	closeOnce sync.Once
}

func AttachTunnel(
	pubPC, subPC *webrtc.PeerConnection,
	track *webrtc.TrackLocalStaticSample,
) (*Tunnel, error) {
	t := &Tunnel{
		pubTrack:     track,
		remoteTracks: make(map[webrtc.SSRC]bool),
		incoming:     make(chan []byte, incomingBufferSize),
		errors:       make(chan error, 1),
		closed:       make(chan struct{}),
	}
	if subPC != nil {
		t.AttachSubPC(subPC)
	}
	return t, nil
}

func (t *Tunnel) SetReconnectCallback(fn func()) {
	t.reconnectMu.Lock()
	t.onReconnect = fn
	t.reconnectMu.Unlock()
}

func (t *Tunnel) fireReconnect() {
	t.reconnectMu.RLock()
	fn := t.onReconnect
	t.reconnectMu.RUnlock()
	if fn != nil {
		fn()
	}
}

func (t *Tunnel) AttachSubPC(subPC *webrtc.PeerConnection) {
	subPC.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		mime := remote.Codec().MimeType
		ssrc := remote.SSRC()
		log.Printf("[vp8] OnTrack: %s ssrc=%d", mime, ssrc)

		if mime != webrtc.MimeTypeVP8 {
			go drainTrack(remote)
			return
		}

		if !t.bindRemoteTrack(remote, subPC) {
			log.Printf("[vp8] OnTrack: ssrc=%d already bound, ignoring", ssrc)
			return
		}
	})
}

func (t *Tunnel) bindRemoteTrack(remote *webrtc.TrackRemote, pc *webrtc.PeerConnection) bool {
	ssrc := remote.SSRC()
	t.remoteMu.Lock()
	if t.remoteTracks[ssrc] {
		t.remoteMu.Unlock()
		return false
	}
	t.remoteTracks[ssrc] = true
	t.remoteMu.Unlock()

	log.Printf("[vp8] bindRemoteTrack: ssrc=%d stream=%s id=%s",
		ssrc, remote.StreamID(), remote.ID())

	go t.pliLoop(ssrc, pc)
	go t.readLoop(remote)
	return true
}

func (t *Tunnel) pliLoop(ssrc webrtc.SSRC, pc *webrtc.PeerConnection) {
	ticker := time.NewTicker(pliInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.closed:
			return
		case <-ticker.C:
		}

		if pc == nil {
			return
		}
		pli := &rtcp.PictureLossIndication{
			MediaSSRC: uint32(ssrc),
		}
		if err := pc.WriteRTCP([]rtcp.Packet{pli}); err != nil {
			log.Printf("[vp8] PLI error: %v", err)
			return
		}
	}
}

func (t *Tunnel) SetSharingTrack(track *webrtc.TrackLocalStaticSample) {
	t.sharingTrack = track
}

func drainTrack(remote *webrtc.TrackRemote) {
	for {
		if _, _, err := remote.ReadRTP(); err != nil {
			return
		}
	}
}

func (t *Tunnel) SetRawMode(_ bool) {}

func (t *Tunnel) WriteRawVP8(data []byte) error {
	select {
	case <-t.closed:
		return errors.New("vp8: tunnel closed")
	default:
	}

	if len(data) > vp8MaxPayload {
		return errors.New("vp8: payload too large")
	}

	done := make(chan error, 1)
	go func() {
		t.writeMu.Lock()
		defer t.writeMu.Unlock()
		done <- t.pubTrack.WriteSample(media.Sample{Data: data, Duration: frameDur})
	}()

	select {
	case err := <-done:
		if err == nil {
			t.lastWriteAt.Store(time.Now().UnixNano())
		}
		return err
	case <-time.After(writeSampleTimeout):
		return errors.New("vp8: write sample timeout")
	case <-t.closed:
		return errors.New("vp8: tunnel closed")
	}
}

func (t *Tunnel) WriteSharingVP8(data []byte) error {
	select {
	case <-t.closed:
		return errors.New("vp8: tunnel closed")
	default:
	}

	if t.sharingTrack == nil {
		return errors.New("vp8: sharing track not set")
	}
	if len(data) > vp8MaxPayload {
		return errors.New("vp8: payload too large")
	}

	done := make(chan error, 1)
	go func() {
		t.writeMu.Lock()
		defer t.writeMu.Unlock()
		done <- t.sharingTrack.WriteSample(media.Sample{Data: data, Duration: frameDur})
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(writeSampleTimeout):
		return errors.New("vp8: sharing write sample timeout")
	case <-t.closed:
		return errors.New("vp8: tunnel closed")
	}
}

func (t *Tunnel) Receive(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.closed:
		return nil, errors.New("vp8: tunnel closed")
	case err := <-t.errors:
		return nil, err
	case data := <-t.incoming:
		return data, nil
	}
}

func (t *Tunnel) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

func isVP8Keyframe(data []byte) bool {
	if len(data) < 1 {
		return false
	}
	return (data[0] & 0x01) == 0
}

func (t *Tunnel) readLoop(remote *webrtc.TrackRemote) {
	var vp8 codecs.VP8Packet

	frameBuf := make([]byte, 0, 256*1024)
	frame := frameBuf[:0]
	frameStarted := false

	var (
		totalFrames          int
		rtpCount             int
		droppedUntilKeyframe int
		seenKeyframe         bool
		droppedFull          int
		gapFrames            int
	)

	var (
		lastSeq     uint16
		haveLastSeq bool
		frameHasGap bool
	)

	var lastFrameTime time.Time

	for {
		select {
		case <-t.closed:
			return
		default:
		}

		pkt, _, err := remote.ReadRTP()
		if err != nil {
			return
		}
		rtpCount++

		if haveLastSeq && pkt.SequenceNumber != lastSeq+1 {
			if frameStarted {
				frameHasGap = true
			}
		}
		lastSeq = pkt.SequenceNumber
		haveLastSeq = true

		payload, err := vp8.Unmarshal(pkt.Payload)
		if err != nil {
			continue
		}

		if vp8.S == 1 {
			frame = frameBuf[:0]
			frameStarted = true
			frameHasGap = false
		}
		if !frameStarted {
			continue
		}

		frame = append(frame, payload...)

		if !pkt.Marker {
			continue
		}

		frameStarted = false
		if len(frame) == 0 {
			continue
		}

		isKey := isVP8Keyframe(frame)

		if !lastFrameTime.IsZero() && isKey && time.Since(lastFrameTime) > reconnectGap {
			log.Printf("[vp8] reconnect detected: %.2fs gap + keyframe",
				time.Since(lastFrameTime).Seconds())
			seenKeyframe = false
			t.fireReconnect()
		}
		lastFrameTime = time.Now()

		if !seenKeyframe {
			if !isKey {
				droppedUntilKeyframe++
				continue
			}
			seenKeyframe = true
			log.Printf("[vp8] GOT KEYFRAME after %d dropped frames, size=%d",
				droppedUntilKeyframe, len(frame))
		}

		if frameHasGap {
			gapFrames++
		}

		totalFrames++
		if totalFrames <= 5 {
			log.Printf("[vp8] recv frame #%d size=%d keyframe=%v (rtp=%d gaps=%d/%d ch_drops=%d)",
				totalFrames, len(frame), isKey,
				rtpCount, gapFrames, totalFrames, droppedFull)
		}

		full := make([]byte, len(frame))
		copy(full, frame)

		select {
		case t.incoming <- full:
		case <-t.closed:
			return
		default:
			droppedFull++
			if droppedFull == 1 {
				log.Printf("[vp8] WARNING: incoming channel full, dropped first frame")
			}
		}
	}
}