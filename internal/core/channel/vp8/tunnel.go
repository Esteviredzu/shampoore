package vp8

import (
	"context"
	"errors"
	"fmt"
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

	incomingBufferSize = 4096

	pliBaseInterval  = 100 * time.Millisecond
	pliMaxInterval   = 3 * time.Second
	pliResetAfter    = 5 * time.Second
	pliMaxGapPackets = 2048
	reconnectGap     = 3 * time.Second
)

type Tunnel struct {
	trackMu      sync.RWMutex
	pubTrack     *webrtc.TrackLocalStaticSample
	sharingTrack *webrtc.TrackLocalStaticSample

	remoteMu     sync.Mutex
	remoteTracks map[webrtc.SSRC]bool

	incoming chan []byte
	errors   chan error

	sessionErr chan error

	gen atomic.Int64

	closed  chan struct{}
	writeMu sync.Mutex

	pliCh    chan struct{}
	pliHook  func(webrtc.SSRC) error
	pliBase  time.Duration
	pliMax   time.Duration
	pliReset time.Duration

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
		sessionErr:   make(chan error, 1),
		closed:       make(chan struct{}),
		pliCh:        make(chan struct{}, 1),
	}
	if subPC != nil {
		t.AttachSubPC(subPC)
	}
	return t, nil
}

func (t *Tunnel) Errors() <-chan error { return t.sessionErr }

func (t *Tunnel) InvalidateSession() {
	t.gen.Add(1)
	for {
		select {
		case <-t.sessionErr:
			continue
		default:
		}
		break
	}
}

func (t *Tunnel) Rebind(subPC *webrtc.PeerConnection, pubTrack *webrtc.TrackLocalStaticSample) error {
	if pubTrack == nil {
		return errors.New("vp8: nil publisher track")
	}
	select {
	case <-t.closed:
		return errors.New("vp8: tunnel closed")
	default:
	}

	t.trackMu.Lock()
	t.pubTrack = pubTrack
	t.trackMu.Unlock()

	if subPC != nil {
		t.AttachSubPC(subPC)
	} else {
		t.gen.Add(1)
		t.remoteMu.Lock()
		t.remoteTracks = make(map[webrtc.SSRC]bool)
		t.remoteMu.Unlock()
	}

	t.fireReconnect()
	return nil
}

func (t *Tunnel) publisherTrack() *webrtc.TrackLocalStaticSample {
	t.trackMu.RLock()
	defer t.trackMu.RUnlock()
	return t.pubTrack
}

func (t *Tunnel) sharingTrackRef() *webrtc.TrackLocalStaticSample {
	t.trackMu.RLock()
	defer t.trackMu.RUnlock()
	return t.sharingTrack
}

func (t *Tunnel) reportError(gen int64, err error) {
	if err == nil || t.gen.Load() != gen {
		return
	}
	select {
	case <-t.closed:
		return
	default:
	}
	select {
	case t.sessionErr <- err:
	default:
	}
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
	if subPC == nil {
		return
	}

	gen := t.gen.Add(1)
	t.remoteMu.Lock()
	t.remoteTracks = make(map[webrtc.SSRC]bool)
	t.remoteMu.Unlock()

	subPC.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		mime := remote.Codec().MimeType
		ssrc := remote.SSRC()
		log.Printf("[vp8] OnTrack: %s ssrc=%d", mime, ssrc)

		if mime != webrtc.MimeTypeVP8 {
			go drainTrack(remote)
			return
		}

		if !t.bindRemoteTrack(remote, subPC, gen) {
			log.Printf("[vp8] OnTrack: ssrc=%d already bound, ignoring", ssrc)
			return
		}
	})
}

func (t *Tunnel) bindRemoteTrack(remote *webrtc.TrackRemote, pc *webrtc.PeerConnection, gen int64) bool {
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

	go t.pliLoop(ssrc, pc, gen)
	go t.readLoop(remote, gen)
	return true
}

func (t *Tunnel) requestPLI() {
	if t.pliCh == nil {
		return
	}
	select {
	case t.pliCh <- struct{}{}:
	default:
	}
}

func (t *Tunnel) writePLI(pc *webrtc.PeerConnection, ssrc webrtc.SSRC) error {
	if t.pliHook != nil {
		return t.pliHook(ssrc)
	}
	if pc == nil {
		return nil
	}
	pli := &rtcp.PictureLossIndication{MediaSSRC: uint32(ssrc)}
	return pc.WriteRTCP([]rtcp.Packet{pli})
}

func (t *Tunnel) pliLoop(ssrc webrtc.SSRC, pc *webrtc.PeerConnection, gen int64) {
	base := t.pliBase
	if base <= 0 {
		base = pliBaseInterval
	}
	maxDelay := t.pliMax
	if maxDelay < base {
		maxDelay = pliMaxInterval
	}
	resetAfter := t.pliReset
	if resetAfter <= 0 {
		resetAfter = pliResetAfter
	}

	delay := base
	var last time.Time
	for {
		select {
		case <-t.closed:
			return
		case <-t.pliCh:
		}

		now := time.Now()
		if !last.IsZero() {
			since := now.Sub(last)
			if since < delay {

				continue
			}
			if since >= resetAfter {
				delay = base
			} else {
				delay *= 2
				if delay > maxDelay {
					delay = maxDelay
				}
			}
		}

		if err := t.writePLI(pc, ssrc); err != nil {
			log.Printf("[vp8] PLI error: %v", err)
			t.reportError(gen, fmt.Errorf("vp8: PLI write: %w", err))
			return
		}
		last = now
	}
}

func (t *Tunnel) SetSharingTrack(track *webrtc.TrackLocalStaticSample) {
	t.trackMu.Lock()
	t.sharingTrack = track
	t.trackMu.Unlock()
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

	track := t.publisherTrack()
	if track == nil {
		return errors.New("vp8: publisher track not set")
	}

	done := make(chan error, 1)
	go func() {
		t.writeMu.Lock()
		defer t.writeMu.Unlock()
		done <- track.WriteSample(media.Sample{Data: data, Duration: frameDur})
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

	track := t.sharingTrackRef()
	if track == nil {
		return errors.New("vp8: sharing track not set")
	}
	if len(data) > vp8MaxPayload {
		return errors.New("vp8: payload too large")
	}

	done := make(chan error, 1)
	go func() {
		t.writeMu.Lock()
		defer t.writeMu.Unlock()
		done <- track.WriteSample(media.Sample{Data: data, Duration: frameDur})
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
	t.closeOnce.Do(func() {
		t.gen.Add(1)
		close(t.closed)
	})
	return nil
}

func isVP8Keyframe(data []byte) bool {
	if len(data) < 1 {
		return false
	}
	return (data[0] & 0x01) == 0
}

func rtpSequenceGap(prev, cur uint16) (uint16, bool) {
	diff := cur - prev
	if diff <= 1 || diff > uint16(pliMaxGapPackets) {
		return 0, false
	}
	return diff - 1, true
}

func (t *Tunnel) readLoop(remote *webrtc.TrackRemote, gen int64) {
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

	var (
		lastTimestamp uint32
		haveTimestamp bool
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
			t.reportError(gen, fmt.Errorf("vp8: track read: %w", err))
			return
		}
		rtpCount++

		if haveLastSeq && pkt.SequenceNumber != lastSeq+1 {
			if _, gap := rtpSequenceGap(lastSeq, pkt.SequenceNumber); gap {
				t.requestPLI()
			}
			if frameStarted {
				frameHasGap = true
			}
		}
		lastSeq = pkt.SequenceNumber
		haveLastSeq = true

		if haveTimestamp && pkt.Timestamp != lastTimestamp && frameStarted {
			frame = frameBuf[:0]
			frameStarted = false
			frameHasGap = false
		}
		lastTimestamp = pkt.Timestamp
		haveTimestamp = true

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
				t.requestPLI()
				continue
			}
			seenKeyframe = true
			log.Printf("[vp8] GOT KEYFRAME after %d dropped frames, size=%d",
				droppedUntilKeyframe, len(frame))
		}

		if frameHasGap {
			gapFrames++
			if gapFrames <= 5 || gapFrames%100 == 0 {
				log.Printf("[vp8] DROP gapped frame #%d size=%d (total gaps=%d)",
					totalFrames+1, len(frame), gapFrames)
			}
			continue
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
			if droppedFull == 1 || droppedFull%100 == 0 {
				log.Printf("[vp8] WARNING: incoming channel full (buffer=%d), dropped frame #%d",
					incomingBufferSize, droppedFull)
			}
		}
	}
}
