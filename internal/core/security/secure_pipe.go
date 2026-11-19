package security

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"sync"

	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/transport"
	"golang.org/x/crypto/chacha20poly1305"
)

const (
	RoleClient = "client"
	RoleServer = "server"

	secureVersion  = byte(2)
	secureTypeData = byte(1)

	maxRetiredSalts = 256
)

var secureMagic = [4]byte{'S', 'P', 'V', '2'}

type SecurePipe struct {
	inner    transport.Pipe
	logger   logging.ContextLogger
	password []byte
	role     string

	sendKey  []byte
	sendSalt []byte

	sendMu  sync.Mutex
	sendSeq uint64

	recvMu   sync.Mutex
	recvSalt []byte
	recvKey  []byte
	replay   replayWindow

	retiredSalts map[string]struct{}
	retiredOrder []string

	maxPlaintext int
	ctr          transport.Counters
}

func NewSecurePipe(inner transport.Pipe, password string, log logging.ContextLogger) (*SecurePipe, error) {
	return NewSecurePipeWithRole(inner, password, "", log)
}

func NewSecurePipeWithRole(inner transport.Pipe, password, role string, log logging.ContextLogger) (*SecurePipe, error) {
	if inner == nil {
		return nil, errors.New("secure-pipe: inner pipe is nil")
	}

	role = strings.ToLower(strings.TrimSpace(role))
	if role != RoleClient && role != RoleServer {
		role = ""
	}

	salt, err := NewSessionSalt()
	if err != nil {
		return nil, err
	}

	sendInfo := InfoC2S
	if role == RoleServer {
		sendInfo = InfoS2C
	}
	key, err := DeriveKeyWithSalt(password, salt, sendInfo)
	if err != nil {
		return nil, err
	}

	maxPlain := inner.MaxMessageSize() - SecureHeaderSize - TagSize
	if maxPlain < 1 {
		return nil, errors.New("secure-pipe: inner pipe too small")
	}

	if role == "" && log != nil {
		log.Warn("secure-pipe: role is not set; using legacy direction auto-detection")
	}

	return &SecurePipe{
		inner:        inner,
		logger:       log,
		password:     []byte(password),
		role:         role,
		sendKey:      key,
		sendSalt:     salt,
		retiredSalts: make(map[string]struct{}),
		maxPlaintext: maxPlain,
	}, nil
}

func (p *SecurePipe) Start(ctx context.Context) error { return p.inner.Start(ctx) }
func (p *SecurePipe) Stop(ctx context.Context) error  { return p.inner.Stop(ctx) }

func (p *SecurePipe) NotifyNetworkChanged() {
	if n, ok := p.inner.(interface{ NotifyNetworkChanged() }); ok {
		n.NotifyNetworkChanged()
	}
}
func (p *SecurePipe) SessionID() string   { return p.inner.SessionID() }
func (p *SecurePipe) MaxMessageSize() int { return p.maxPlaintext }

func (p *SecurePipe) Authenticated() bool { return true }

func (p *SecurePipe) SendMessage(ctx context.Context, data []byte, key string) error {
	if len(data) > p.maxPlaintext {
		return errors.New("secure-pipe: plaintext too large")
	}

	p.sendMu.Lock()
	seq := p.sendSeq
	if seq == math.MaxUint64 {
		p.sendMu.Unlock()
		return errors.New("secure-pipe: sequence exhausted; re-establish session")
	}
	p.sendSeq++
	p.sendMu.Unlock()

	header := make([]byte, SecureHeaderSize)
	copy(header[0:4], secureMagic[:])
	header[4] = secureVersion
	header[5] = secureTypeData
	copy(header[6:6+SaltSize], p.sendSalt)
	binary.BigEndian.PutUint64(header[6+SaltSize:SecureHeaderSize], seq)

	aead, err := chacha20poly1305.NewX(p.sendKey)
	if err != nil {
		return err
	}

	blob := make([]byte, 0, SecureHeaderSize+len(data)+aead.Overhead())
	blob = append(blob, header...)
	blob = aead.Seal(blob, nonceForSequence(seq), data, header)

	if err := p.inner.SendMessage(ctx, blob, key); err != nil {
		return err
	}
	p.ctr.UsefulSent.Add(int64(len(data)))
	p.ctr.NetworkSent.Add(int64(len(blob)))
	p.ctr.MessagesSent.Add(1)
	p.ctr.FramesSent.Add(1)
	return nil
}

func (p *SecurePipe) ReceiveMessage(ctx context.Context) ([]byte, error) {
	for {
		blob, err := p.inner.ReceiveMessage(ctx)
		if err != nil {
			return nil, err
		}
		plain, ok := p.open(blob)
		if !ok {
			continue
		}
		p.ctr.UsefulRecv.Add(int64(len(plain)))
		p.ctr.NetworkRecv.Add(int64(len(blob)))
		p.ctr.MessagesRecv.Add(1)
		p.ctr.FramesRecv.Add(1)
		return plain, nil
	}
}

func (p *SecurePipe) open(blob []byte) ([]byte, bool) {
	if len(blob) < SecureHeaderSize+TagSize {
		p.warn("secure-pipe: dropping short message")
		return nil, false
	}
	if blob[0] != secureMagic[0] || blob[1] != secureMagic[1] ||
		blob[2] != secureMagic[2] || blob[3] != secureMagic[3] {
		p.warn("secure-pipe: dropping message with bad magic")
		return nil, false
	}
	if blob[4] != secureVersion || blob[5] != secureTypeData {
		p.warn("secure-pipe: dropping message with unsupported version/type")
		return nil, false
	}

	salt := blob[6 : 6+SaltSize]
	seq := binary.BigEndian.Uint64(blob[6+SaltSize : SecureHeaderSize])
	header := blob[:SecureHeaderSize]
	ciphertext := blob[SecureHeaderSize:]

	if bytes.Equal(salt, p.sendSalt) {
		p.warn("secure-pipe: dropping reflected message")
		return nil, false
	}

	p.recvMu.Lock()
	defer p.recvMu.Unlock()

	var plain []byte
	if len(p.recvSalt) > 0 && bytes.Equal(p.recvSalt, salt) {
		var err error
		plain, err = openWithKey(p.recvKey, header, seq, ciphertext)
		if err != nil {
			p.warn("secure-pipe: dropping undecryptable message: ", err)
			return nil, false
		}
	} else {
		if _, retired := p.retiredSalts[string(salt)]; retired {
			p.warn("secure-pipe: dropping message with a retired session salt")
			return nil, false
		}
		plain2, key, err := p.openWithPeerSalt(salt, header, seq, ciphertext)
		if err != nil {
			p.warn("secure-pipe: dropping undecryptable message: ", err)
			return nil, false
		}
		plain = plain2
		if len(p.recvSalt) > 0 {
			p.retireSaltLocked(p.recvSalt)
		}
		p.recvSalt = append(p.recvSalt[:0], salt...)
		p.recvKey = key

		p.replay = replayWindow{}
	}

	if !p.replay.checkAndMark(seq) {
		p.warn("secure-pipe: dropping replayed message seq=", seq)
		return nil, false
	}
	return plain, true
}

func (p *SecurePipe) openWithPeerSalt(salt, header []byte, seq uint64, ciphertext []byte) ([]byte, []byte, error) {
	var lastErr error
	for _, info := range p.receiveInfos() {
		key, err := DeriveKeyWithSalt(string(p.password), salt, info)
		if err != nil {
			return nil, nil, err
		}
		plain, err := openWithKey(key, header, seq, ciphertext)
		if err == nil {
			return plain, key, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("no receive key candidates")
	}
	return nil, nil, lastErr
}

func (p *SecurePipe) retireSaltLocked(salt []byte) {
	if p.retiredSalts == nil {
		p.retiredSalts = make(map[string]struct{})
	}
	key := string(salt)
	if _, ok := p.retiredSalts[key]; ok {
		return
	}
	p.retiredSalts[key] = struct{}{}
	p.retiredOrder = append(p.retiredOrder, key)
	for len(p.retiredOrder) > maxRetiredSalts {
		oldest := p.retiredOrder[0]
		p.retiredOrder = p.retiredOrder[1:]
		delete(p.retiredSalts, oldest)
	}
}

func (p *SecurePipe) receiveInfos() []string {
	switch p.role {
	case RoleClient:
		return []string{InfoS2C}
	case RoleServer:
		return []string{InfoC2S}
	default:

		return []string{InfoS2C, InfoC2S}
	}
}

func openWithKey(key, header []byte, seq uint64, ciphertext []byte) ([]byte, error) {
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < aead.Overhead() {
		return nil, errors.New("ciphertext too short")
	}
	return aead.Open(nil, nonceForSequence(seq), ciphertext, header)
}

func nonceForSequence(seq uint64) []byte {
	nonce := make([]byte, NonceSize)
	binary.BigEndian.PutUint64(nonce[NonceSize-8:], seq)
	return nonce
}

func (p *SecurePipe) warn(args ...any) {
	if p.logger != nil {
		p.logger.Warn(args...)
	}
}

func (p *SecurePipe) DeleteMessage(ctx context.Context, key string) error {
	return p.inner.DeleteMessage(ctx, key)
}

func (p *SecurePipe) WalkLayers() []transport.Layer {
	out := []transport.Layer{{
		Kind: "security", Type: "xchacha",
		Snapshot: p.ctr.Snapshot(),
	}}
	if tw, ok := p.inner.(transport.Telemetry); ok {
		out = append(out, tw.WalkLayers()...)
	}
	return out
}

var _ transport.Telemetry = (*SecurePipe)(nil)
