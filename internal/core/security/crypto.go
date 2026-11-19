package security

import (
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

const (
	NonceSize = chacha20poly1305.NonceSizeX
	TagSize   = 16
	KeySize   = 32

	SaltSize = 16

	SecureHeaderSize = 4 + 1 + 1 + SaltSize + 8

	ReplayWindowSize = 256

	replayWindowWords = ReplayWindowSize / 64
)

const (
	InfoC2S = "shampoore-c2s"
	InfoS2C = "shampoore-s2c"
)

var (
	legacyHKDFSalt = []byte("yandex-board-vpn/v1/secure-pipe")
	legacyHKDFInfo = []byte("xchacha20poly1305")
)

func NewSessionSalt() ([]byte, error) {
	salt := make([]byte, SaltSize)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("security: generate session salt: %w", err)
	}
	return salt, nil
}

func DeriveKeyWithSalt(password string, salt []byte, info string) ([]byte, error) {
	if password == "" {
		return nil, errors.New("password must be non-empty")
	}
	if len(salt) == 0 {
		return nil, errors.New("salt must be non-empty")
	}
	if info == "" {
		return nil, errors.New("hkdf info must be non-empty")
	}
	r := hkdf.New(sha256.New, []byte(password), salt, []byte(info))
	key := make([]byte, KeySize)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, err
	}
	return key, nil
}

func DeriveKey(password string) ([]byte, error) {
	if password == "" {
		return nil, errors.New("password must be non-empty")
	}
	r := hkdf.New(sha256.New, []byte(password), legacyHKDFSalt, legacyHKDFInfo)
	key := make([]byte, KeySize)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, err
	}
	return key, nil
}

func Encrypt(key, plaintext []byte) ([]byte, error) {
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(nonce)+len(plaintext)+aead.Overhead())
	out = append(out, nonce...)
	out = aead.Seal(out, nonce, plaintext, nil)
	return out, nil
}

func Decrypt(key, payload []byte) ([]byte, error) {
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}
	ns := aead.NonceSize()
	if len(payload) < ns+aead.Overhead() {
		return nil, errors.New("ciphertext too short")
	}
	nonce := payload[:ns]
	ct := payload[ns:]
	return aead.Open(nil, nonce, ct, nil)
}

type replayWindow struct {
	max  uint64
	bits [replayWindowWords]uint64
	init bool
}

func (w *replayWindow) checkAndMark(seq uint64) bool {
	if !w.init {
		w.init = true
		w.max = seq
		w.bits = [replayWindowWords]uint64{}
		w.bits[0] = 1
		return true
	}

	if seq > w.max {
		w.advance(seq - w.max)
		w.max = seq
		w.bits[0] |= 1
		return true
	}

	diff := w.max - seq
	if diff >= ReplayWindowSize {
		return false
	}
	word := diff / 64
	bit := uint(diff % 64)
	mask := uint64(1) << bit
	if w.bits[word]&mask != 0 {
		return false
	}
	w.bits[word] |= mask
	return true
}

func (w *replayWindow) advance(n uint64) {
	if n == 0 {
		return
	}
	if n >= ReplayWindowSize {
		w.bits = [replayWindowWords]uint64{}
		return
	}

	wordShift := int(n / 64)
	bitShift := uint(n % 64)
	for i := replayWindowWords - 1; i >= 0; i-- {
		src := i - wordShift
		var v uint64
		if src >= 0 {
			v = w.bits[src] << bitShift
			if bitShift != 0 && src-1 >= 0 {
				v |= w.bits[src-1] >> (64 - bitShift)
			}
		}
		w.bits[i] = v
	}
}
