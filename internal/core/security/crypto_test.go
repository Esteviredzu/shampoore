package security

import (
	"bytes"
	"testing"
)

func TestDeriveKeyDirectionsDiffer(t *testing.T) {
	salt, err := NewSessionSalt()
	if err != nil {
		t.Fatalf("NewSessionSalt: %v", err)
	}

	c2s, err := DeriveKeyWithSalt("secret", salt, InfoC2S)
	if err != nil {
		t.Fatalf("derive c2s: %v", err)
	}
	s2c, err := DeriveKeyWithSalt("secret", salt, InfoS2C)
	if err != nil {
		t.Fatalf("derive s2c: %v", err)
	}
	if bytes.Equal(c2s, s2c) {
		t.Fatal("c2s and s2c keys are identical")
	}

	ciphertext, err := Encrypt(c2s, []byte("client to server"))
	if err != nil {
		t.Fatalf("encrypt with c2s key: %v", err)
	}
	if _, err := Decrypt(s2c, ciphertext); err == nil {
		t.Fatal("c2s packet was decrypted with the s2c key")
	}
	if plain, err := Decrypt(c2s, ciphertext); err != nil {
		t.Fatalf("decrypt with c2s key: %v", err)
	} else if !bytes.Equal(plain, []byte("client to server")) {
		t.Fatalf("decrypted plaintext = %q", plain)
	}
}

func TestSessionSaltIsRandom(t *testing.T) {
	a, err := NewSessionSalt()
	if err != nil {
		t.Fatalf("NewSessionSalt: %v", err)
	}
	b, err := NewSessionSalt()
	if err != nil {
		t.Fatalf("NewSessionSalt: %v", err)
	}
	if len(a) != SaltSize || len(b) != SaltSize {
		t.Fatalf("salt sizes = %d/%d, want %d", len(a), len(b), SaltSize)
	}
	if bytes.Equal(a, b) {
		t.Fatal("two session salts are identical")
	}
}

func TestReplayWindow(t *testing.T) {
	var w replayWindow

	if !w.checkAndMark(0) {
		t.Fatal("first packet was rejected")
	}
	if w.checkAndMark(0) {
		t.Fatal("duplicate seq=0 was accepted")
	}
	if !w.checkAndMark(1) {
		t.Fatal("seq=1 was rejected")
	}
	if !w.checkAndMark(2) {
		t.Fatal("seq=2 was rejected")
	}
	if w.checkAndMark(1) {
		t.Fatal("duplicate seq=1 was accepted")
	}

	if !w.checkAndMark(ReplayWindowSize) {
		t.Fatal("far-ahead seq was rejected")
	}
	if w.checkAndMark(0) {
		t.Fatal("seq=0 was accepted after leaving the replay window")
	}
	if w.checkAndMark(2) {
		t.Fatal("seq=2 was accepted after leaving the replay window")
	}

	if !w.checkAndMark(ReplayWindowSize - 1) {
		t.Fatal("seq inside the replay window was rejected")
	}
	if w.checkAndMark(ReplayWindowSize - 1) {
		t.Fatal("duplicate seq inside the replay window was accepted")
	}
}

func TestReplayWindowShiftAcrossWords(t *testing.T) {
	var w replayWindow
	for seq := uint64(0); seq < 4; seq++ {
		if !w.checkAndMark(seq) {
			t.Fatalf("seq=%d was rejected", seq)
		}
	}

	if !w.checkAndMark(130) {
		t.Fatal("seq=130 was rejected")
	}
	if w.checkAndMark(1) {
		t.Fatal("old seq=1 was accepted after a multi-word shift")
	}
	if w.checkAndMark(3) {
		t.Fatal("old seq=3 was accepted after a multi-word shift")
	}
	if !w.checkAndMark(129) {
		t.Fatal("seq=129 inside the shifted window was rejected")
	}
}
