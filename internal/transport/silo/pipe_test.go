package silo

import (
	"testing"
	"time"
)

func TestNextPollDelayBacksOff(t *testing.T) {
	base := 10 * time.Millisecond
	max := 500 * time.Millisecond
	want := []time.Duration{
		20 * time.Millisecond,
		40 * time.Millisecond,
		80 * time.Millisecond,
		160 * time.Millisecond,
		320 * time.Millisecond,
		500 * time.Millisecond,
		500 * time.Millisecond,
	}

	cur := base
	for i, expected := range want {
		cur = nextPollDelay(cur, base, max)
		if cur != expected {
			t.Fatalf("step %d: delay = %s, want %s", i, cur, expected)
		}
	}
}

func TestNextPollDelayClampsBase(t *testing.T) {
	if got := nextPollDelay(0, 0, 0); got != 10*time.Millisecond {
		t.Fatalf("delay = %s, want 10ms", got)
	}
	if got := nextPollDelay(2*time.Second, time.Second, 500*time.Millisecond); got != time.Second {
		t.Fatalf("delay = %s, want base 1s", got)
	}
}
