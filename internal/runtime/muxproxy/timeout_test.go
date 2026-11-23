package muxproxy

import (
	"context"
	"testing"
	"time"
)

func TestWithTimeoutAddsDeadline(t *testing.T) {
	ctx, cancel := withTimeout(context.Background(), time.Second)
	defer cancel()
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("withTimeout did not add a deadline")
	}
}

func TestWithTimeoutKeepsExistingDeadline(t *testing.T) {
	want := time.Now().Add(5 * time.Second)
	parent, cancel := context.WithDeadline(context.Background(), want)
	defer cancel()

	ctx, cancel2 := withTimeout(parent, time.Millisecond)
	defer cancel2()
	got, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected a deadline")
	}
	if !got.Equal(want) {
		t.Fatalf("deadline = %v, want the parent deadline %v", got, want)
	}
}

func TestWithTimeoutHandlesNilContext(t *testing.T) {
	ctx, cancel := withTimeout(nil, time.Millisecond)
	defer cancel()
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("nil context did not get a deadline")
	}
}
