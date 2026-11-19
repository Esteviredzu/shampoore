package yandex

import (
	"testing"
)

func TestOutboundSnapshotAckPreservesConcurrentUpdate(t *testing.T) {
	p := &YandexPipe{
		cfg:           PipeConfig{SendBatch: 2},
		outboundBuf:   map[string]string{"a": "1", "b": "2", "c": "3"},
		outboundOrder: []string{"a", "b", "c"},
	}

	batch := p.snapshotOutbound()
	if batch.orderLen != 2 || len(batch.ids) != 2 {
		t.Fatalf("unexpected snapshot: %+v", batch)
	}
	if batch.ids[0] != "a" || batch.values[0] != "1" ||
		batch.ids[1] != "b" || batch.values[1] != "2" {
		t.Fatalf("unexpected snapshot contents: %+v", batch)
	}

	p.outboundBuf["a"] = "updated"
	p.ackOutbound(batch)

	if _, ok := p.outboundBuf["b"]; ok {
		t.Fatal("delivered value b was not removed")
	}
	if got := p.outboundBuf["a"]; got != "updated" {
		t.Fatalf("updated value was lost: %q", got)
	}
	if _, ok := p.outboundBuf["c"]; !ok {
		t.Fatal("queued value c was lost")
	}
	if len(p.outboundOrder) != 2 || p.outboundOrder[0] != "c" || p.outboundOrder[1] != "a" {
		t.Fatalf("outbound order = %v, want [c a]", p.outboundOrder)
	}
}

func TestOutboundSnapshotSkipsMissingEntries(t *testing.T) {
	p := &YandexPipe{
		cfg:           PipeConfig{SendBatch: 3},
		outboundBuf:   map[string]string{"a": "1", "c": "3"},
		outboundOrder: []string{"a", "missing", "c"},
	}

	batch := p.snapshotOutbound()
	if batch.orderLen != 3 || len(batch.ids) != 2 {
		t.Fatalf("unexpected snapshot: %+v", batch)
	}
	p.ackOutbound(batch)

	if len(p.outboundBuf) != 0 {
		t.Fatalf("outbound buffer is not empty: %v", p.outboundBuf)
	}
	if len(p.outboundOrder) != 0 {
		t.Fatalf("outbound order is not empty: %v", p.outboundOrder)
	}
}
