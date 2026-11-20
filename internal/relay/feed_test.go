package relay

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFeedOfflineSimulation(t *testing.T) {
	now := fixedNow()
	f := NewFeed(FeedOptions{
		Offline: true,
		Now:     func() time.Time { return now },
	})
	f.Refresh(context.Background())

	snap := f.Snapshot()
	if !snap.Simulated {
		t.Fatal("offline feed was not marked as simulated")
	}
	if len(snap.Quotes) != len(defaultSymbols) {
		t.Fatalf("quotes = %v", snap.Quotes)
	}
	for _, q := range snap.Quotes {
		if q.Price <= 0 {
			t.Fatalf("quote %+v has no price", q)
		}
		base := defaultBasePrices[q.Symbol]
		if base > 0 && (q.Price < base*0.99 || q.Price > base*1.01) {
			t.Fatalf("simulated price %f drifted away from base %f", q.Price, base)
		}
	}
	if len(snap.News) == 0 {
		t.Fatal("offline feed has no headlines")
	}

	tick := f.Tick()
	if tick.Type != "tick" || tick.TS != now.UTC().Unix() {
		t.Fatalf("tick = %+v", tick)
	}
	if len(tick.Quotes) == 0 || len(tick.News) != 1 {
		t.Fatalf("tick payload = %+v", tick)
	}
	next := f.Tick()
	if next.News[0].Title == tick.News[0].Title {
		t.Fatal("headlines are not rotating between ticks")
	}
}

func TestFeedFetchesRemoteQuotesAndNews(t *testing.T) {
	quotes := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[
			{"symbol":"BTCUSDT","lastPrice":"50000.5","priceChangePercent":"1.25"},
			{"symbol":"ETHUSDT","lastPrice":"2000","priceChangePercent":"-2.5"},
			{"symbol":"BROKEN","lastPrice":"oops","priceChangePercent":"0"}
		]`)
	}))
	defer quotes.Close()

	newsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/rss+xml")
		_, _ = io.WriteString(w, `<?xml version="1.0"?>
<rss><channel><title>Wire</title>
<item><title>First headline</title><link>https://example.test/1</link><pubDate>Mon, 17 Nov 2025 10:00:00 +0000</pubDate></item>
<item><title>Second headline</title><link>https://example.test/2</link><pubDate>Mon, 17 Nov 2025 09:00:00 +0000</pubDate></item>
</channel></rss>`)
	}))
	defer newsSrv.Close()

	now := fixedNow()
	f := NewFeed(FeedOptions{
		QuotesURL: quotes.URL,
		NewsURL:   newsSrv.URL,
		Symbols:   []string{"BTCUSDT", "ETHUSDT"},
		Now:       func() time.Time { return now },
	})
	f.Refresh(context.Background())

	snap := f.Snapshot()
	if snap.Simulated {
		t.Fatal("remote feed was marked as simulated")
	}
	if len(snap.Quotes) != 2 {
		t.Fatalf("quotes = %+v", snap.Quotes)
	}
	if snap.Quotes[0].Symbol != "BTCUSDT" || snap.Quotes[0].Price != 50000.5 {
		t.Fatalf("btc quote = %+v", snap.Quotes[0])
	}
	if snap.Quotes[1].ChangePct != -2.5 {
		t.Fatalf("eth change = %f", snap.Quotes[1].ChangePct)
	}
	if len(snap.News) != 2 || snap.News[0].Title != "First headline" || snap.News[0].Source != "Wire" {
		t.Fatalf("news = %+v", snap.News)
	}
	if published := snap.News[0].Published; published != time.Date(2025, 11, 17, 10, 0, 0, 0, time.UTC).Unix() {
		t.Fatalf("published = %d", published)
	}
}

func TestFeedFallsBackToSimulationOnUpstreamFailure(t *testing.T) {
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer broken.Close()

	f := NewFeed(FeedOptions{
		QuotesURL: broken.URL,
		NewsURL:   broken.URL,
		Symbols:   []string{"BTCUSDT"},
		Now:       fixedNow,
	})
	f.Refresh(context.Background())

	snap := f.Snapshot()
	if !snap.Simulated {
		t.Fatal("feed did not fall back to simulation")
	}
	if len(snap.Quotes) != 1 || snap.Quotes[0].Price <= 0 {
		t.Fatalf("quotes = %+v", snap.Quotes)
	}
	if len(snap.News) == 0 {
		t.Fatal("fallback headlines are missing")
	}
}

func TestFeedStartStopsWithContext(t *testing.T) {
	f := NewFeed(FeedOptions{Offline: true, Interval: 10 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	f.Start(ctx)
	f.Start(ctx)
	time.Sleep(30 * time.Millisecond)
	cancel()
	if snap := f.Snapshot(); len(snap.Quotes) == 0 {
		t.Fatal("feed did not refresh on start")
	}
}

func TestParseFeedTimeFallback(t *testing.T) {
	now := fixedNow()
	if got := parseFeedTime("not a date", now); got != now.Unix() {
		t.Fatalf("fallback = %d", got)
	}
	if got := parseFeedTime("", now); got != now.Unix() {
		t.Fatalf("empty fallback = %d", got)
	}
}
