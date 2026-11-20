package relay

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DefaultFeedInterval = 15 * time.Second

	DefaultQuotesURL = "https://api.binance.com/api/v3/ticker/24hr?symbols=%5B%22BTCUSDT%22,%22ETHUSDT%22,%22SOLUSDT%22%5D"

	DefaultNewsURL = "https://cointelegraph.com/rss"

	feedHTTPTimeout = 5 * time.Second
	maxFeedBody     = 1 << 20
	feedJitter      = 0.0015
	feedNewsLimit   = 5
	maxFeedNews     = 12
)

var defaultSymbols = []string{"BTCUSDT", "ETHUSDT", "SOLUSDT"}

var defaultBasePrices = map[string]float64{
	"BTCUSDT": 97250.0,
	"ETHUSDT": 3420.0,
	"SOLUSDT": 198.5,
}

type Quote struct {
	Symbol    string  `json:"symbol"`
	Price     float64 `json:"price"`
	ChangePct float64 `json:"change_pct"`
}

type NewsItem struct {
	Title     string `json:"title"`
	Link      string `json:"link,omitempty"`
	Source    string `json:"source,omitempty"`
	Published int64  `json:"published,omitempty"`
}

type MarketSnapshot struct {
	Quotes    []Quote
	News      []NewsItem
	Updated   time.Time
	Simulated bool
}

type Tick struct {
	Type      string     `json:"type"`
	TS        int64      `json:"ts"`
	Quotes    []Quote    `json:"quotes"`
	News      []NewsItem `json:"news,omitempty"`
	Simulated bool       `json:"simulated,omitempty"`
}

type FeedOptions struct {
	Interval  time.Duration
	QuotesURL string
	NewsURL   string
	Client    *http.Client
	Symbols   []string

	BasePrices map[string]float64
	Now        func() time.Time
	Offline    bool
}

type Feed struct {
	interval   time.Duration
	quotesURL  string
	newsURL    string
	client     *http.Client
	symbols    []string
	basePrices map[string]float64
	now        func() time.Time
	offline    bool

	mu       sync.Mutex
	snapshot MarketSnapshot
	tickIdx  int

	startOnce sync.Once
}

func NewFeed(opts FeedOptions) *Feed {
	f := &Feed{
		interval:   opts.Interval,
		quotesURL:  strings.TrimSpace(opts.QuotesURL),
		newsURL:    strings.TrimSpace(opts.NewsURL),
		client:     opts.Client,
		symbols:    append([]string(nil), opts.Symbols...),
		basePrices: make(map[string]float64, len(defaultBasePrices)),
		now:        opts.Now,
		offline:    opts.Offline,
	}
	if f.interval <= 0 {
		f.interval = DefaultFeedInterval
	}
	if f.client == nil {
		f.client = &http.Client{Timeout: feedHTTPTimeout}
	}
	if len(f.symbols) == 0 {
		f.symbols = append([]string(nil), defaultSymbols...)
	}
	for k, v := range defaultBasePrices {
		f.basePrices[k] = v
	}
	for k, v := range opts.BasePrices {
		f.basePrices[k] = v
	}
	if f.now == nil {
		f.now = time.Now
	}
	if !f.offline {
		if f.quotesURL == "" {
			f.quotesURL = DefaultQuotesURL
		}
		if f.newsURL == "" {
			f.newsURL = DefaultNewsURL
		}
	}
	return f
}

func (f *Feed) Start(ctx context.Context) {
	if f == nil {
		return
	}
	f.startOnce.Do(func() {
		go f.run(ctx)
	})
}

func (f *Feed) run(ctx context.Context) {
	f.Refresh(ctx)
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.Refresh(ctx)
		}
	}
}

func (f *Feed) Refresh(ctx context.Context) {
	if f == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var (
		wg     sync.WaitGroup
		quotes []Quote
		news   []NewsItem
	)
	if !f.offline {
		wg.Add(2)
		go func() {
			defer wg.Done()
			quotes = f.fetchQuotes(ctx)
		}()
		go func() {
			defer wg.Done()
			news = f.fetchNews(ctx)
		}()
	}
	wg.Wait()

	now := f.now()
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(quotes) > 0 {
		f.snapshot.Quotes = quotes
		f.snapshot.Simulated = false
	} else {
		f.snapshot.Quotes = f.simulateQuotesLocked()
		f.snapshot.Simulated = true
	}
	if len(news) > 0 {
		f.snapshot.News = news
	} else if len(f.snapshot.News) == 0 {
		f.snapshot.News = fallbackNews(now)
	}
	f.snapshot.Updated = now
}

func (f *Feed) Snapshot() MarketSnapshot {
	if f == nil {
		return MarketSnapshot{}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	out := MarketSnapshot{
		Quotes:    append([]Quote(nil), f.snapshot.Quotes...),
		News:      append([]NewsItem(nil), f.snapshot.News...),
		Updated:   f.snapshot.Updated,
		Simulated: f.snapshot.Simulated,
	}
	return out
}

func (f *Feed) Tick() Tick {
	if f == nil {
		return Tick{Type: "tick", TS: time.Now().UTC().Unix()}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	tick := Tick{
		Type:      "tick",
		TS:        f.now().UTC().Unix(),
		Quotes:    append([]Quote(nil), f.snapshot.Quotes...),
		Simulated: f.snapshot.Simulated,
	}
	if len(f.snapshot.News) > 0 {
		idx := f.tickIdx % len(f.snapshot.News)
		f.tickIdx++
		tick.News = []NewsItem{f.snapshot.News[idx]}
	}
	return tick
}

func (f *Feed) simulateQuotesLocked() []Quote {
	prev := make(map[string]Quote, len(f.snapshot.Quotes))
	for _, q := range f.snapshot.Quotes {
		prev[q.Symbol] = q
	}
	out := make([]Quote, 0, len(f.symbols))
	for _, symbol := range f.symbols {
		q, ok := prev[symbol]
		if !ok {
			q = Quote{Symbol: symbol}
		}
		if q.Price <= 0 {
			q.Price = f.basePrices[symbol]
		}
		if q.Price <= 0 {
			q.Price = 100
		}
		delta := (rand.Float64()*2 - 1) * feedJitter
		q.Price = q.Price * (1 + delta)
		q.ChangePct += delta * 100
		out = append(out, q)
	}
	return out
}

func (f *Feed) fetchQuotes(ctx context.Context) []Quote {
	reqCtx, cancel := context.WithTimeout(ctx, feedHTTPTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, f.quotesURL, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; shampoore-relay/1.0)")

	resp, err := f.client.Do(req)
	if err != nil {
		return nil
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxFeedBody))
	if err != nil {
		return nil
	}

	var raw []struct {
		Symbol             string `json:"symbol"`
		LastPrice          string `json:"lastPrice"`
		PriceChangePercent string `json:"priceChangePercent"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil
	}

	out := make([]Quote, 0, len(raw))
	for _, item := range raw {
		price, err := strconv.ParseFloat(item.LastPrice, 64)
		if err != nil || price <= 0 {
			continue
		}
		change, _ := strconv.ParseFloat(item.PriceChangePercent, 64)
		out = append(out, Quote{Symbol: item.Symbol, Price: price, ChangePct: change})
	}
	return out
}

func (f *Feed) fetchNews(ctx context.Context) []NewsItem {
	reqCtx, cancel := context.WithTimeout(ctx, feedHTTPTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, f.newsURL, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("Accept", "application/rss+xml, application/xml, text/xml")
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; shampoore-relay/1.0)")

	resp, err := f.client.Do(req)
	if err != nil {
		return nil
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxFeedBody))
	if err != nil {
		return nil
	}
	return parseRSS(body, f.now(), feedNewsLimit)
}

func parseRSS(body []byte, now time.Time, limit int) []NewsItem {
	var doc struct {
		Channel struct {
			Title string `xml:"title"`
			Items []struct {
				Title   string `xml:"title"`
				Link    string `xml:"link"`
				PubDate string `xml:"pubDate"`
			} `xml:"item"`
		} `xml:"channel"`
	}
	if err := xml.Unmarshal(body, &doc); err != nil {
		return nil
	}
	out := make([]NewsItem, 0, len(doc.Channel.Items))
	for _, item := range doc.Channel.Items {
		title := strings.TrimSpace(item.Title)
		if title == "" {
			continue
		}
		out = append(out, NewsItem{
			Title:     title,
			Link:      strings.TrimSpace(item.Link),
			Source:    strings.TrimSpace(doc.Channel.Title),
			Published: parseFeedTime(item.PubDate, now),
		})
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	if len(out) > maxFeedNews {
		out = out[:maxFeedNews]
	}
	return out
}

func parseFeedTime(raw string, fallback time.Time) int64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback.UTC().Unix()
	}
	for _, layout := range []string{time.RFC1123Z, time.RFC1123, time.RFC822Z, time.RFC822, time.RFC3339} {
		if ts, err := time.Parse(layout, raw); err == nil {
			return ts.UTC().Unix()
		}
	}
	return fallback.UTC().Unix()
}

func fallbackNews(now time.Time) []NewsItem {
	titles := []string{
		"Bitcoin holds range as ETF inflows steady",
		"Ethereum validators approve routine network upgrade",
		"Solana activity climbs on rising on-chain volume",
		"Analysts watch liquidity shifts across major venues",
	}
	out := make([]NewsItem, 0, len(titles))
	ts := now.UTC().Unix()
	for i, title := range titles {
		out = append(out, NewsItem{Title: title, Source: "market-wire", Published: ts - int64(i*90)})
	}
	return out
}
