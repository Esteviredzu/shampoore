package yandex

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

const (
	maxValueSize     = 15000
	maxOutboundBatch = 8192
	statsInterval    = 5 * time.Second
)

type PipeConfig struct {
	QueueSize     int
	SendBatch     int
	SendFlushMs   int
	DeleteEnabled bool
	DeleteBatch   int
	DeleteFlushMs int
	StatsEnabled  bool
}

func DefaultPipeConfig() PipeConfig {
	return PipeConfig{
		QueueSize:     65536,
		SendBatch:     32,
		SendFlushMs:   100,
		DeleteEnabled: true,
		DeleteBatch:   512,
		DeleteFlushMs: 2000,
		StatsEnabled:  true,
	}
}

type YandexPipe struct {
	client  *BoardClient
	logger  logging.ContextLogger
	cfg     PipeConfig
	session string

	messages   chan []byte
	receiveErr error
	resetCh    chan struct{}

	flushMu        sync.Mutex
	outboundMu     sync.Mutex
	outboundBuf    map[string]string
	outboundOrder  []string
	outboundTicker *time.Ticker

	deleteMu     sync.Mutex
	deleteBuf    []string
	flushCloseMu sync.Once
	flushDone    chan struct{}

	ctx     context.Context
	cancel  context.CancelFunc
	started bool

	statsMu      sync.Mutex
	sentMessages uint64
	sentBytes    uint64
	recvMessages uint64
	recvBytes    uint64
	statsTicker  *time.Ticker

	ctr transport.Counters
}

type outboundBatch struct {
	orderLen int
	ids      []string
	values   []string
}

func NewYandexPipe(client *BoardClient, logger logging.ContextLogger, cfg PipeConfig) *YandexPipe {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 65536
	}
	if cfg.SendBatch <= 0 {
		cfg.SendBatch = 32
	}
	if cfg.DeleteBatch <= 0 {
		cfg.DeleteBatch = 512
	}
	return &YandexPipe{
		client:         client,
		logger:         logger,
		cfg:            cfg,
		session:        uuid.NewString(),
		messages:       make(chan []byte, cfg.QueueSize),
		resetCh:        make(chan struct{}, 1),
		flushDone:      make(chan struct{}),
		outboundBuf:    make(map[string]string),
		outboundTicker: time.NewTicker(time.Duration(cfg.SendFlushMs) * time.Millisecond),
		statsTicker:    time.NewTicker(statsInterval),
	}
}

func (p *YandexPipe) Start(ctx context.Context) error {
	if p.started {
		return nil
	}
	p.ctx, p.cancel = context.WithCancel(ctx)
	p.started = true

	go p.receiveLoop()
	go p.outboundFlushLoop()
	go p.deleteFlushLoop()
	go p.statsLoop()
	return nil
}

func (p *YandexPipe) Stop(ctx context.Context) error {
	if !p.started {
		return nil
	}
	p.started = false

	p.cancel()
	p.flushCloseMu.Do(func() { close(p.flushDone) })

	_ = p.flushOutbound(context.Background())

	p.deleteMu.Lock()
	if len(p.deleteBuf) > 0 {
		_ = p.flushDeletesLocked(context.Background())
	}
	p.deleteMu.Unlock()

	if p.client != nil {
		_ = p.client.Close()
	}

	return nil
}

func (p *YandexPipe) NotifyNetworkChanged() {
	if p.client != nil {
		p.client.NotifyNetworkChanged()
	}
}

func (p *YandexPipe) SessionID() string   { return p.session }
func (p *YandexPipe) MaxMessageSize() int { return maxValueSize }

func (p *YandexPipe) WalkLayers() []transport.Layer {
	return []transport.Layer{{Kind: "pipe", Type: "yandex", Snapshot: p.ctr.Snapshot()}}
}

func (p *YandexPipe) SendMessage(ctx context.Context, data []byte, key string) error {
	value := base64.StdEncoding.EncodeToString(data)
	objectID := objectIDFromKey(key)

	p.outboundMu.Lock()
	if _, exists := p.outboundBuf[objectID]; !exists {
		if len(p.outboundBuf) >= maxOutboundBatch {
			p.outboundMu.Unlock()
			return errors.New("yandex-pipe: outbound batch overflow")
		}
		p.outboundOrder = append(p.outboundOrder, objectID)
	}
	p.outboundBuf[objectID] = value
	p.outboundMu.Unlock()

	p.statsMu.Lock()
	p.sentMessages++
	p.sentBytes += uint64(len(data))
	p.statsMu.Unlock()

	p.ctr.UsefulSent.Add(int64(len(data)))
	p.ctr.MessagesSent.Add(1)
	return nil
}

func (p *YandexPipe) ReceiveMessage(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.resetCh:
		return nil, transport.ErrPipeReset
	case data, ok := <-p.messages:
		if !ok {
			if p.receiveErr != nil {
				return nil, p.receiveErr
			}
			return nil, io.EOF
		}
		return data, nil
	}
}

func (p *YandexPipe) DeleteMessage(ctx context.Context, key string) error {
	if !p.cfg.DeleteEnabled {
		return nil
	}
	objectID := objectIDFromKey(key)

	p.deleteMu.Lock()
	p.deleteBuf = append(p.deleteBuf, objectID)
	p.deleteMu.Unlock()
	return nil
}

func (p *YandexPipe) outboundFlushLoop() {
	defer p.outboundTicker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-p.flushDone:
			return
		case <-p.outboundTicker.C:
			_ = p.flushOutbound(context.Background())
		}
	}
}

func (p *YandexPipe) snapshotOutbound() outboundBatch {
	batchSize := p.cfg.SendBatch
	if batchSize <= 0 {
		batchSize = 32
	}

	p.outboundMu.Lock()
	defer p.outboundMu.Unlock()

	n := batchSize
	if n > len(p.outboundOrder) {
		n = len(p.outboundOrder)
	}
	batch := outboundBatch{orderLen: n}
	if n == 0 {
		return batch
	}
	batch.ids = make([]string, 0, n)
	batch.values = make([]string, 0, n)
	for _, id := range p.outboundOrder[:n] {
		value, ok := p.outboundBuf[id]
		if !ok {
			continue
		}
		batch.ids = append(batch.ids, id)
		batch.values = append(batch.values, value)
	}
	return batch
}

func (p *YandexPipe) ackOutbound(batch outboundBatch) {
	p.outboundMu.Lock()
	defer p.outboundMu.Unlock()

	order := p.outboundOrder
	n := batch.orderLen
	if n > len(order) {
		n = len(order)
	}
	tail := order[n:]
	copy(order, tail)
	order = order[:len(tail)]

	for i, id := range batch.ids {
		cur, ok := p.outboundBuf[id]
		if !ok {
			continue
		}
		if cur == batch.values[i] {
			delete(p.outboundBuf, id)
			continue
		}
		order = append(order, id)
	}

	p.outboundOrder = order
	if len(order) == 0 {
		p.outboundOrder = nil
	}
}

func (p *YandexPipe) flushOutbound(ctx context.Context) error {

	p.flushMu.Lock()
	defer p.flushMu.Unlock()

	for {
		batch := p.snapshotOutbound()
		if batch.orderLen == 0 {
			return nil
		}
		if len(batch.ids) == 0 {
			p.ackOutbound(batch)
			continue
		}

		objects := make([]map[string]any, 0, len(batch.ids))
		valueChanges := make(map[string]bool, len(batch.ids))
		var wire int64
		for i, id := range batch.ids {
			obj := createBoardObject(
				id, batch.values[i],
				p.client.creds.Participant,
				p.client.creds.Dashboard,
			)
			objects = append(objects, obj)
			valueChanges[id] = false
			if b, err := json.Marshal(obj); err == nil {
				wire += int64(len(b))
			}
		}

		cnt := len(objects)
		if _, err := p.client.SendObjects(ctx, objects, valueChanges, false); err != nil {
			p.logger.Warn("yandex-pipe: outbound flush failed (",
				cnt, " objects): ", err)
			return err
		}

		p.ctr.NetworkSent.Add(wire)
		p.ctr.FramesSent.Add(int64(cnt))
		p.ackOutbound(batch)
	}
}

func (p *YandexPipe) receiveLoop() {
	defer close(p.messages)

	for {
		select {
		case <-p.ctx.Done():
			p.receiveErr = io.EOF
			return
		case event, ok := <-p.client.EventQueue():
			if !ok {
				p.receiveErr = io.EOF
				return
			}

			if _, isRestored := event.(*BoardConnectionRestored); isRestored {
				p.logger.Info("yandex-pipe: websocket reconnected, sending reset")
				select {
				case p.resetCh <- struct{}{}:
				default:
				}
				continue
			}

			for _, value := range p.extractValues(event) {
				p.statsMu.Lock()
				p.recvMessages++
				p.recvBytes += uint64(len(value))
				p.statsMu.Unlock()

				p.ctr.UsefulRecv.Add(int64(len(value)))
				p.ctr.NetworkRecv.Add(int64(len(value))*4/3 + 500)
				p.ctr.MessagesRecv.Add(1)

				select {
				case p.messages <- value:
				case <-p.ctx.Done():
					p.receiveErr = io.EOF
					return
				}
			}
		}
	}
}

func (p *YandexPipe) deleteFlushLoop() {
	ticker := time.NewTicker(time.Duration(p.cfg.DeleteFlushMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-p.flushDone:
			return
		case <-ticker.C:
			p.deleteMu.Lock()
			if len(p.deleteBuf) > 0 {
				_ = p.flushDeletesLocked(context.Background())
			}
			p.deleteMu.Unlock()
		}
	}
}

func (p *YandexPipe) statsLoop() {
	defer p.statsTicker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-p.statsTicker.C:
			p.statsMu.Lock()
			s := p.sentMessages
			sb := p.sentBytes
			r := p.recvMessages
			rb := p.recvBytes
			p.sentMessages = 0
			p.sentBytes = 0
			p.recvMessages = 0
			p.recvBytes = 0
			p.statsMu.Unlock()

			if p.cfg.StatsEnabled {
				p.logger.Info("[pipe-stats] sent ", s, " msgs / ", sb,
					" bytes; recv ", r, " msgs / ", rb,
					" bytes (", int(statsInterval.Seconds()), "s)")
			}
		}
	}
}

func (p *YandexPipe) flushDeletesLocked(ctx context.Context) error {
	if len(p.deleteBuf) == 0 {
		return nil
	}

	batchSize := p.cfg.DeleteBatch
	if batchSize <= 0 {
		batchSize = 512
	}

	for len(p.deleteBuf) > 0 {
		n := batchSize
		if n > len(p.deleteBuf) {
			n = len(p.deleteBuf)
		}

		ids := make([]string, n)
		copy(ids, p.deleteBuf[:n])

		objects := make([]map[string]any, 0, n)
		for _, id := range ids {
			objects = append(objects, createBoardObject(
				id, "",
				p.client.creds.Participant,
				p.client.creds.Dashboard,
			))
		}

		if err := p.client.DropObjects(ctx, objects); err != nil {
			p.logger.Warn("yandex-pipe: drop batch failed (", n, " objects): ", err)
			return err
		}

		p.deleteBuf = p.deleteBuf[n:]
		if len(p.deleteBuf) == 0 {
			p.deleteBuf = nil
		}
	}
	return nil
}

func (p *YandexPipe) extractValues(event any) [][]byte {
	arr, ok := event.([]any)
	if !ok || len(arr) < 2 {
		return nil
	}
	if name, _ := arr[0].(string); name != "dashboard" {
		return nil
	}
	eventData, ok := arr[1].(map[string]any)
	if !ok {
		return nil
	}
	if action, _ := eventData["action"].(string); action != "server-modify-objects" {
		return nil
	}
	data, ok := eventData["data"].(map[string]any)
	if !ok {
		return nil
	}
	objects, ok := data["objects"].([]any)
	if !ok {
		return nil
	}

	var result [][]byte
	for _, obj := range objects {
		m, ok := obj.(map[string]any)
		if !ok {
			continue
		}
		attrs, ok := m["_attributes_"].(map[string]any)
		if !ok {
			continue
		}

		if creator, _ := attrs["creatorHash"].(string); creator == p.client.creds.Participant {
			continue
		}

		value, ok := attrs["value"].(string)
		if !ok || value == "" {
			continue
		}
		decoded, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			continue
		}
		result = append(result, decoded)
	}
	return result
}

func objectIDFromKey(key string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("yandex-pipe:"+key)).String()
}

func createBoardObject(objectID, value, participant, dashboard string) map[string]any {
	return map[string]any{
		"_attributes_": map[string]any{
			"id":    objectID,
			"value": value,
			"style": "html=1;aspect=fixed;fillColor=#00000000;" +
				"strokeColor=#00000000;connectable=0;rotatable=0;" +
				"allowArrows=0;editable=0",
			"vertex":      "1",
			"type":        "dragline",
			"path":        "m 0 0 l 1 1",
			"stroke":      "rgba(17,17,17)",
			"strokeWidth": "1",
			"base_width":  "1",
			"base_height": "1",
			"creatorHash": participant,
			"parent":      dashboard,
			"index":       "1",
		},
		"mxGeometry": []any{
			map[string]any{
				"_attributes_": map[string]any{
					"x": "0", "y": "0",
					"width": "1", "height": "1",
					"as": "geometry",
				},
			},
		},
		"Object": []any{
			map[string]any{
				"_attributes_": map[string]any{
					"x": "-25", "y": "-25", "as": "offset",
				},
			},
		},
		"hash": objectID,
	}
}
