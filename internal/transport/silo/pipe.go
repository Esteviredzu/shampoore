package silo

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

type Pipe struct {
	cfg    *Config
	logger logging.ContextLogger
	client *minio.Client

	writePrefix string
	readPrefix  string

	ctx     context.Context
	cancel  context.CancelFunc
	started bool

	sendCh chan []byte
	recvCh chan []byte

	lastKeyMu sync.Mutex
	lastKey   string

	ctr transport.Counters
	wg  sync.WaitGroup
}

func NewPipe(cfg *Config, role string, logger logging.ContextLogger) (*Pipe, error) {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	cl, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure:    cfg.UseSSL,
		Transport: tr,
	})
	if err != nil {
		return nil, fmt.Errorf("silo: create client: %w", err)
	}

	exists, err := cl.BucketExists(context.Background(), cfg.Bucket)
	if err != nil || !exists {
		return nil, fmt.Errorf("silo: bucket %q unavailable: %v", cfg.Bucket, err)
	}

	writePrefix := fmt.Sprintf("%s/c2s/", cfg.TunnelID)
	readPrefix := fmt.Sprintf("%s/s2c/", cfg.TunnelID)
	if role == "server" {
		writePrefix, readPrefix = readPrefix, writePrefix
	}

	return &Pipe{
		cfg:         cfg,
		logger:      logger,
		client:      cl,
		writePrefix: writePrefix,
		readPrefix:  readPrefix,
		sendCh:      make(chan []byte, 4096),
		recvCh:      make(chan []byte, 4096),
	}, nil
}

func (p *Pipe) Start(ctx context.Context) error {
	if p.started {
		return nil
	}
	p.ctx, p.cancel = context.WithCancel(ctx)
	p.started = true

	p.wg.Add(2)
	go p.receiveLoop()
	go p.sendLoop()

	p.logger.Info("silo: pipe started (write=", p.writePrefix, ", read=", p.readPrefix, ")")
	return nil
}

func (p *Pipe) Stop(ctx context.Context) error {
	if !p.started {
		return nil
	}
	p.cancel()
	p.wg.Wait()
	p.started = false
	return nil
}

func (p *Pipe) SessionID() string { return p.cfg.TunnelID }

func (p *Pipe) MaxMessageSize() int { return p.cfg.MaxFileSizeKB * 1024 }

func (p *Pipe) WalkLayers() []transport.Layer {
	return []transport.Layer{{Kind: "pipe", Type: "silo", Snapshot: p.ctr.Snapshot()}}
}

func (p *Pipe) SendMessage(ctx context.Context, data []byte, _ string) error {
	if len(data) == 0 {
		return nil
	}
	cp := make([]byte, len(data))
	copy(cp, data)

	select {
	case p.sendCh <- cp:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Pipe) ReceiveMessage(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case data, ok := <-p.recvCh:
		if !ok {
			return nil, io.EOF
		}
		return data, nil
	}
}

func (p *Pipe) DeleteMessage(ctx context.Context, key string) error {
	return p.client.RemoveObject(ctx, p.cfg.Bucket, p.writePrefix+key, minio.RemoveObjectOptions{})
}

func (p *Pipe) sendLoop() {
	defer p.wg.Done()

	minB := p.cfg.MinFileSizeKB * 1024
	maxB := p.cfg.MaxFileSizeKB * 1024
	flushDur := time.Duration(p.cfg.FlushMs) * time.Millisecond

	targetSize := rand.Intn(maxB-minB+1) + minB

	buffer := make([]byte, 0, targetSize)
	timer := time.NewTimer(flushDur)
	defer timer.Stop()

	flush := func() {
		if len(buffer) == 0 {
			return
		}
		p.upload(buffer)
		buffer = buffer[:0]
		targetSize = rand.Intn(maxB-minB+1) + minB
	}

	for {
		select {
		case <-p.ctx.Done():
			flush()
			return
		case data := <-p.sendCh:
			msg := make([]byte, 4+len(data))
			binary.BigEndian.PutUint32(msg[0:4], uint32(len(data)))
			copy(msg[4:], data)
			buffer = append(buffer, msg...)

			if len(buffer) >= targetSize {
				flush()
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(flushDur)
			}
		case <-timer.C:
			flush()
			timer.Reset(flushDur)
		}
	}
}

func (p *Pipe) upload(buf []byte) {
	objName := fmt.Sprintf("msg_%d_%d", time.Now().UnixNano(), rand.Int63())
	fullKey := p.writePrefix + objName

	_, err := p.client.PutObject(p.ctx, p.cfg.Bucket, fullKey, bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		p.logger.Warn("silo: put object failed: ", err)
		return
	}

	p.ctr.NetworkSent.Add(int64(len(buf)))
	p.ctr.MessagesSent.Add(1)
}

func (p *Pipe) receiveLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(time.Duration(p.cfg.PollIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.pollOnce()
		}
	}
}

func (p *Pipe) pollOnce() {
	p.lastKeyMu.Lock()
	startAfter := p.lastKey
	p.lastKeyMu.Unlock()

	opts := minio.ListObjectsOptions{
		Prefix:    p.readPrefix,
		Recursive: true,
	}
	if startAfter != "" {
		opts.StartAfter = startAfter
	}

	var keys []string
	objectCh := p.client.ListObjects(p.ctx, p.cfg.Bucket, opts)

	for object := range objectCh {
		if object.Err != nil {
			continue
		}
		if time.Since(object.LastModified) > 30*time.Second {
			_ = p.client.RemoveObject(p.ctx, p.cfg.Bucket, object.Key, minio.RemoveObjectOptions{})
			continue
		}
		keys = append(keys, object.Key)
	}

	if len(keys) == 0 {
		return
	}

	sort.Strings(keys)

	for _, key := range keys {
		p.processFile(key)

		p.lastKeyMu.Lock()
		if key > p.lastKey {
			p.lastKey = key
		}
		p.lastKeyMu.Unlock()
	}
}

func (p *Pipe) processFile(key string) {
	objReader, err := p.client.GetObject(p.ctx, p.cfg.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		_ = p.client.RemoveObject(p.ctx, p.cfg.Bucket, key, minio.RemoveObjectOptions{})
		return
	}
	data, err := io.ReadAll(objReader)
	objReader.Close()

	_ = p.client.RemoveObject(p.ctx, p.cfg.Bucket, key, minio.RemoveObjectOptions{})

	if err != nil || len(data) == 0 {
		return
	}

	p.ctr.NetworkRecv.Add(int64(len(data)))

	offset := 0
	for offset+4 <= len(data) {
		msgLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4

		if offset+msgLen > len(data) {
			break
		}

		msgData := data[offset : offset+msgLen]
		offset += msgLen

		out := make([]byte, len(msgData))
		copy(out, msgData)

		p.ctr.UsefulRecv.Add(int64(len(out)))
		p.ctr.MessagesRecv.Add(1)

		select {
		case p.recvCh <- out:
		case <-p.ctx.Done():
			return
		}
	}
}

var _ transport.Pipe = (*Pipe)(nil)
var _ transport.Telemetry = (*Pipe)(nil)
