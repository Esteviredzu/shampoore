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
	"github.com/Esteviredzu/shampoore/internal/netdial"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

type objectStore interface {
	BucketExists(ctx context.Context, bucket string) (bool, error)
	MakeBucket(ctx context.Context, bucket string, opts minio.MakeBucketOptions) error
	PutObject(ctx context.Context, bucket, object string, reader io.Reader, size int64, opts minio.PutObjectOptions) (minio.UploadInfo, error)
	GetObject(ctx context.Context, bucket, object string, opts minio.GetObjectOptions) (io.ReadCloser, error)
	RemoveObject(ctx context.Context, bucket, object string, opts minio.RemoveObjectOptions) error
	ListObjects(ctx context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo
}

type minioStore struct {
	client *minio.Client
}

func (s *minioStore) BucketExists(ctx context.Context, bucket string) (bool, error) {
	return s.client.BucketExists(ctx, bucket)
}

func (s *minioStore) MakeBucket(ctx context.Context, bucket string, opts minio.MakeBucketOptions) error {
	return s.client.MakeBucket(ctx, bucket, opts)
}

func (s *minioStore) PutObject(ctx context.Context, bucket, object string, reader io.Reader, size int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	return s.client.PutObject(ctx, bucket, object, reader, size, opts)
}

func (s *minioStore) GetObject(ctx context.Context, bucket, object string, opts minio.GetObjectOptions) (io.ReadCloser, error) {
	return s.client.GetObject(ctx, bucket, object, opts)
}

func (s *minioStore) RemoveObject(ctx context.Context, bucket, object string, opts minio.RemoveObjectOptions) error {
	return s.client.RemoveObject(ctx, bucket, object, opts)
}

func (s *minioStore) ListObjects(ctx context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	return s.client.ListObjects(ctx, bucket, opts)
}

func newMinioStore(cfg *Config) (objectStore, error) {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			Control:   netdial.Control,
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
	return &minioStore{client: cl}, nil
}

type Pipe struct {
	cfg    *Config
	logger logging.ContextLogger
	store  objectStore

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
	store, err := newMinioStore(cfg)
	if err != nil {
		return nil, err
	}
	return newPipeWithStore(cfg, role, logger, store)
}

func ensureBucket(ctx context.Context, store objectStore, bucket string) error {
	exists, err := store.BucketExists(ctx, bucket)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	if err := store.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.Code == "BucketAlreadyOwnedByYou" || resp.Code == "BucketAlreadyExists" {
			return nil
		}

		if exists, checkErr := store.BucketExists(ctx, bucket); checkErr == nil && exists {
			return nil
		}
		return err
	}
	return nil
}

func newPipeWithStore(cfg *Config, role string, logger logging.ContextLogger, store objectStore) (*Pipe, error) {
	if err := ensureBucket(context.Background(), store, cfg.Bucket); err != nil {
		return nil, fmt.Errorf("silo: ensure bucket %q: %w", cfg.Bucket, err)
	}

	writePrefix, readPrefix := buildPrefixes(cfg, role)
	return &Pipe{
		cfg:         cfg,
		logger:      logger,
		store:       store,
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
	return p.store.RemoveObject(ctx, p.cfg.Bucket, p.writePrefix+key, minio.RemoveObjectOptions{})
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
			var hdr [4]byte
			binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))
			buffer = append(buffer, hdr[:]...)
			buffer = append(buffer, data...)

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

	_, err := p.store.PutObject(p.ctx, p.cfg.Bucket, fullKey, bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		p.logger.Warn("silo: put object failed: ", err)
		return
	}

	p.ctr.NetworkSent.Add(int64(len(buf)))
	p.ctr.MessagesSent.Add(1)
}

const maxPollBackoff = 500 * time.Millisecond

func nextPollDelay(cur, base, max time.Duration) time.Duration {
	if base <= 0 {
		base = 10 * time.Millisecond
	}
	if max < base {
		max = base
	}
	if cur < base {
		cur = base
	}
	cur *= 2
	if cur > max {
		cur = max
	}
	return cur
}

func (p *Pipe) receiveLoop() {
	defer p.wg.Done()

	base := time.Duration(p.cfg.PollIntervalMs) * time.Millisecond
	if base < 10*time.Millisecond {
		base = 10 * time.Millisecond
	}
	delay := base
	timer := time.NewTimer(delay)
	defer timer.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-timer.C:
			if p.pollOnce() > 0 {
				delay = base
			} else {
				delay = nextPollDelay(delay, base, maxPollBackoff)
			}
			timer.Reset(delay)
		}
	}
}

func (p *Pipe) pollOnce() int {
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
	objectCh := p.store.ListObjects(p.ctx, p.cfg.Bucket, opts)

	for object := range objectCh {
		if object.Err != nil {
			continue
		}
		keys = append(keys, object.Key)
	}

	if len(keys) == 0 {
		return 0
	}

	sort.Strings(keys)

	delivered := 0
	for _, key := range keys {

		if !p.processFile(key) {
			break
		}

		p.lastKeyMu.Lock()
		if key > p.lastKey {
			p.lastKey = key
		}
		p.lastKeyMu.Unlock()
		delivered++
	}
	return delivered
}

func isMissingObject(err error) bool {
	if err == nil {
		return false
	}
	resp := minio.ToErrorResponse(err)
	return resp.Code == "NoSuchKey" || resp.StatusCode == http.StatusNotFound
}

func (p *Pipe) processFile(key string) bool {
	objReader, err := p.store.GetObject(p.ctx, p.cfg.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return isMissingObject(err)
	}
	data, err := io.ReadAll(objReader)
	_ = objReader.Close()
	if err != nil {
		return isMissingObject(err)
	}

	if len(data) == 0 {
		if err := p.store.RemoveObject(p.ctx, p.cfg.Bucket, key, minio.RemoveObjectOptions{}); err != nil &&
			!isMissingObject(err) {
			return false
		}
		return true
	}

	p.ctr.NetworkRecv.Add(int64(len(data)))

	offset := 0
	malformed := false
	for offset+4 <= len(data) {
		msgLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4

		if msgLen < 0 || offset+msgLen > len(data) {
			malformed = true
			break
		}

		msg := data[offset : offset+msgLen]
		offset += msgLen

		p.ctr.UsefulRecv.Add(int64(len(msg)))
		p.ctr.MessagesRecv.Add(1)

		select {
		case p.recvCh <- msg:
		case <-p.ctx.Done():
			return false
		}
	}
	if malformed {
		p.logger.Warn("silo: dropping malformed object ", key)
	}

	if err := p.store.RemoveObject(p.ctx, p.cfg.Bucket, key, minio.RemoveObjectOptions{}); err != nil &&
		!isMissingObject(err) {
		p.logger.Warn("silo: remove delivered object ", key, ": ", err)
		return false
	}
	return true
}

var _ transport.Pipe = (*Pipe)(nil)
var _ transport.Telemetry = (*Pipe)(nil)
