package silo

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/net/utlsdial"
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

	tr := utlsdial.NewHTTP1Transport(utlsdial.Profile(strings.TrimSpace(cfg.UTLSProfile)))

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
	role   string

	writePrefix string
	readPrefix  string

	mu      sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	started bool

	sendCh chan uploadMsg
	recvCh chan []byte

	wake chan struct{}

	headMu      sync.Mutex
	lastKey     string
	historyHead string
	initialized bool

	deferMu       sync.Mutex
	deferred      map[string]*deferredBatch
	pendingDelete map[string]struct{}

	ctr transport.Counters
	wg  sync.WaitGroup

	sawTraffic atomic.Bool

	lastWrite atomic.Int64

	deadOnce sync.Once
	deadCh   chan struct{}
	deadMu   sync.Mutex
	deadErr  error
}

type listedObject struct {
	key      string
	size     int64
	modified time.Time
}

type deferredBatch struct {
	firstSeen time.Time
	attempts  int
}

var (
	incompleteBatchGrace    = 30 * time.Second
	incompleteBatchMaxTries = 20
)

var (
	errBatchIncomplete = errors.New("incomplete batch")
	errBatchCorrupt    = errors.New("corrupt batch")
)

const maxBatchMessageSize = 64 << 20

func parseBatch(data []byte, maxMessage int) ([][]byte, error) {
	if len(data) == 0 {
		return nil, errBatchIncomplete
	}
	messages := make([][]byte, 0, 8)
	offset := 0
	for offset < len(data) {
		if offset+4 > len(data) {
			return nil, errBatchIncomplete
		}
		msgLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		if msgLen <= 0 {
			return nil, errBatchCorrupt
		}
		if maxMessage > 0 && msgLen > maxMessage {
			return nil, errBatchCorrupt
		}
		if offset+msgLen > len(data) {
			return nil, errBatchIncomplete
		}
		messages = append(messages, data[offset:offset+msgLen])
		offset += msgLen
	}
	if len(messages) == 0 {
		return nil, errBatchIncomplete
	}
	return messages, nil
}

type uploadMsg struct {
	data []byte
	done chan error
}

func resolveUpload(msg uploadMsg, err error) {
	if msg.done == nil {
		return
	}
	select {
	case msg.done <- err:
	default:
	}
}

const stopFlushTimeout = 2 * time.Second

const bucketTimeout = 15 * time.Second

var s3RequestTimeout = 15 * time.Second

var downloadSilenceLimit = 15 * time.Minute

var pendingDeleteInterval = 500 * time.Millisecond

var purgeTimeout = 15 * time.Second

var staleInlineDeleteBudget = 250 * time.Millisecond

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
	writePrefix, readPrefix := buildPrefixes(cfg, role)
	return &Pipe{
		cfg:           cfg,
		logger:        logger,
		store:         store,
		role:          role,
		writePrefix:   writePrefix,
		readPrefix:    readPrefix,
		sendCh:        make(chan uploadMsg, 4096),
		recvCh:        make(chan []byte, 4096),
		wake:          make(chan struct{}, 1),
		deferred:      make(map[string]*deferredBatch),
		pendingDelete: make(map[string]struct{}),
		deadCh:        make(chan struct{}),
	}, nil
}

var stoppedContext = func() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}()

func (p *Pipe) pipeContext() context.Context {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ctx == nil {
		return stoppedContext
	}
	return p.ctx
}

func (p *Pipe) failSession(err error) {
	if err == nil {
		err = transport.ErrPipeReset
	}
	p.deadMu.Lock()
	if p.deadErr == nil {
		p.deadErr = fmt.Errorf("%w: %v", transport.ErrPipeReset, err)
	}
	p.deadMu.Unlock()
	if p.logger != nil {
		p.logger.Warn("silo: session failed: ", err)
	}
	p.deadOnce.Do(func() { close(p.deadCh) })
}

func (p *Pipe) deadError() error {
	p.deadMu.Lock()
	defer p.deadMu.Unlock()
	if p.deadErr == nil {
		return transport.ErrPipeReset
	}
	return p.deadErr
}

func (p *Pipe) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()

	bucketCtx, cancelBucket := context.WithTimeout(ctx, bucketTimeout)
	if err := ensureBucket(bucketCtx, p.store, p.cfg.Bucket); err != nil {
		cancelBucket()
		return fmt.Errorf("silo: ensure bucket %q: %w", p.cfg.Bucket, err)
	}
	cancelBucket()

	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return nil
	}
	pipeCtx, cancel := context.WithCancel(ctx)
	p.ctx, p.cancel = pipeCtx, cancel
	p.started = true

	p.wg.Add(1)
	p.mu.Unlock()

	p.headMu.Lock()
	p.lastKey = ""
	p.historyHead = ""
	p.initialized = false
	p.headMu.Unlock()
	p.sawTraffic.Store(false)

	initCtx, cancelInit := context.WithTimeout(pipeCtx, s3RequestTimeout)
	p.initializeReadCursor(initCtx)
	cancelInit()

	p.mu.Lock()
	if pipeCtx.Err() != nil {
		p.started = false
		p.mu.Unlock()
		p.wg.Done()
		return nil
	}
	if p.role == "server" {
		p.wg.Add(1)
		go p.purgeStale(pipeCtx)
	}

	p.wg.Add(3)
	go p.receiveLoop(pipeCtx)
	go p.sendLoop(pipeCtx)
	go p.deleteLoop(pipeCtx)
	p.wg.Done()
	p.mu.Unlock()

	p.logger.Info("silo: pipe started (write=", p.writePrefix, ", read=", p.readPrefix, ")")
	return nil
}

func (p *Pipe) ResetReadHead() {
	p.rewindReadHead(true)
}

func (p *Pipe) rescanReadHead() {
	p.rewindReadHead(false)
}

func (p *Pipe) rewindReadHead(_ bool) {
	p.headMu.Lock()
	p.lastKey = p.historyHead
	p.headMu.Unlock()

	p.deferMu.Lock()
	p.deferred = make(map[string]*deferredBatch)
	p.deferMu.Unlock()

	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *Pipe) PurgeWritePrefix(ctx context.Context) {
	if err := p.purgeWritePrefix(ctx); err != nil && ctx.Err() == nil {
		p.logger.Debug("silo: purge ", p.writePrefix, ": ", err)
	}
}

func (p *Pipe) purgeWritePrefix(ctx context.Context) error {
	objectCh := p.store.ListObjects(ctx, p.cfg.Bucket, minio.ListObjectsOptions{
		Prefix:    p.writePrefix,
		Recursive: true,
	})
	var keys []string
	for object := range objectCh {
		if object.Err != nil {
			if ctx.Err() == nil {
				p.logger.Warn("silo: list objects under ", p.writePrefix, ": ", object.Err)
			}
			continue
		}
		keys = append(keys, object.Key)
	}
	for _, key := range keys {
		if err := p.store.RemoveObject(ctx, p.cfg.Bucket, key, minio.RemoveObjectOptions{}); err != nil &&
			!isMissingObject(err) {
			p.logger.Warn("silo: purge object ", key, ": ", err)
		}
	}
	return nil
}

func (p *Pipe) purgeStale(ctx context.Context) {
	defer p.wg.Done()
	purgeCtx, cancel := context.WithTimeout(ctx, purgeTimeout)
	defer cancel()
	if err := p.purgeWritePrefix(purgeCtx); err != nil && ctx.Err() == nil {
		p.logger.Debug("silo: background purge ", p.writePrefix, ": ", err)
	}
}

func (p *Pipe) deleteLoop(ctx context.Context) {
	defer p.wg.Done()

	interval := pendingDeleteInterval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.deadCh:
			return
		case <-ticker.C:
		}
		p.retryPendingDeletes(ctx)
	}
}

func (p *Pipe) flushDeletes(ctx context.Context) {
	p.deferMu.Lock()
	keys := make([]string, 0, len(p.pendingDelete))
	for key := range p.pendingDelete {
		keys = append(keys, key)
	}
	p.deferMu.Unlock()
	if len(keys) == 0 {
		return
	}
	for _, key := range keys {
		if ctx.Err() != nil {
			return
		}
		keyCtx, cancel := context.WithTimeout(ctx, s3RequestTimeout)
		ok := p.removeObject(keyCtx, key)
		cancel()
		if !ok {
			continue
		}
		p.deferMu.Lock()
		delete(p.pendingDelete, key)
		p.deferMu.Unlock()
	}
}

func (p *Pipe) Stop(ctx context.Context) error {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return nil
	}
	p.started = false
	cancel := p.cancel
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	p.wg.Wait()
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
	pipeCtx := p.pipeContext()
	cp := make([]byte, len(data))
	copy(cp, data)

	msg := uploadMsg{data: cp, done: make(chan error, 1)}
	select {
	case p.sendCh <- msg:
		p.lastWrite.Store(time.Now().UnixNano())
	case <-ctx.Done():
		return ctx.Err()
	case <-pipeCtx.Done():
		return transport.ErrPipeReset
	case <-p.deadCh:
		return p.deadError()
	}

	select {
	case err := <-msg.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-pipeCtx.Done():
		return transport.ErrPipeReset
	case <-p.deadCh:
		return p.deadError()
	}
}

func (p *Pipe) ReceiveMessage(ctx context.Context) ([]byte, error) {
	pipeCtx := p.pipeContext()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-pipeCtx.Done():
		return nil, transport.ErrPipeReset
	case <-p.deadCh:
		return nil, p.deadError()
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

func (p *Pipe) sendLoop(ctx context.Context) {
	defer p.wg.Done()

	minB := p.cfg.MinFileSizeKB * 1024
	maxB := p.cfg.MaxFileSizeKB * 1024
	flushDur := time.Duration(p.cfg.FlushMs) * time.Millisecond

	targetSize := rand.Intn(maxB-minB+1) + minB

	buffer := make([]byte, 0, targetSize)
	var pending []uploadMsg
	timer := time.NewTimer(flushDur)
	defer timer.Stop()

	flush := func(sendCtx context.Context) {
		if len(buffer) == 0 {
			for _, msg := range pending {
				resolveUpload(msg, nil)
			}
			pending = nil
			return
		}
		err := p.upload(sendCtx, buffer)
		for _, msg := range pending {
			resolveUpload(msg, err)
		}
		pending = nil
		buffer = buffer[:0]
		targetSize = rand.Intn(maxB-minB+1) + minB
	}

	defer func() {
		for _, msg := range pending {
			resolveUpload(msg, transport.ErrPipeReset)
		}
	}()

	for {
		select {
		case <-ctx.Done():

			flushCtx, cancel := context.WithTimeout(context.Background(), stopFlushTimeout)
			flush(flushCtx)
			cancel()
			return
		case <-p.deadCh:

			return
		case msg := <-p.sendCh:
			emptyBuffer := len(buffer) == 0
			var hdr [4]byte
			binary.BigEndian.PutUint32(hdr[:], uint32(len(msg.data)))
			buffer = append(buffer, hdr[:]...)
			buffer = append(buffer, msg.data...)
			pending = append(pending, msg)

			if emptyBuffer {

				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				next := flushDur
				if len(p.sendCh) == 0 {

					next = 0
				}
				timer.Reset(next)
			}

			if len(buffer) >= targetSize {
				flush(ctx)
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}

				timer.Reset(flushDur)
			}
		case <-timer.C:
			flush(ctx)
			timer.Reset(flushDur)
		}
	}
}

func (p *Pipe) upload(ctx context.Context, buf []byte) error {
	objName := fmt.Sprintf("msg_%d_%d", time.Now().UnixNano(), rand.Int63())
	fullKey := p.writePrefix + objName

	reqCtx, cancel := context.WithTimeout(ctx, s3RequestTimeout)
	defer cancel()
	_, err := p.store.PutObject(reqCtx, p.cfg.Bucket, fullKey, bytes.NewReader(buf), int64(len(buf)), minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		p.logger.Warn("silo: put object failed: ", err)
		return fmt.Errorf("silo: put object: %w", err)
	}

	p.ctr.NetworkSent.Add(int64(len(buf)))
	p.ctr.MessagesSent.Add(1)
	p.lastWrite.Store(time.Now().UnixNano())
	return nil
}

const (
	maxPollBackoff = 150 * time.Millisecond

	maxIdlePollInterval = 150 * time.Millisecond
)

var idleRescanInterval = 2 * time.Second

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

func (p *Pipe) receiveLoop(ctx context.Context) {
	defer p.wg.Done()

	base := time.Duration(p.cfg.PollIntervalMs) * time.Millisecond
	if base < 10*time.Millisecond {
		base = 10 * time.Millisecond
	}
	if base > maxIdlePollInterval {
		base = maxIdlePollInterval
	}
	delay := base
	timer := time.NewTimer(delay)
	defer timer.Stop()
	lastProgress := time.Now()
	sawTraffic := p.sawTraffic.Load()

	poll := func() bool {
		if p.idleHead(lastProgress) {
			p.rescanReadHead()
		}
		delivered := p.pollOnce(ctx)
		if delivered > 0 {
			sawTraffic = true

			delay = 0
			lastProgress = time.Now()
			return true
		}
		delay = nextPollDelay(delay, base, maxPollBackoff)
		if sawTraffic && downloadSilenceLimit > 0 && time.Since(lastProgress) >= downloadSilenceLimit {
			p.failSession(fmt.Errorf("silo: downlink silent for %s", downloadSilenceLimit))
			return false
		}
		return true
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.deadCh:
			return
		case <-p.wake:
			if !poll() {
				return
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(delay)
		case <-timer.C:
			if !poll() {
				return
			}
			timer.Reset(delay)
		}
	}
}

func (p *Pipe) idleHead(lastProgress time.Time) bool {
	p.headMu.Lock()
	hasHead := p.lastKey != ""
	p.headMu.Unlock()
	if !hasHead || time.Since(lastProgress) < idleRescanInterval {
		return false
	}

	if last := p.lastWrite.Load(); last != 0 {
		if time.Since(time.Unix(0, last)) < idleRescanInterval {
			return false
		}
	}
	return true
}

func (p *Pipe) pollOnce(ctx context.Context) int {
	p.headMu.Lock()
	initialized := p.initialized
	startAfter := p.lastKey
	p.headMu.Unlock()

	if !initialized {
		p.initializeReadCursor(ctx)
		p.headMu.Lock()
		if !p.initialized {
			p.headMu.Unlock()
			return 0
		}
		startAfter = p.lastKey
		p.headMu.Unlock()
	}

	opts := minio.ListObjectsOptions{
		Prefix:    p.readPrefix,
		Recursive: true,
	}
	if startAfter != "" {
		opts.StartAfter = startAfter
	}

	var objects []listedObject
	listCtx, cancelList := context.WithTimeout(ctx, s3RequestTimeout)
	objectCh := p.store.ListObjects(listCtx, p.cfg.Bucket, opts)

	for object := range objectCh {
		if object.Err != nil {
			if listCtx.Err() == nil && ctx.Err() == nil {
				p.logger.Warn("silo: list objects under ", p.readPrefix, ": ", object.Err)
			}
			continue
		}
		objects = append(objects, listedObject{
			key:      object.Key,
			size:     object.Size,
			modified: object.LastModified,
		})
	}
	cancelList()

	if len(objects) == 0 {
		return 0
	}

	sort.Slice(objects, func(i, j int) bool { return objects[i].key < objects[j].key })

	keep := make(map[string]struct{}, len(objects))
	for _, obj := range objects {
		keep[obj.key] = struct{}{}
	}
	p.pruneDeferred(keep)

	delivered := 0
	for _, obj := range objects {
		if startAfter != "" && obj.key <= startAfter {

			continue
		}
		if p.isPendingDelete(obj.key) {

			continue
		}

		if !p.processFile(ctx, obj) {
			break
		}

		p.advanceReadHead(obj.key)
		delivered++
	}
	return delivered
}

var existingHistorySkipThreshold = 16

func (p *Pipe) initializeReadCursor(ctx context.Context) bool {
	last, history, ok := p.initReadHead(ctx)
	if !ok {
		return false
	}
	p.headMu.Lock()
	if p.initialized {
		p.headMu.Unlock()
		return true
	}
	p.initialized = true
	p.headMu.Unlock()

	skipHistory := len(history) > existingHistorySkipThreshold || p.role != "server"
	if !skipHistory {

		for _, obj := range history {
			if ctx.Err() != nil {
				break
			}
			if !p.processFile(ctx, obj) {
				break
			}
			p.advanceReadHead(obj.key)
		}
		return true
	}

	p.headMu.Lock()
	p.lastKey = last
	p.historyHead = last
	p.headMu.Unlock()

	budget := time.Now().Add(staleInlineDeleteBudget)
	for _, obj := range history {
		key := obj.key
		if time.Now().After(budget) || ctx.Err() != nil {
			p.scheduleDelete(key)
			continue
		}
		delCtx, cancel := context.WithTimeout(ctx, s3RequestTimeout)
		ok := p.removeObject(delCtx, key)
		cancel()
		if !ok {
			p.scheduleDelete(key)
		}
	}
	p.logger.Info("silo: skipped ", len(history), " existing objects up to ", last)
	return true
}

func (p *Pipe) initReadHead(ctx context.Context) (string, []listedObject, bool) {
	listCtx, cancel := context.WithTimeout(ctx, s3RequestTimeout)
	defer cancel()

	objectCh := p.store.ListObjects(listCtx, p.cfg.Bucket, minio.ListObjectsOptions{
		Prefix:    p.readPrefix,
		Recursive: true,
	})
	objects := make([]listedObject, 0, 16)
	failed := false
	for object := range objectCh {
		if object.Err != nil {
			failed = true
			if listCtx.Err() == nil && ctx.Err() == nil {
				p.logger.Warn("silo: list objects under ", p.readPrefix, ": ", object.Err)
			}
			continue
		}
		objects = append(objects, listedObject{
			key:      object.Key,
			size:     object.Size,
			modified: object.LastModified,
		})
	}
	if listCtx.Err() != nil && ctx.Err() == nil {
		p.logger.Warn("silo: initial list under ", p.readPrefix, " timed out")
	}
	if failed {
		return "", nil, false
	}
	if len(objects) == 0 {
		return "", nil, true
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].key < objects[j].key })
	return objects[len(objects)-1].key, objects, true
}

func (p *Pipe) advanceReadHead(key string) {
	p.headMu.Lock()
	if key > p.lastKey {
		p.lastKey = key
	}
	p.headMu.Unlock()
}

func isMissingObject(err error) bool {
	if err == nil {
		return false
	}
	resp := minio.ToErrorResponse(err)
	return resp.Code == "NoSuchKey" || resp.StatusCode == http.StatusNotFound
}

func (p *Pipe) noteDeferred(key string, modified time.Time) bool {
	p.deferMu.Lock()
	defer p.deferMu.Unlock()
	if p.deferred == nil {
		p.deferred = make(map[string]*deferredBatch)
	}
	state := p.deferred[key]
	if state == nil {
		state = &deferredBatch{firstSeen: time.Now()}
		p.deferred[key] = state
	}
	state.attempts++
	if state.attempts > incompleteBatchMaxTries {
		return true
	}
	since := state.firstSeen
	if !modified.IsZero() {
		since = modified
	}
	return time.Since(since) > incompleteBatchGrace
}

func (p *Pipe) clearDeferred(key string) {
	p.deferMu.Lock()
	delete(p.deferred, key)
	p.deferMu.Unlock()
}

func (p *Pipe) pruneDeferred(keep map[string]struct{}) {
	p.deferMu.Lock()
	for key := range p.deferred {
		if _, ok := keep[key]; !ok {
			delete(p.deferred, key)
		}
	}
	p.deferMu.Unlock()
}

func (p *Pipe) deferOrDrop(obj listedObject, reason string) bool {
	if p.noteDeferred(obj.key, obj.modified) {
		p.logger.Warn("silo: dropping undeliverable object ", obj.key, " (", reason, ")")
		return true
	}
	p.logger.Debug("silo: deferring object ", obj.key, " (", reason, ")")
	return false
}

const maxPendingDeletes = 4096

func (p *Pipe) removeObject(ctx context.Context, key string) bool {
	for attempt := 0; attempt < 2; attempt++ {
		err := p.store.RemoveObject(ctx, p.cfg.Bucket, key, minio.RemoveObjectOptions{})
		if err == nil || isMissingObject(err) {
			return true
		}
		if attempt == 0 {
			select {
			case <-ctx.Done():
			case <-time.After(20 * time.Millisecond):
			}
			continue
		}
		p.logger.Warn("silo: remove object ", key, ": ", err)
	}
	return false
}

func (p *Pipe) scheduleDelete(key string) {
	p.deferMu.Lock()
	if p.pendingDelete == nil {
		p.pendingDelete = make(map[string]struct{})
	}
	if len(p.pendingDelete) < maxPendingDeletes {
		p.pendingDelete[key] = struct{}{}
	}
	p.deferMu.Unlock()
}

func (p *Pipe) isPendingDelete(key string) bool {
	p.deferMu.Lock()
	_, ok := p.pendingDelete[key]
	p.deferMu.Unlock()
	return ok
}

func (p *Pipe) retryPendingDeletes(ctx context.Context) {
	p.flushDeletes(ctx)
}

func (p *Pipe) processFile(ctx context.Context, obj listedObject) bool {
	key := obj.key

	readCtx, cancelRead := context.WithTimeout(ctx, s3RequestTimeout)
	objReader, err := p.store.GetObject(readCtx, p.cfg.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		cancelRead()
		return isMissingObject(err)
	}
	data, err := io.ReadAll(objReader)
	_ = objReader.Close()
	cancelRead()
	if err != nil {
		return isMissingObject(err)
	}

	if obj.size > 0 && int64(len(data)) != obj.size {
		if p.deferOrDrop(obj, "size mismatch") {
			return p.removeObject(ctx, key)
		}
		return false
	}

	messages, err := parseBatch(data, maxBatchMessageSize)
	if err != nil {
		if p.deferOrDrop(obj, err.Error()) {
			return p.removeObject(ctx, key)
		}
		return false
	}

	p.ctr.NetworkRecv.Add(int64(len(data)))
	if len(messages) > 0 {
		p.sawTraffic.Store(true)
	}
	for _, msg := range messages {
		p.ctr.UsefulRecv.Add(int64(len(msg)))
		p.ctr.MessagesRecv.Add(1)

		select {
		case p.recvCh <- msg:
		case <-ctx.Done():
			return false
		}
	}
	p.clearDeferred(key)

	if !p.removeObject(ctx, key) {
		p.scheduleDelete(key)
	}
	return true
}

var _ transport.Pipe = (*Pipe)(nil)
var _ transport.Telemetry = (*Pipe)(nil)
