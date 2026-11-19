package telemost

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Esteviredzu/shampoore/internal/core/channel/vp8"
	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

const (
	publishTimeout   = 45 * time.Second
	subscribeTimeout = 30 * time.Second
	retryMinDelay    = time.Second
	retryMaxDelay    = 30 * time.Second
)

type session struct {
	sig    *signaling
	tunnel *vp8.Tunnel

	lost     chan struct{}
	done     chan struct{}
	lostOnce sync.Once
	closing  atomic.Bool
}

func (s *session) markLost() {
	if s == nil {
		return
	}
	s.lostOnce.Do(func() { close(s.lost) })
}

func (s *session) isClosing() bool {
	return s != nil && s.closing.Load()
}

func (s *session) Close() {
	if s == nil {
		return
	}
	s.closing.Store(true)
	if s.sig != nil {
		s.sig.Close()
	}
	if s.done != nil {
		<-s.done
	}
}

type openSessionFunc func(ctx context.Context, boot *bootstrapResult, reuse *vp8.Tunnel) (*session, error)

type Pipe struct {
	cfg    *Config
	logger logging.ContextLogger

	mu      sync.RWMutex
	boot    *bootstrapResult
	sig     *signaling
	tunnel  *vp8.Tunnel
	sess    *session
	started bool
	stopped bool
	cancel  context.CancelFunc

	wg       sync.WaitGroup
	stopOnce sync.Once

	resetMu            sync.RWMutex
	transportResetFunc func()

	bypass bypassCache

	openSessionFn openSessionFunc

	publishWait   time.Duration
	subscribeWait time.Duration
	retryMin      time.Duration
	retryMax      time.Duration
}

func NewPipe(cfg *Config, boot *bootstrapResult, logger logging.ContextLogger) *Pipe {
	return &Pipe{
		cfg:           cfg,
		boot:          boot,
		logger:        logger,
		publishWait:   publishTimeout,
		subscribeWait: subscribeTimeout,
		retryMin:      retryMinDelay,
		retryMax:      retryMaxDelay,
	}
}

func (p *Pipe) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return errors.New("telemost: pipe is stopped")
	}
	if p.started {
		p.mu.Unlock()
		return nil
	}
	p.started = true
	runCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	boot := p.boot
	p.mu.Unlock()

	sess, err := p.open(runCtx, boot, nil)
	if err != nil {
		cancel()
		p.mu.Lock()
		p.started = false
		p.cancel = nil
		p.mu.Unlock()
		return fmt.Errorf("telemost: start session: %w", err)
	}

	p.mu.Lock()
	p.sess = sess
	p.sig = sess.sig
	p.tunnel = sess.tunnel
	p.wg.Add(1)
	p.mu.Unlock()

	go p.supervise(runCtx)

	p.logger.Info("telemost: started")
	return nil
}

func (p *Pipe) Stop(_ context.Context) error {
	p.stopOnce.Do(func() {
		p.mu.Lock()
		p.stopped = true
		cancel := p.cancel
		sess := p.sess
		p.mu.Unlock()

		if cancel != nil {
			cancel()
		}
		sess.Close()
		p.wg.Wait()

		p.mu.Lock()
		tunnel := p.tunnel
		p.tunnel = nil
		p.sess = nil
		p.sig = nil
		p.mu.Unlock()

		if tunnel != nil {
			if err := tunnel.Close(); err != nil {
				p.logger.Warn("telemost: close tunnel: ", err)
			}
		}
		p.bypass.clear()
	})
	return nil
}

func (p *Pipe) Tunnel() *vp8.Tunnel {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.tunnel
}

func (p *Pipe) NotifyNetworkChanged() {
	p.mu.RLock()
	sess := p.sess
	p.mu.RUnlock()
	if sess == nil || sess.isClosing() {
		return
	}
	p.logger.Info("telemost: network changed, restarting session")
	sess.markLost()
}

func (p *Pipe) SetTransportResetFunc(fn func()) {
	p.resetMu.Lock()
	p.transportResetFunc = fn
	p.resetMu.Unlock()
}

func (p *Pipe) supervise(ctx context.Context) {
	defer p.wg.Done()

	defer func() {
		p.mu.Lock()
		sess := p.sess
		p.sess = nil
		p.sig = nil
		p.mu.Unlock()
		sess.Close()
	}()

	for {
		p.mu.RLock()
		sess := p.sess
		p.mu.RUnlock()
		if sess == nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-sess.lost:
		}

		p.logger.Warn("telemost: session lost, restarting (", transport.ErrTransportReset, ")")

		sess.Close()
		p.mu.Lock()
		if p.sess == sess {
			p.sess = nil
			p.sig = nil
		}
		reuse := p.tunnel
		p.mu.Unlock()

		if reuse != nil {
			reuse.InvalidateSession()
		}

		delay := p.minRetry()
		for {
			if err := sleepCtx(ctx, delay); err != nil {
				return
			}

			boot, err := bootstrap(ctx, p.cfg)
			if err != nil {
				p.logger.Warn("telemost: bootstrap for restart failed: ", err)
				delay = nextRetryDelay(delay, p.maxRetry())
				continue
			}

			newSess, err := p.open(ctx, boot, reuse)
			if err != nil {
				p.logger.Warn("telemost: session restart failed: ", err)
				delay = nextRetryDelay(delay, p.maxRetry())
				continue
			}

			p.mu.Lock()
			p.boot = boot
			p.sess = newSess
			p.sig = newSess.sig
			if p.tunnel == nil {
				p.tunnel = newSess.tunnel
			}
			stopped := p.stopped
			p.mu.Unlock()

			if stopped {
				newSess.Close()
				return
			}

			p.logger.Info("telemost: session restarted; signalling transport reset")
			p.fireTransportReset()
			break
		}
	}
}

func (p *Pipe) open(
	ctx context.Context,
	boot *bootstrapResult,
	reuse *vp8.Tunnel,
) (*session, error) {
	p.mu.RLock()
	fn := p.openSessionFn
	p.mu.RUnlock()
	if fn != nil {
		return fn(ctx, boot, reuse)
	}
	return p.openSession(ctx, boot, reuse)
}

func (p *Pipe) fireTransportReset() {
	p.resetMu.RLock()
	fn := p.transportResetFunc
	p.resetMu.RUnlock()
	if fn != nil {
		fn()
	}
}

func (p *Pipe) publishWaitDuration() time.Duration {
	if p.publishWait > 0 {
		return p.publishWait
	}
	return publishTimeout
}

func (p *Pipe) subscribeWaitDuration() time.Duration {
	if p.subscribeWait > 0 {
		return p.subscribeWait
	}
	return subscribeTimeout
}

func (p *Pipe) minRetry() time.Duration {
	if p.retryMin > 0 {
		return p.retryMin
	}
	return retryMinDelay
}

func (p *Pipe) maxRetry() time.Duration {
	if p.retryMax > 0 {
		return p.retryMax
	}
	return retryMaxDelay
}

func nextRetryDelay(cur, max time.Duration) time.Duration {
	next := cur * 2
	if next <= 0 || next > max {
		return max
	}
	return next
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (p *Pipe) openSession(
	ctx context.Context,
	boot *bootstrapResult,
	reuse *vp8.Tunnel,
) (*session, error) {
	if boot == nil {
		var err error
		boot, err = bootstrap(ctx, p.cfg)
		if err != nil {
			return nil, err
		}
	}
	sig := newSignaling(p.cfg, boot, p.logger)
	if reuse != nil {
		sig.setReuseTunnel(reuse)
	}

	sess := &session{
		sig:    sig,
		tunnel: reuse,
		lost:   make(chan struct{}),
		done:   make(chan struct{}),
	}

	pubReady := make(chan struct{})
	subReady := make(chan struct{})
	var pubOnce, subOnce sync.Once

	sig.setPublishReadyCallback(func() { pubOnce.Do(func() { close(pubReady) }) })
	sig.setSubscribeCallback(func() { subOnce.Do(func() { close(subReady) }) })
	sig.setSubscriberReconnectCallback(func() {
		p.logger.Info("telemost: signaling reconnect")
		p.fireTransportReset()
	})
	sig.setSessionLostCallback(func(reason string) {
		if sess.isClosing() {
			return
		}
		p.logger.Warn("telemost: signalling reported a lost session: ", reason)
		sess.markLost()
	})

	runErr := make(chan error, 1)
	go func() {
		err := sig.Run(ctx)
		runErr <- err
		close(sess.done)
	}()

	go func() {
		select {
		case <-ctx.Done():
		case <-sess.done:
			if !sess.isClosing() {
				sess.markLost()
			}
		}
	}()

	var createdTunnel *vp8.Tunnel
	abort := func(err error) (*session, error) {

		if reuse == nil && createdTunnel == nil {
			createdTunnel = sig.Tunnel()
		}
		sess.Close()
		if reuse == nil && createdTunnel != nil {
			if cerr := createdTunnel.Close(); cerr != nil {
				p.logger.Warn("telemost: close tunnel after failed start: ", cerr)
			}
		}
		return nil, err
	}

	p.logger.Info("telemost: waiting for publish signaling...")
	pubTimer := time.NewTimer(p.publishWaitDuration())
	defer pubTimer.Stop()
	select {
	case <-pubReady:
	case <-sess.lost:
		return abort(errors.New("telemost: session lost during start"))
	case err := <-runErr:
		return abort(fmt.Errorf("telemost: signaling stopped: %w", err))
	case <-ctx.Done():
		return abort(ctx.Err())
	case <-pubTimer.C:
		return abort(errors.New("telemost: publish signaling timeout"))
	}
	p.logger.Info("telemost: publish ready")

	tunnel := sig.Tunnel()
	if tunnel == nil {
		return abort(errors.New("telemost: tunnel not created"))
	}
	sess.tunnel = tunnel
	if reuse == nil {
		createdTunnel = tunnel
	}
	tunnel.SetReconnectCallback(func() {
		p.logger.Info("telemost: tunnel gap+keyframe (KCP reset deferred to signaling)")
	})

	go func() {
		select {
		case <-ctx.Done():
		case <-sess.done:
		case err := <-tunnel.Errors():
			if sess.isClosing() {
				return
			}
			p.logger.Warn("telemost: receive path failed: ", err)
			sess.markLost()
		}
	}()

	p.logger.Info("telemost: waiting for subscribe signaling...")
	subTimer := time.NewTimer(p.subscribeWaitDuration())
	defer subTimer.Stop()
	select {
	case <-subReady:
		p.logger.Info("telemost: subscribe ready")
	case <-sess.lost:
		return abort(errors.New("telemost: session lost during start"))
	case err := <-runErr:
		return abort(fmt.Errorf("telemost: signaling stopped: %w", err))
	case <-ctx.Done():
		return abort(ctx.Err())
	case <-subTimer.C:
		p.logger.Warn("telemost: no subscriber yet, continuing")
	}

	p.updateBypassHosts(boot)
	return sess, nil
}
