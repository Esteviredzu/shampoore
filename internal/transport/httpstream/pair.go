package httpstream

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Esteviredzu/shampoore/internal/logging"
	"github.com/Esteviredzu/shampoore/internal/transport"
)

type PairBroker struct {
	handler *Handler
	logger  logging.ContextLogger

	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	rooms  map[string]*pairRoom
	closed bool
}

type pairRoom struct {
	key string

	mu     sync.Mutex
	client *Session
	exit   *Session

	seeds map[string]uplinkSeed

	pumpCancel context.CancelFunc
	pumpEpoch  uint64
	pumpWG     sync.WaitGroup
	closed     bool
}

type uplinkSeed struct {
	epoch     uint32
	delivered uint64
}

const pairSendTimeout = 20 * time.Second

const maxPairSeeds = 16

func NewPairBroker(profile MimicProfile, cfg ServerConfig) (*PairBroker, error) {
	profile = profile.Normalize()
	cfg.RequireUser = true
	cfg.PinnedUserID = ""
	cfg.PinnedExitNodeID = ""
	cfg.Authorize = nil
	cfg.AllowedRoles = []string{RoleClient, RoleExit}
	cfg.ReapStaleSessions = true
	if cfg.DownlinkReplayBytes <= 0 {
		cfg.DownlinkReplayBytes = profile.DownlinkReplayBytes
	}

	b := &PairBroker{
		rooms: make(map[string]*pairRoom),
	}
	cfg.SessionSeed = b.sessionSeed
	b.ctx, b.cancel = context.WithCancel(context.Background())

	handler := NewHandler(profile, cfg, b.handleSession)
	if err := handler.Err(); err != nil {
		b.cancel()
		return nil, err
	}
	b.handler = handler
	b.logger = handler.logger
	return b, nil
}

func (b *PairBroker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.handler.ServeHTTP(w, r)
}
func (b *PairBroker) Close() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	rooms := make([]*pairRoom, 0, len(b.rooms))
	for _, room := range b.rooms {
		rooms = append(rooms, room)
	}
	b.rooms = nil
	b.mu.Unlock()

	if b.cancel != nil {
		b.cancel()
	}
	for _, room := range rooms {
		room.mu.Lock()
		room.closed = true
		room.stopPumpsLocked()
		room.client, room.exit = nil, nil
		room.mu.Unlock()
	}
	if b.handler != nil {
		if err := b.handler.Close(); err != nil {
			return err
		}
	}
	for _, room := range rooms {
		room.pumpWG.Wait()
	}
	return nil
}

func (b *PairBroker) SessionCount() int {
	if b == nil || b.handler == nil {
		return 0
	}
	return b.handler.SessionCount()
}

func (b *PairBroker) RoomCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.rooms)
}

func (b *PairBroker) WalkLayers() []transport.Layer {
	if b == nil || b.handler == nil {
		return nil
	}
	return b.handler.WalkLayers()
}

func (b *PairBroker) handleSession(ctx context.Context, sess *Session) error {
	if ctx == nil {
		ctx = context.Background()
	}
	info := sess.Info()
	role := info.Role
	if role != RoleClient && role != RoleExit {
		return fmt.Errorf("httpstream: pair broker: unsupported role %q", role)
	}
	if info.UserID == "" {
		return fmt.Errorf("httpstream: pair broker: empty user id")
	}

	key := info.Namespace + "\x00" + info.UserID
	room := b.roomFor(key)
	if room == nil {
		return nil
	}

	stale, ready := room.attach(role, sess)
	if b.logger != nil {
		b.logger.Debug("pair: attach role=", role, " user=", info.UserID,
			" sid=", shortSessionID(info.SessionID), " stale=", stale != nil, " ready=", ready)
	}

	if stale != nil && stale != sess {
		_ = stale.Stop(context.Background())
	}
	if ready {
		room.startPumps(b)
	}

	<-ctx.Done()

	current := room.detach(role, sess)
	if b.logger != nil {
		b.logger.Debug("pair: detach role=", role, " user=", info.UserID,
			" sid=", shortSessionID(info.SessionID), " current=", current)
	}
	if current {
		b.dropRoomIfEmpty(key, room)
	}
	return nil
}

func (b *PairBroker) sessionSeed(info SessionInfo) (uint32, uint64) {
	if info.UserID == "" {
		return 0, 0
	}
	key := info.Namespace + "\x00" + info.UserID
	seedKey := seedKey(info.Role, info.SessionID)
	b.mu.Lock()
	defer b.mu.Unlock()
	room := b.rooms[key]
	if room == nil {
		return 0, 0
	}
	room.mu.Lock()
	defer room.mu.Unlock()
	if seed, ok := room.seeds[seedKey]; ok {
		return seed.epoch, seed.delivered
	}

	if current := room.slot(info.Role); current != nil &&
		strings.EqualFold(current.SessionID(), info.SessionID) {
		return current.UplinkState()
	}
	return 0, 0
}

func (b *PairBroker) roomFor(key string) *pairRoom {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	room := b.rooms[key]
	if room == nil {
		room = &pairRoom{key: key}
		b.rooms[key] = room
	}
	return room
}

func (b *PairBroker) dropRoomIfEmpty(key string, room *pairRoom) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rooms[key] != room {
		return
	}
	room.mu.Lock()
	empty := room.client == nil && room.exit == nil
	room.mu.Unlock()
	if empty {
		delete(b.rooms, key)
	}
}

func (b *PairBroker) pump(ctx context.Context, room *pairRoom, epoch uint64, src, dst *Session) {
	defer room.finishPump(epoch)
	for {
		msg, err := src.Receive(ctx)
		if err != nil {
			if ctx.Err() == nil && b.logger != nil {
				b.logger.Debug("pair: pump receive stop: ", err)
			}
			return
		}
		sendCtx, cancel := context.WithTimeout(ctx, pairSendTimeout)
		err = dst.Send(sendCtx, msg)
		cancel()
		if err != nil {
			if ctx.Err() == nil && b.logger != nil {
				b.logger.Debug("pair: pump send stop: ", err)
			}

			room.dropPeer(epoch, dst)
			return
		}
	}
}

func (r *pairRoom) attach(role string, sess *Session) (stale *Session, ready bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, false
	}
	old := r.slot(role)
	if old == sess {
		return nil, r.readyLocked()
	}
	if old != nil {
		r.recordLocked(role, old)
		stale = old
	}
	r.stopPumpsLocked()
	r.setSlot(role, sess)
	return stale, r.readyLocked()
}

func (r *pairRoom) detach(role string, sess *Session) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.slot(role) != sess {
		return false
	}
	r.recordLocked(role, sess)
	r.stopPumpsLocked()
	r.setSlot(role, nil)
	return true
}

func (r *pairRoom) startPumps(b *PairBroker) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.pumpCancel != nil || r.client == nil || r.exit == nil {
		return
	}
	ctx, cancel := context.WithCancel(b.ctx)
	r.pumpEpoch++
	epoch := r.pumpEpoch
	r.pumpCancel = cancel
	client, exit := r.client, r.exit
	r.pumpWG.Add(2)
	go func() {
		defer r.pumpWG.Done()
		b.pump(ctx, r, epoch, client, exit)
	}()
	go func() {
		defer r.pumpWG.Done()
		b.pump(ctx, r, epoch, exit, client)
	}()
}

func (r *pairRoom) finishPump(epoch uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pumpEpoch != epoch {
		return
	}
	if r.pumpCancel != nil {
		r.pumpCancel()
		r.pumpCancel = nil
	}
	r.pumpEpoch++
}

func (r *pairRoom) dropPeer(epoch uint64, sess *Session) {
	if sess == nil {
		return
	}
	r.mu.Lock()
	if r.closed || r.pumpEpoch != epoch {
		r.mu.Unlock()
		return
	}
	role := RoleClient
	switch sess {
	case r.exit:
		role = RoleExit
	case r.client:
	default:
		r.mu.Unlock()
		return
	}
	r.recordLocked(role, sess)
	r.setSlot(role, nil)
	r.stopPumpsLocked()
	r.mu.Unlock()
	_ = sess.Stop(context.Background())
}

func (r *pairRoom) stopPumpsLocked() {
	if r.pumpCancel != nil {
		r.pumpCancel()
		r.pumpCancel = nil
	}
	r.pumpEpoch++
}

func (r *pairRoom) slot(role string) *Session {
	if role == RoleExit {
		return r.exit
	}
	return r.client
}

func (r *pairRoom) setSlot(role string, sess *Session) {
	if role == RoleExit {
		r.exit = sess
		return
	}
	r.client = sess
}

func (r *pairRoom) readyLocked() bool {
	return r.client != nil && r.exit != nil
}

func (r *pairRoom) recordLocked(role string, sess *Session) {
	if sess == nil {
		return
	}
	if r.seeds == nil {
		r.seeds = make(map[string]uplinkSeed)
	}
	key := seedKey(role, sess.Info().SessionID)
	if _, ok := r.seeds[key]; !ok && len(r.seeds) >= maxPairSeeds {

		for stale := range r.seeds {
			delete(r.seeds, stale)
			break
		}
	}
	epoch, delivered := sess.UplinkState()
	r.seeds[key] = uplinkSeed{epoch: epoch, delivered: delivered}
}

func seedKey(role, sessionID string) string {
	return role + "\x00" + strings.ToLower(strings.TrimSpace(sessionID))
}
