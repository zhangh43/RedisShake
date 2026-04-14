package writer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/ratelimit"

	"RedisShake/internal/client"
	"RedisShake/internal/client/proto"
	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
)

type RedisWriterOptions struct {
	Cluster   bool                   `mapstructure:"cluster" default:"false"`
	Address   string                 `mapstructure:"address" default:""`
	Username  string                 `mapstructure:"username" default:""`
	Password  string                 `mapstructure:"password" default:""`
	Tls       bool                   `mapstructure:"tls" default:"false"`
	TlsConfig client.TlsConfig       `mapstructure:"tls_config" default:"{}"`
	OffReply  bool                   `mapstructure:"off_reply" default:"false"`
	Sentinel  client.SentinelOptions `mapstructure:"sentinel"`
}

type redisStandaloneWriter struct {
	address     string
	client      *client.Redis
	DbId        int
	ctx         context.Context
	reconnectFn func() error

	chWaitReply chan *replyItem
	chWaitWg    sync.WaitGroup
	offReply    bool
	ch          chan *entry.Entry
	chWg        sync.WaitGroup
	replyEpoch  uint64
	pendingMu   sync.Mutex
	pending     []*entry.Entry
	replayMu    sync.Mutex
	replayQueue []*entry.Entry
	maxQPS      int
	pipeLimit   uint64

	stat struct {
		Name              string `json:"name"`
		UnansweredBytes   int64  `json:"unanswered_bytes"`
		UnansweredEntries int64  `json:"unanswered_entries"`
	}
}

type replyItem struct {
	entry *entry.Entry
	epoch uint64
}

func (w *redisStandaloneWriter) reconnectIO() bool {
	if !config.Opt.Advanced.IOReconnect {
		return false
	}
	delay := time.Duration(config.Opt.Advanced.IOReconnectDelayMs) * time.Millisecond
	for attempt := 1; attempt <= config.Opt.Advanced.IOReconnectMaxTimes; attempt++ {
		log.Warnf("[%s] reconnecting target redis. attempt=[%d/%d], delay=[%s]",
			w.stat.Name, attempt, config.Opt.Advanced.IOReconnectMaxTimes, delay)
		time.Sleep(delay)
		reconnectFn := w.reconnectFn
		if reconnectFn == nil {
			reconnectFn = w.client.Reconnect
		}
		if err := reconnectFn(); err == nil {
			if w.DbId != 0 {
				reply, selectErr := w.client.DoWithStringReplyWithError("select", strconv.Itoa(w.DbId))
				if selectErr != nil || reply != "OK" {
					continue
				}
			}
			log.Warnf("[%s] reconnected target redis. db=[%d]", w.stat.Name, w.DbId)
			return true
		}
	}
	return false
}

func NewRedisStandaloneWriter(ctx context.Context, opts *RedisWriterOptions) Writer {
	shards := resolveStandaloneWriterShards()
	log.Infof("redis_writer effective option: cluster=[%v], address=[%s], target_redis_writer_shards=[%d], resolved_writer_shards=[%d]",
		opts.Cluster, opts.Address, config.Opt.Advanced.TargetRedisWriterShards, shards)
	if shards > 1 {
		return newRedisShardedStandaloneWriter(ctx, opts, shards)
	}
	return newRedisStandaloneWriterWithLimits(ctx, opts, config.Opt.Advanced.TargetRedisMaxQPS, config.Opt.Advanced.PipelineCountLimit)
}

func newRedisStandaloneWriterWithLimits(ctx context.Context, opts *RedisWriterOptions, maxQPS int, pipeLimit uint64) *redisStandaloneWriter {
	if maxQPS <= 0 {
		maxQPS = config.Opt.Advanced.TargetRedisMaxQPS
	}
	if pipeLimit == 0 {
		pipeLimit = config.Opt.Advanced.PipelineCountLimit
	}
	rw := new(redisStandaloneWriter)
	rw.address = opts.Address
	rw.stat.Name = "writer_" + strings.Replace(opts.Address, ":", "_", -1)
	rw.client = client.NewRedisClient(ctx, opts.Address, opts.Username, opts.Password, opts.Tls, opts.TlsConfig, false)
	rw.maxQPS = maxQPS
	rw.pipeLimit = pipeLimit
	pipeCap := int(rw.pipeLimit)
	rw.ch = make(chan *entry.Entry, pipeCap)
	if opts.OffReply {
		log.Infof("turn off the reply of write")
		rw.offReply = true
		rw.client.Send("CLIENT", "REPLY", "OFF")
	} else {
		rw.chWaitReply = make(chan *replyItem, pipeCap*2)
		rw.chWaitWg.Add(1)
		go rw.processReply()
	}
	return rw
}

func (w *redisStandaloneWriter) Close() {
	if !w.offReply {
		close(w.ch)
		w.chWg.Wait()
		close(w.chWaitReply)
		w.chWaitWg.Wait()
	}
}

func (w *redisStandaloneWriter) StartWrite(ctx context.Context) chan *entry.Entry {
	w.ctx = ctx
	w.chWg = sync.WaitGroup{}
	w.chWg.Add(1)
	go w.processWrite(ctx)
	return w.ch
}

func (w *redisStandaloneWriter) Write(e *entry.Entry) {
	w.ch <- e
}

func (w *redisStandaloneWriter) addPending(e *entry.Entry) {
	if e == nil || strings.EqualFold(e.CmdName, "select") {
		return
	}
	w.pendingMu.Lock()
	w.pending = append(w.pending, e)
	w.pendingMu.Unlock()
}

func (w *redisStandaloneWriter) ackPending(e *entry.Entry) {
	if e == nil || strings.EqualFold(e.CmdName, "select") {
		return
	}
	w.pendingMu.Lock()
	defer w.pendingMu.Unlock()
	if len(w.pending) == 0 {
		return
	}
	w.pending = w.pending[1:]
}

func (w *redisStandaloneWriter) resetInflight() []*entry.Entry {
	w.pendingMu.Lock()
	pending := append([]*entry.Entry(nil), w.pending...)
	w.pending = nil
	w.pendingMu.Unlock()
	atomic.StoreInt64(&w.stat.UnansweredBytes, 0)
	atomic.StoreInt64(&w.stat.UnansweredEntries, 0)
	atomic.AddUint64(&w.replyEpoch, 1)
	return pending
}

func (w *redisStandaloneWriter) replayInflight() {
	entries := w.resetInflight()
	if len(entries) == 0 {
		return
	}
	w.enqueueReplay(entries)
}

func (w *redisStandaloneWriter) enqueueReplay(entries []*entry.Entry) {
	if len(entries) == 0 {
		return
	}
	w.replayMu.Lock()
	w.replayQueue = append(w.replayQueue, entries...)
	w.replayMu.Unlock()
}

func (w *redisStandaloneWriter) nextReplay() *entry.Entry {
	w.replayMu.Lock()
	defer w.replayMu.Unlock()
	if len(w.replayQueue) == 0 {
		return nil
	}
	e := w.replayQueue[0]
	w.replayQueue = w.replayQueue[1:]
	return e
}

func (w *redisStandaloneWriter) switchDbTo(newDbId int) {
	log.Debugf("[%s] switch db to [%d]", w.stat.Name, newDbId)
	w.client.Send("select", strconv.Itoa(newDbId))
	w.DbId = newDbId
	if !w.offReply {
		w.chWaitReply <- &replyItem{
			entry: &entry.Entry{
				Argv:    []string{"select", strconv.Itoa(newDbId)},
				CmdName: "select",
			},
			epoch: atomic.LoadUint64(&w.replyEpoch),
		}
	}
}

func (w *redisStandaloneWriter) processWrite(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	var rl ratelimit.Limiter = nil
	rl = ratelimit.New(w.maxQPS)
	log.Infof("set target redis max qps to %d", w.maxQPS)

	for {
		if replayEntry := w.nextReplay(); replayEntry != nil {
			if w.processEntry(rl, replayEntry) {
				continue
			}
			continue
		}
		select {
		case <-ctx.Done():
			// do nothing until w.ch is closed
		case <-ticker.C:
			if err := w.client.FlushWithError(); err != nil {
				if client.IsReconnectableIOError(err) && w.reconnectIO() {
					w.replayInflight()
					continue
				}
				log.Panicf("[%s] flush failed. error=[%v]", w.stat.Name, err)
			}
		case e, ok := <-w.ch:
			if !ok {
				// clean up and exit
				w.client.Flush()
				w.chWg.Done()
				return
			}
			if w.processEntry(rl, e) {
				continue
			}
		}
	}
}

// processEntry returns true when caller should continue outer write loop immediately.
func (w *redisStandaloneWriter) processEntry(rl ratelimit.Limiter, e *entry.Entry) bool {
	// switch db if we need
	if w.DbId != e.DbId {
		w.switchDbTo(e.DbId)
	}
	// send
	bytes := e.Serialize()
	for e.SerializedSize+atomic.LoadInt64(&w.stat.UnansweredBytes) > config.Opt.Advanced.TargetRedisClientMaxQuerybufLen {
		time.Sleep(1 * time.Nanosecond)
	}
	rl.Take()
	log.Debugf("[%s] send cmd. cmd=[%s]", w.stat.Name, e.String())
	if !w.offReply {
		w.addPending(e)
		item := &replyItem{entry: e, epoch: atomic.LoadUint64(&w.replyEpoch)}
		select {
		case w.chWaitReply <- item:
		default:
			if err := w.client.FlushWithError(); err != nil {
				if client.IsReconnectableIOError(err) && w.reconnectIO() {
					w.replayInflight()
					return true
				}
				log.Panicf("[%s] flush failed. error=[%v]", w.stat.Name, err)
			}
			w.chWaitReply <- item
		}
		atomic.AddInt64(&w.stat.UnansweredBytes, e.SerializedSize)
		atomic.AddInt64(&w.stat.UnansweredEntries, 1)
	}
	if err := w.client.SendBytesBuffWithError(bytes); err != nil {
		if client.IsReconnectableIOError(err) && w.reconnectIO() {
			w.replayInflight()
			return true
		}
		log.Panicf("[%s] send failed. cmd=[%s], error=[%v]", w.stat.Name, e.String(), err)
	}
	return false
}

func (w *redisStandaloneWriter) processReply() {
	for item := range w.chWaitReply {
		if item.epoch != atomic.LoadUint64(&w.replyEpoch) {
			continue
		}
		e := item.entry
		reply, err := w.client.Receive()
		log.Debugf("[%s] receive reply. reply=[%v], cmd=[%s]", w.stat.Name, reply, e.String())

		// It's good to skip the nil error since some write commands will return the null reply. For example,
		// the SET command with NX option will return nil if the key already exists.
		if err != nil && !errors.Is(err, proto.Nil) {
			if client.IsReconnectableIOError(err) && w.reconnectIO() {
				w.replayInflight()
				continue
			}
			if err.Error() == "BUSYKEY Target key name already exists." {
				if config.Opt.Advanced.RDBRestoreCommandBehavior == "skip" {
					log.Debugf("[%s] redisStandaloneWriter received BUSYKEY reply. cmd=[%s]", w.stat.Name, e.String())
				} else if config.Opt.Advanced.RDBRestoreCommandBehavior == "panic" {
					log.Panicf("[%s] redisStandaloneWriter received BUSYKEY reply. cmd=[%s]", w.stat.Name, e.String())
				}
			} else if w.shouldRequeueOnOOM(err, e) {
				w.ackPending(e)
				atomic.AddInt64(&w.stat.UnansweredBytes, -e.SerializedSize)
				atomic.AddInt64(&w.stat.UnansweredEntries, -1)
				e.RetryCount++
				delay := time.Duration(config.Opt.Advanced.TargetRedisOOMRequeueDelayMs) * time.Millisecond
				log.Warnf("[%s] receive reply failed with OOM. requeue cmd=[%s], retry_count=[%d/%d], delay=[%s]",
					w.stat.Name, e.String(), e.RetryCount, config.Opt.Advanced.TargetRedisOOMRequeueMaxTimes, delay)
				go func(en *entry.Entry) {
					time.Sleep(delay)
					if w.ctx != nil {
						select {
						case <-w.ctx.Done():
							return
						default:
						}
					}
					w.Write(en)
				}(e)
				continue
			} else {
				log.Panicf("[%s] receive reply failed. cmd=[%s], error=[%v]", w.stat.Name, e.String(), err)
			}
		}
		w.ackPending(e)
		atomic.AddInt64(&w.stat.UnansweredBytes, -e.SerializedSize)
		atomic.AddInt64(&w.stat.UnansweredEntries, -1)
	}
	w.chWaitWg.Done()
}

func (w *redisStandaloneWriter) shouldRequeueOnOOM(err error, e *entry.Entry) bool {
	if !config.Opt.Advanced.TargetRedisOOMRequeue {
		return false
	}
	if e == nil || strings.EqualFold(e.CmdName, "select") {
		return false
	}
	if e.RetryCount >= config.Opt.Advanced.TargetRedisOOMRequeueMaxTimes {
		return false
	}
	if err == nil {
		return false
	}
	return isTargetRedisOOMError(err)
}

func isTargetRedisOOMError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "out of memory") || strings.Contains(msg, "oom")
}

func (w *redisStandaloneWriter) Status() interface{} {
	return w.stat
}

func (w *redisStandaloneWriter) StatusString() string {
	return fmt.Sprintf("[%s]: unanswered_entries=%d", w.stat.Name, atomic.LoadInt64(&w.stat.UnansweredEntries))
}

func (w *redisStandaloneWriter) StatusConsistent() bool {
	return atomic.LoadInt64(&w.stat.UnansweredBytes) == 0 && atomic.LoadInt64(&w.stat.UnansweredEntries) == 0
}
