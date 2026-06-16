package reader

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"RedisShake/internal/client"
	"RedisShake/internal/client/proto"
	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"RedisShake/internal/rdb/types"
	"RedisShake/internal/utils"
)

type ScanReaderOptions struct {
	Cluster               bool             `mapstructure:"cluster" default:"false"`
	Address               string           `mapstructure:"address" default:""`
	Username              string           `mapstructure:"username" default:""`
	Password              string           `mapstructure:"password" default:""`
	Tls                   bool             `mapstructure:"tls" default:"false"`
	TlsConfig             client.TlsConfig `mapstructure:"tls_config" default:"{}"`
	ReadMode              string           `mapstructure:"read_mode" default:"dump"`
	Scan                  bool             `mapstructure:"scan" default:"true"`
	KSN                   bool             `mapstructure:"ksn" default:"false"`
	DBS                   []int            `mapstructure:"dbs"`
	PreferReplica         bool             `mapstructure:"prefer_replica" default:"false"`
	Count                 int              `mapstructure:"count" default:"1"`
	ScanHighQueueLen      int              `mapstructure:"scan_high_queue_len" default:"500000"`
	ScanLowQueueLen       int              `mapstructure:"scan_low_queue_len" default:"0"`
	DropKSNOnBackpressure bool             `mapstructure:"drop_ksn_on_backpressure" default:"false"`
	SkipUnknownType       []string         `mapstructure:"skip_unknown_type" default:"[]"`
	SampleOnStart         bool             `mapstructure:"sample_on_start" default:"true"`
	SampleCountPerDB      int              `mapstructure:"sample_count_per_db" default:"3"`
	SampleValueMaxLen     int              `mapstructure:"sample_value_max_len" default:"256"`
}

type dbKey struct {
	db  int
	key string
}

type needRestoreItem struct {
	dbId int
	key  string
}

type scanStandaloneReader struct {
	ctx             context.Context
	dbs             []int
	opts            *ScanReaderOptions
	ch              chan *entry.Entry
	needDumpQueue   *utils.UniqueQueue
	needRestoreChan chan *needRestoreItem
	dumpClient      *client.Redis
	subWG           sync.WaitGroup
	queueLen        func() int
	reconnectFn     func(*client.Redis) error
	isValkey        bool
	ksnDropLogged   bool
	scanPaused      atomic.Bool

	stat struct {
		Name               string  `json:"name"`
		ScanFinished       bool    `json:"scan_finished"`
		ScanDbId           int     `json:"scan_dbId"`
		ScanCursor         uint64  `json:"scan_cursor"`
		SyncPercent        string  `json:"sync_percent"`
		SyncedKeyCount     int64   `json:"synced_key_count"`
		SyncedKeyOps       float64 `json:"synced_key_ops"`
		NeedUpdateCount    int64   `json:"need_update_count"`
		KSNDroppedCount    int64   `json:"ksn_dropped_count"`
		EstimatedTotalKeys int64   `json:"estimated_total_keys"`
		ScannedKeyCount    int64   `json:"scanned_key_count"`

		lastSyncedKeyCount          int64
		lastSyncedKeyUpdateTSSecond float64
	}
}

func NewScanStandaloneReader(ctx context.Context, opts *ScanReaderOptions) Reader {
	r := new(scanStandaloneReader)
	r.dbs = opts.DBS
	r.opts = opts
	r.ch = make(chan *entry.Entry, 1024)
	r.stat.Name = "reader_" + strings.Replace(opts.Address, ":", "_", -1)
	r.needDumpQueue = utils.NewUniqueQueue(100000000)     // cache 100000000 keys
	r.needRestoreChan = make(chan *needRestoreItem, 1024) // inflight 1024 keys
	r.queueLen = r.needDumpQueue.Len
	log.Infof("[%s] scanStandaloneReader init finished. dbs=[%v]", r.stat.Name, r.dbs)
	return r
}

func (r *scanStandaloneReader) getQueueLen() int {
	if r.queueLen != nil {
		return r.queueLen()
	}
	return 0
}

func (r *scanStandaloneReader) getScanHighQueueLen() int {
	return r.opts.ScanHighQueueLen
}

func (r *scanStandaloneReader) getScanLowQueueLen() int {
	if r.opts.ScanLowQueueLen > 0 {
		return r.opts.ScanLowQueueLen
	}
	return r.opts.ScanHighQueueLen
}

func (r *scanStandaloneReader) waitForScanQueueCapacity(dbId int) bool {
	high := r.getScanHighQueueLen()
	low := r.getScanLowQueueLen()
	if high <= 0 {
		return true
	}
	if low > high {
		low = high
	}

	if !r.scanPaused.Load() && r.getQueueLen() >= high {
		log.Warnf("[%s] scan backpressure activated. queue_len=[%d], scan_high_queue_len=[%d], scan_low_queue_len=[%d], db=[%d]",
			r.stat.Name, r.getQueueLen(), high, low, dbId)
		r.scanPaused.Store(true)
	}

	for r.scanPaused.Load() && r.getQueueLen() > low {
		select {
		case <-r.ctx.Done():
			return false
		case <-time.After(100 * time.Millisecond):
		}
	}
	if r.scanPaused.Load() {
		log.Warnf("[%s] scan backpressure released. queue_len=[%d], scan_high_queue_len=[%d], scan_low_queue_len=[%d], db=[%d]",
			r.stat.Name, r.getQueueLen(), high, low, dbId)
		r.scanPaused.Store(false)
	}
	return true
}

func (r *scanStandaloneReader) shouldDropKSNOnBackpressure() bool {
	if !r.opts.DropKSNOnBackpressure {
		return false
	}
	if r.getScanHighQueueLen() <= 0 {
		return false
	}
	return r.scanPaused.Load() || r.getQueueLen() >= r.getScanHighQueueLen()
}

func (r *scanStandaloneReader) logKSNDropState(active bool) {
	if active {
		if r.ksnDropLogged {
			return
		}
		log.Warnf("[%s] drop_ksn_on_backpressure activated. queue_len=[%d], scan_high_queue_len=[%d], scan_low_queue_len=[%d]",
			r.stat.Name, r.getQueueLen(), r.getScanHighQueueLen(), r.getScanLowQueueLen())
		r.ksnDropLogged = true
		return
	}
	if r.ksnDropLogged {
		log.Warnf("[%s] drop_ksn_on_backpressure released. queue_len=[%d], scan_high_queue_len=[%d], scan_low_queue_len=[%d]",
			r.stat.Name, r.getQueueLen(), r.getScanHighQueueLen(), r.getScanLowQueueLen())
		r.ksnDropLogged = false
	}
}

func (r *scanStandaloneReader) StartRead(ctx context.Context) []chan *entry.Entry {
	r.ctx = ctx
	r.logStartupSamples()
	go r.runSyncedKeyOPSTicker()
	if r.opts.KSN {
		r.subWG.Add(1)
		go r.subscribe()
		r.subWG.Wait()
	}
	if r.opts.Scan {
		go r.scan()
	}
	go r.dump()
	go r.restore()
	return []chan *entry.Entry{r.ch}
}

func (r *scanStandaloneReader) doSourceCommand(c *client.Redis, args ...interface{}) (interface{}, error) {
	if err := c.SendWithError(args...); err != nil {
		return nil, err
	}
	return c.Receive()
}

func (r *scanStandaloneReader) resolveScanDBs(c *client.Redis) []int {
	if len(r.dbs) > 0 {
		return r.dbs
	}
	reply, err := r.doSourceCommand(c, "info", "keyspace")
	if err != nil {
		log.Warnf("[%s] startup sampling skipped: load db list failed. err=[%v]", r.stat.Name, err)
		return nil
	}
	dbs := utils.ParseDBs(reply.(string))
	if len(dbs) == 0 {
		dbs = []int{0}
	}
	return dbs
}

func (r *scanStandaloneReader) updateEstimatedTotalKeys(info string, dbs []int) {
	counts := utils.ParseDBKeyCounts(info)
	var total int64
	for _, db := range dbs {
		total += counts[db]
	}
	r.stat.EstimatedTotalKeys = total
	r.updateSyncPercent()
}

func (r *scanStandaloneReader) updateSyncPercent() {
	total := r.stat.EstimatedTotalKeys
	if total <= 0 {
		if r.stat.ScannedKeyCount > 0 {
			total = r.stat.ScannedKeyCount + r.stat.NeedUpdateCount
		} else if r.stat.SyncedKeyCount+r.stat.NeedUpdateCount > 0 {
			total = r.stat.SyncedKeyCount + r.stat.NeedUpdateCount
		}
	}
	if total <= 0 {
		r.stat.SyncPercent = "0.00%"
		return
	}
	progress := float64(r.stat.SyncedKeyCount) / float64(total) * 100
	if progress > 100 {
		progress = 100
	}
	if progress < 0 {
		progress = 0
	}
	r.stat.SyncPercent = fmt.Sprintf("%.2f%%", progress)
}

func (r *scanStandaloneReader) logStartupSamples() {
	if !r.opts.SampleOnStart {
		return
	}
	sampleCount := r.opts.SampleCountPerDB
	if sampleCount <= 0 {
		sampleCount = 3
	}
	c := client.NewRedisClient(r.ctx, r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls, r.opts.TlsConfig, r.opts.PreferReplica)
	defer c.Close()

	dbs := r.resolveScanDBs(c)
	reply, err := r.doSourceCommand(c, "info", "keyspace")
	if err == nil {
		r.updateEstimatedTotalKeys(reply.(string), dbs)
	}
	for _, dbId := range dbs {
		r.logStartupSamplesForDB(c, dbId, sampleCount)
	}
}

func (r *scanStandaloneReader) logStartupSamplesForDB(c *client.Redis, dbId int, sampleCount int) {
	r.selectSourceDB(c, dbId)
	log.Infof("[%s] startup sampling begin. db=[%d], sample_count=[%d]", r.stat.Name, dbId, sampleCount)

	cursor := uint64(0)
	collected := 0
	for collected < sampleCount {
		newCursor, keys, err := c.ScanWithError(cursor, sampleCount)
		if err != nil {
			log.Warnf("[%s] startup sampling aborted. db=[%d], err=[%v]", r.stat.Name, dbId, err)
			return
		}
		cursor = newCursor
		for _, key := range keys {
			if err := r.logSampleKey(c, dbId, key); err != nil {
				log.Warnf("[%s] startup sampling key failed. db=[%d], key=[%s], err=[%v]", r.stat.Name, dbId, key, err)
				continue
			}
			collected++
			if collected >= sampleCount {
				break
			}
		}
		if cursor == 0 {
			break
		}
	}
	log.Infof("[%s] startup sampling finished. db=[%d], sampled=[%d]", r.stat.Name, dbId, collected)
}

func (r *scanStandaloneReader) logSampleKey(c *client.Redis, dbId int, key string) error {
	reply, err := r.doSourceCommand(c, "TYPE", key)
	if err != nil {
		return err
	}
	keyType, ok := reply.(string)
	if !ok {
		return errors.New("sample TYPE reply is not string")
	}
	preview, err := r.fetchSampleValuePreview(c, key, keyType)
	if err != nil {
		return err
	}
	log.Infof("[%s] startup sample. db=[%d], key=[%s], type=[%s], value_preview=[%s]",
		r.stat.Name, dbId, key, keyType, preview)
	return nil
}

func (r *scanStandaloneReader) fetchSampleValuePreview(c *client.Redis, key string, keyType string) (string, error) {
	const collectionSampleCount = 5
	var (
		reply interface{}
		err   error
	)
	switch strings.ToLower(keyType) {
	case "string":
		reply, err = r.doSourceCommand(c, "GET", key)
	case "hash":
		reply, err = r.doSourceCommand(c, "HGETALL", key)
	case "list":
		reply, err = r.doSourceCommand(c, "LRANGE", key, 0, collectionSampleCount-1)
	case "set":
		reply, err = r.doSourceCommand(c, "SSCAN", key, 0, "COUNT", collectionSampleCount)
	case "zset":
		reply, err = r.doSourceCommand(c, "ZRANGE", key, 0, collectionSampleCount-1, "WITHSCORES")
	case "stream":
		reply, err = r.doSourceCommand(c, "XRANGE", key, "-", "+", "COUNT", collectionSampleCount)
	default:
		return "unsupported-type-preview", nil
	}
	if err != nil {
		if errors.Is(err, proto.Nil) {
			return "nil", nil
		}
		return "", err
	}
	return truncateSampleValue(formatSampleReply(reply), r.opts.SampleValueMaxLen), nil
}

func formatSampleReply(reply interface{}) string {
	switch v := reply.(type) {
	case nil:
		return "nil"
	case string:
		return v
	case []byte:
		return string(v)
	case []interface{}:
		items := make([]string, 0, len(v))
		for _, item := range v {
			items = append(items, formatSampleReply(item))
		}
		return "[" + strings.Join(items, ", ") + "]"
	default:
		return fmt.Sprintf("%v", v)
	}
}

func truncateSampleValue(value string, maxLen int) string {
	if maxLen <= 0 {
		maxLen = 256
	}
	if len(value) <= maxLen {
		return value
	}
	if maxLen <= 3 {
		return value[:maxLen]
	}
	return value[:maxLen-3] + "..."
}

func (r *scanStandaloneReader) useCommandReadMode() bool {
	return strings.EqualFold(strings.TrimSpace(r.opts.ReadMode), "command")
}

func (r *scanStandaloneReader) shouldSkipSourceType(typeStr string) bool {
	for _, skipType := range r.opts.SkipUnknownType {
		if strings.EqualFold(typeStr, skipType) {
			return true
		}
	}
	return false
}

func (r *scanStandaloneReader) getRewriteBatchSize() int {
	if config.Opt.Advanced.RewriteCollectionBatchSize <= 0 {
		return 128
	}
	return config.Opt.Advanced.RewriteCollectionBatchSize
}

func replyToString(v interface{}) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	default:
		return "", fmt.Errorf("reply is not string, type=%T", v)
	}
}

func replyToStringSlice(v interface{}) ([]string, error) {
	items, ok := v.([]interface{})
	if !ok {
		return nil, fmt.Errorf("reply is not array, type=%T", v)
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		s, err := replyToString(item)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

func scanReplyToCursorAndItems(reply interface{}) (uint64, []string, error) {
	array, ok := reply.([]interface{})
	if !ok || len(array) != 2 {
		return 0, nil, fmt.Errorf("scan-like reply shape invalid: %T", reply)
	}
	cursorStr, err := replyToString(array[0])
	if err != nil {
		return 0, nil, err
	}
	cursor, err := strconv.ParseUint(cursorStr, 10, 64)
	if err != nil {
		return 0, nil, err
	}
	items, err := replyToStringSlice(array[1])
	if err != nil {
		return 0, nil, err
	}
	return cursor, items, nil
}

func (r *scanStandaloneReader) emitEntry(dbId int, argv ...string) {
	e := entry.NewEntry()
	e.DbId = dbId
	e.Argv = argv
	r.ch <- e
}

func (r *scanStandaloneReader) emitCommandBatches(dbId int, key string, cmd string, values []string) {
	if len(values) == 0 {
		return
	}
	batchSize := r.getRewriteBatchSize()
	for i := 0; i < len(values); i += batchSize {
		end := i + batchSize
		if end > len(values) {
			end = len(values)
		}
		argv := make([]string, 2, 2+(end-i))
		argv[0] = cmd
		argv[1] = key
		argv = append(argv, values[i:end]...)
		r.emitEntry(dbId, argv...)
	}
}

func (r *scanStandaloneReader) emitPairCommandBatches(dbId int, key string, cmd string, pairs []string) {
	if len(pairs) == 0 {
		return
	}
	batchSize := r.getRewriteBatchSize() * 2
	for i := 0; i < len(pairs); i += batchSize {
		end := i + batchSize
		if end > len(pairs) {
			end = len(pairs)
		}
		argv := make([]string, 2, 2+(end-i))
		argv[0] = cmd
		argv[1] = key
		argv = append(argv, pairs[i:end]...)
		r.emitEntry(dbId, argv...)
	}
}

func (r *scanStandaloneReader) reconnectSource(c *client.Redis, reason error) bool {
	if !config.Opt.Advanced.IOReconnect || !client.IsReconnectableIOError(reason) {
		return false
	}
	delay := time.Duration(config.Opt.Advanced.IOReconnectDelayMs) * time.Millisecond
	maxTimes := config.Opt.Advanced.IOReconnectMaxTimes
	if maxTimes <= 0 {
		maxTimes = 1
	}
	for attempt := 1; ; attempt++ {
		if r.ctx != nil {
			select {
			case <-r.ctx.Done():
				return false
			default:
			}
		}
		roundAttempt := (attempt-1)%maxTimes + 1
		log.Warnf("[%s] reconnecting source redis. attempt=[%d/%d], delay=[%s], error=[%v]",
			r.stat.Name, roundAttempt, maxTimes, delay, reason)
		if delay > 0 {
			if r.ctx != nil {
				select {
				case <-r.ctx.Done():
					return false
				case <-time.After(delay):
				}
			} else {
				time.Sleep(delay)
			}
		}
		reconnectFn := r.reconnectFn
		if reconnectFn == nil {
			reconnectFn = func(rc *client.Redis) error { return rc.Reconnect() }
		}
		if err := reconnectFn(c); err == nil {
			log.Warnf("[%s] reconnected source redis", r.stat.Name)
			return true
		}
		if roundAttempt == maxTimes {
			log.Warnf("[%s] source redis reconnect attempts exhausted. continuing to retry until context is canceled", r.stat.Name)
		}
	}
}

func (r *scanStandaloneReader) selectSourceDB(c *client.Redis, dbId int) {
	if dbId == 0 {
		return
	}
	for {
		reply, err := c.DoWithStringReplyWithError("SELECT", strconv.Itoa(dbId))
		if err == nil && reply == "OK" {
			return
		}
		reason := err
		if reason == nil {
			reason = errors.New("scanStandaloneReader select db failed")
		}
		if !r.reconnectSource(c, reason) {
			log.Panicf("scanStandaloneReader select db failed. db=[%d]", dbId)
		}
	}
}

func (r *scanStandaloneReader) scanKeys(c *client.Redis, dbId int, cursor uint64, count int) (uint64, []string) {
	for {
		newCursor, keys, err := c.ScanWithError(cursor, count)
		if err == nil {
			return newCursor, keys
		}
		if !r.reconnectSource(c, err) {
			log.Panicf(err.Error())
		}
		r.selectSourceDB(c, dbId)
	}
}

func (r *scanStandaloneReader) sendDumpSequence(dbId int, key string) {
	for {
		if r.dumpClient.SendWithError("SELECT", strconv.Itoa(dbId)) == nil &&
			r.dumpClient.SendWithError("DUMP", key) == nil &&
			r.dumpClient.SendWithError("PTTL", key) == nil {
			if len(r.opts.SkipUnknownType) == 0 || r.dumpClient.SendWithError("TYPE", key) == nil {
				return
			}
		}
		if !r.reconnectSource(r.dumpClient, errors.New("unexpected EOF")) {
			log.Panicf("scanStandaloneReader dump failed. db=[%d], key=[%s]", dbId, key)
		}
	}
}

func (r *scanStandaloneReader) resendDumpSequence(dbId int, key string) {
	r.sendDumpSequence(dbId, key)
}

func (r *scanStandaloneReader) doSourceCommandWithReconnect(c *client.Redis, dbId int, args ...interface{}) interface{} {
	for {
		reply, err := r.doSourceCommand(c, args...)
		if err == nil {
			return reply
		}
		if errors.Is(err, proto.Nil) {
			return nil
		}
		if !r.reconnectSource(c, err) {
			log.Panicf(err.Error())
		}
		r.selectSourceDB(c, dbId)
	}
}

func (r *scanStandaloneReader) fetchSourceType(c *client.Redis, dbId int, key string) string {
	reply := r.doSourceCommandWithReconnect(c, dbId, "TYPE", key)
	typeStr, err := replyToString(reply)
	if err != nil {
		log.Panicf("type reply parse failed. key=[%s], err=[%v]", key, err)
	}
	return strings.ToLower(typeStr)
}

func (r *scanStandaloneReader) fetchSourcePTTL(c *client.Redis, dbId int, key string) int {
	reply := r.doSourceCommandWithReconnect(c, dbId, "PTTL", key)
	switch v := reply.(type) {
	case int64:
		pttl := int(v)
		if pttl == 0 {
			return 1
		}
		if pttl == -1 {
			return 0
		}
		return pttl
	case string:
		log.Panicf("pttl reply is string, key=[%s], pttl=[%s]", key, v)
	default:
		log.Panicf("unexpected pttl reply type. key=[%s], type=%T", key, reply)
	}
	return 0
}

func (r *scanStandaloneReader) rewriteKeyByCommand(dbId int, key string) bool {
	c := r.dumpClient
	keyType := r.fetchSourceType(c, dbId, key)
	if keyType == "none" {
		return false
	}
	if r.shouldSkipSourceType(keyType) {
		log.Infof("skip restore key=[%s] type=[%s]", key, keyType)
		return false
	}

	pttl := r.fetchSourcePTTL(c, dbId, key)
	if pttl == -2 {
		return false
	}

	switch keyType {
	case "string":
		reply := r.doSourceCommandWithReconnect(c, dbId, "GET", key)
		if reply == nil {
			return false
		}
		value, err := replyToString(reply)
		if err != nil {
			log.Panicf("get reply parse failed. key=[%s], err=[%v]", key, err)
		}
		r.emitEntry(dbId, "SET", key, value)
	case "hash":
		r.emitEntry(dbId, "DEL", key)
		cursor := uint64(0)
		for {
			reply := r.doSourceCommandWithReconnect(c, dbId, "HSCAN", key, strconv.FormatUint(cursor, 10), "COUNT", r.getRewriteBatchSize())
			nextCursor, items, err := scanReplyToCursorAndItems(reply)
			if err != nil {
				log.Panicf("hscan reply parse failed. key=[%s], err=[%v]", key, err)
			}
			r.emitPairCommandBatches(dbId, key, "HSET", items)
			cursor = nextCursor
			if cursor == 0 {
				break
			}
		}
	case "list":
		reply := r.doSourceCommandWithReconnect(c, dbId, "LLEN", key)
		listLen, ok := reply.(int64)
		if !ok {
			log.Panicf("llen reply type invalid. key=[%s], type=%T", key, reply)
		}
		r.emitEntry(dbId, "DEL", key)
		batchSize := r.getRewriteBatchSize()
		for start := 0; start < int(listLen); start += batchSize {
			stop := start + batchSize - 1
			reply := r.doSourceCommandWithReconnect(c, dbId, "LRANGE", key, start, stop)
			items, err := replyToStringSlice(reply)
			if err != nil {
				log.Panicf("lrange reply parse failed. key=[%s], start=[%d], err=[%v]", key, start, err)
			}
			r.emitCommandBatches(dbId, key, "RPUSH", items)
		}
	case "set":
		r.emitEntry(dbId, "DEL", key)
		cursor := uint64(0)
		for {
			reply := r.doSourceCommandWithReconnect(c, dbId, "SSCAN", key, strconv.FormatUint(cursor, 10), "COUNT", r.getRewriteBatchSize())
			nextCursor, items, err := scanReplyToCursorAndItems(reply)
			if err != nil {
				log.Panicf("sscan reply parse failed. key=[%s], err=[%v]", key, err)
			}
			r.emitCommandBatches(dbId, key, "SADD", items)
			cursor = nextCursor
			if cursor == 0 {
				break
			}
		}
	case "zset":
		r.emitEntry(dbId, "DEL", key)
		cursor := uint64(0)
		for {
			reply := r.doSourceCommandWithReconnect(c, dbId, "ZSCAN", key, strconv.FormatUint(cursor, 10), "COUNT", r.getRewriteBatchSize())
			nextCursor, items, err := scanReplyToCursorAndItems(reply)
			if err != nil {
				log.Panicf("zscan reply parse failed. key=[%s], err=[%v]", key, err)
			}
			r.emitPairCommandBatches(dbId, key, "ZADD", items)
			cursor = nextCursor
			if cursor == 0 {
				break
			}
		}
	default:
		log.Panicf("scan_reader command read mode does not support type=[%s], key=[%s]. use read_mode=dump or skip_unknown_type", keyType, key)
	}

	if pttl != 0 {
		r.emitEntry(dbId, "PEXPIRE", key, strconv.Itoa(pttl))
	}
	return true
}

func (r *scanStandaloneReader) subscribe() {
	c := client.NewRedisClient(r.ctx, r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls, r.opts.TlsConfig, r.opts.PreferReplica)
	log.Infof("[%s] scanStandaloneReader subscribe started. dbs=[%v]", r.stat.Name, r.dbs)
	subscribe := func() {
		for {
			if len(r.dbs) == 0 {
				if err := c.SendWithError("psubscribe", "__keyevent@*__:*"); err == nil {
					if _, recvErr := c.Receive(); recvErr == nil {
						return
					} else if r.reconnectSource(c, recvErr) {
						continue
					} else {
						log.Panicf(recvErr.Error())
					}
				} else if r.reconnectSource(c, err) {
					continue
				} else {
					log.Panicf(err.Error())
				}
			}
			args := []interface{}{"psubscribe"}
			for _, db := range r.dbs {
				args = append(args, fmt.Sprintf("__keyevent@%v__:*", db))
			}
			if err := c.SendWithError(args...); err != nil {
				if r.reconnectSource(c, err) {
					continue
				}
				log.Panicf(err.Error())
			}
			resubscribed := true
			for range r.dbs {
				if _, err := c.Receive(); err != nil {
					if r.reconnectSource(c, err) {
						resubscribed = false
						break
					}
					log.Panicf(err.Error())
				}
			}
			if resubscribed {
				return
			}
		}
	}
	subscribe()

	// wait
	r.subWG.Done()

	regex := regexp.MustCompile(`\d+`)
	for {
		select {
		case <-r.ctx.Done():
			log.Infof("[%s] scanStandaloneReader subscribe finished.", r.stat.Name)
			r.needDumpQueue.Close()
			return
		default:
			resp, err := c.Receive()
			if err != nil {
				if r.reconnectSource(c, err) {
					subscribe()
					continue
				}
				log.Panicf(err.Error())
			}
			respSlice := resp.([]interface{})
			key := respSlice[3].(string)
			dbId := regex.FindString(respSlice[2].(string))
			dbIdInt, err := strconv.Atoi(dbId)
			if err != nil {
				log.Panicf(err.Error())
			}
			// handle del action
			eventSlice := strings.Split(respSlice[2].(string), ":")
			if eventSlice[1] == "del" {
				e := entry.NewEntry()
				e.DbId = dbIdInt
				e.Argv = []string{"DEL", key}
				r.ch <- e
				continue
			}
			dropKSN := r.shouldDropKSNOnBackpressure()
			r.logKSNDropState(dropKSN)
			if dropKSN {
				r.stat.KSNDroppedCount++
				continue
			}
			r.needDumpQueue.Put(dbKey{db: dbIdInt, key: key})
			r.stat.NeedUpdateCount = int64(r.needDumpQueue.Len())
			r.updateSyncPercent()
		}
	}
}

func (r *scanStandaloneReader) scan() {
	c := client.NewRedisClient(r.ctx, r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls, r.opts.TlsConfig, r.opts.PreferReplica)
	defer c.Close()
	dbs := r.dbs
	if len(r.dbs) == 0 {
		for {
			info, err := r.doSourceCommand(c, "info", "keyspace")
			if err == nil {
				dbs = utils.ParseDBs(info.(string))
				r.updateEstimatedTotalKeys(info.(string), dbs)
				break
			}
			if !r.reconnectSource(c, err) {
				log.Panicf(err.Error())
			}
		}
	}
	for _, dbId := range dbs {
		r.selectSourceDB(c, dbId)

		var cursor uint64 = 0
		count := r.opts.Count
		for {
			select {
			case <-r.ctx.Done():
				log.Infof("[%s] scanStandaloneReader scan finished.", r.stat.Name)
				r.needDumpQueue.Close()
				return
			default:
			}
			if !r.waitForScanQueueCapacity(dbId) {
				log.Infof("[%s] scanStandaloneReader scan finished.", r.stat.Name)
				r.needDumpQueue.Close()
				return
			}

			var keys []string
			cursor, keys = r.scanKeys(c, dbId, cursor, count)
			for _, key := range keys {
				r.needDumpQueue.Put(dbKey{dbId, key}) // pass value not pointer
				r.stat.ScannedKeyCount++
			}

			// stat
			r.stat.ScanCursor = cursor
			r.stat.ScanDbId = dbId
			r.stat.NeedUpdateCount = int64(r.needDumpQueue.Len())
			r.updateSyncPercent()

			if cursor == 0 {
				break
			}
		}
	}
	r.stat.ScanFinished = true
	if !r.opts.KSN {
		r.needDumpQueue.Close()
	}
}

func (r *scanStandaloneReader) dump() {
	nowDbId := 0
	r.dumpClient = client.NewRedisClient(r.ctx, r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls, r.opts.TlsConfig, r.opts.PreferReplica)
	r.isValkey = r.dumpClient.IsValkey()
	log.Infof("[%s] detected server type: %s", r.stat.Name, map[bool]string{true: "Valkey", false: "Redis"}[r.isValkey])
	// Support prefer_replica=true in both Cluster and Standalone mode
	if r.opts.PreferReplica {
		r.dumpClient.Do("READONLY")
		log.Infof("running dump() in read-only mode")
	}

	for item := range r.needDumpQueue.Ch {
		r.stat.NeedUpdateCount = int64(r.needDumpQueue.Len())
		r.updateSyncPercent()
		dbId := item.(dbKey).db
		key := item.(dbKey).key
		if nowDbId != dbId {
			nowDbId = dbId
		}
		if !r.useCommandReadMode() {
			r.sendDumpSequence(dbId, key)
		}
		r.needRestoreChan <- &needRestoreItem{dbId, key}
	}
	close(r.needRestoreChan)
	log.Infof("[%s] scanStandaloneReader dump finished.", r.stat.Name)
}

// restore sends RESTORE commands to the target Redis.
// Note: rdb_restore_command_behavior configuration only applies when RESTORE command is used.
// For large values exceeding target_redis_proto_max_bulk_len, individual commands (SET, HSET, etc.)
// are used instead, which may not respect the rdb_restore_command_behavior setting.
func (r *scanStandaloneReader) restore() {
	nowDbId := 0
	for item := range r.needRestoreChan {
		dbId := item.dbId
		key := item.key
		if nowDbId != dbId {
			nowDbId = dbId
			if r.useCommandReadMode() {
				r.selectSourceDB(r.dumpClient, dbId)
			}
		}
		if r.useCommandReadMode() {
			if r.rewriteKeyByCommand(dbId, key) {
				r.stat.SyncedKeyCount++
				r.stat.NeedUpdateCount = int64(r.needDumpQueue.Len())
				r.updateSyncPercent()
			}
			continue
		}
	retryReceive:
		reply, err := r.dumpClient.Receive()
		if err != nil || reply != "OK" {
			reason := err
			if reason == nil {
				reason = errors.New("scanStandaloneReader select db failed")
			}
			if r.reconnectSource(r.dumpClient, reason) {
				r.resendDumpSequence(dbId, key)
				goto retryReceive
			}
			log.Panicf("scanStandaloneReader select db failed. db=[%d]", dbId)
		}
		iDump, err1 := r.dumpClient.Receive()
		iPttl, err2 := r.dumpClient.Receive()
		if len(r.opts.SkipUnknownType) > 0 {
			iType, err3 := r.dumpClient.Receive()
			if err3 != nil {
				if r.reconnectSource(r.dumpClient, err3) {
					r.resendDumpSequence(dbId, key)
					goto retryReceive
				}
				log.Panicf(err3.Error())
			}
			typeStr := iType.(string)
			if r.shouldSkipSourceType(typeStr) {
				log.Infof("skip restore key=[%s] type=[%s]", key, typeStr)
				continue
			}
		}
		if errors.Is(err1, proto.Nil) {
			continue // key not exist
		} else if err1 != nil {
			if r.reconnectSource(r.dumpClient, err1) {
				r.resendDumpSequence(dbId, key)
				goto retryReceive
			}
			log.Panicf(err1.Error())
		} else if err2 != nil {
			if r.reconnectSource(r.dumpClient, err2) {
				r.resendDumpSequence(dbId, key)
				goto retryReceive
			}
			log.Panicf(err2.Error())
		}
		dump := iDump.(string)
		pttl := 0
		switch v := iPttl.(type) {
		case int64:
			pttl = int(v)
			if pttl == 0 {
				pttl = 1
			}
		case string:
			log.Panicf("iPttl is string, this should not happen. key=[%s], pttl=[%s]", key, v)
		default:
			log.Panicf("unexpected type for pttl: %T", iPttl)
		}

		if pttl == -2 {
			continue // key not exist
		}
		if pttl == -1 {
			pttl = 0 // -1 means no expire
		}
		if uint64(len(dump)) > config.Opt.Advanced.TargetRedisProtoMaxBulkLen {
			typeByte := dump[0]
			anotherReader := strings.NewReader(dump[1 : len(dump)-10])
			o := types.ParseObject(anotherReader, typeByte, key, r.isValkey)
			cmdC := o.Rewrite()
			for cmd := range cmdC {
				e := entry.NewEntry()
				e.DbId = dbId
				e.Argv = cmd
				r.ch <- e
			}
			if pttl != 0 {
				e := entry.NewEntry()
				e.DbId = dbId
				e.Argv = []string{"PEXPIRE", key, strconv.Itoa(pttl)}
				r.ch <- e
			}
			r.stat.SyncedKeyCount++
			r.stat.NeedUpdateCount = int64(r.needDumpQueue.Len())
			r.updateSyncPercent()
		} else {
			argv := []string{"RESTORE", key, strconv.Itoa(pttl), dump}
			if config.Opt.Advanced.RDBRestoreCommandBehavior == "rewrite" {
				argv = append(argv, "replace")
			}
			r.ch <- &entry.Entry{
				DbId: dbId,
				Argv: argv,
			}
			r.stat.SyncedKeyCount++
			r.stat.NeedUpdateCount = int64(r.needDumpQueue.Len())
			r.updateSyncPercent()
		}
	}
	log.Infof("[%s] scanStandaloneReader restore finished.", r.stat.Name)
	close(r.ch)
}

func (r *scanStandaloneReader) Status() interface{} {
	return r.stat
}

func (r *scanStandaloneReader) StatusString() string {
	if !r.opts.Scan || r.stat.ScanFinished {
		return fmt.Sprintf("queue_len=[%d], scan_paused=[%t], synced_key_count=[%d], synced_key_ops=[%.2f], need_update_count=[%d]",
			r.getQueueLen(), r.scanPaused.Load(), r.stat.SyncedKeyCount, r.stat.SyncedKeyOps, r.stat.NeedUpdateCount)
	}
	return fmt.Sprintf("scan_dbid=[%d], sync_percent=[%s], queue_len=[%d], scan_paused=[%t], synced_key_count=[%d], synced_key_ops=[%.2f], need_update_count=[%d]",
		r.stat.ScanDbId, r.stat.SyncPercent, r.getQueueLen(), r.scanPaused.Load(), r.stat.SyncedKeyCount, r.stat.SyncedKeyOps, r.stat.NeedUpdateCount)
}

func (r *scanStandaloneReader) StatusConsistent() bool {
	return r.stat.ScanFinished && r.stat.NeedUpdateCount == 0
}

func (r *scanStandaloneReader) updateSyncedKeyOPS() {
	nowTS := float64(time.Now().UnixNano()) / 1e9
	if r.stat.lastSyncedKeyUpdateTSSecond != 0 {
		intervalSec := nowTS - r.stat.lastSyncedKeyUpdateTSSecond
		if intervalSec > 0 {
			r.stat.SyncedKeyOps = float64(r.stat.SyncedKeyCount-r.stat.lastSyncedKeyCount) / intervalSec
		}
	}
	r.stat.lastSyncedKeyCount = r.stat.SyncedKeyCount
	r.stat.lastSyncedKeyUpdateTSSecond = nowTS
}

func (r *scanStandaloneReader) runSyncedKeyOPSTicker() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.updateSyncedKeyOPS()
		}
	}
}
