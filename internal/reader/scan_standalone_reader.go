package reader

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"regexp"
	"strconv"
	"strings"
	"sync"
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
	scanPaused      bool

	stat struct {
		Name              string  `json:"name"`
		ScanFinished      bool    `json:"scan_finished"`
		ScanDbId          int     `json:"scan_dbId"`
		ScanCursor        uint64  `json:"scan_cursor"`
		ScanPercentByDbId string  `json:"scan_percent"`
		SyncedKeyCount    int64   `json:"synced_key_count"`
		SyncedKeyOps      float64 `json:"synced_key_ops"`
		NeedUpdateCount   int64   `json:"need_update_count"`
		KSNDroppedCount   int64   `json:"ksn_dropped_count"`

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

	if !r.scanPaused && r.getQueueLen() >= high {
		log.Warnf("[%s] scan backpressure activated. queue_len=[%d], scan_high_queue_len=[%d], scan_low_queue_len=[%d], db=[%d]",
			r.stat.Name, r.getQueueLen(), high, low, dbId)
		r.scanPaused = true
	}

	for r.scanPaused && r.getQueueLen() > low {
		select {
		case <-r.ctx.Done():
			return false
		case <-time.After(100 * time.Millisecond):
		}
	}
	if r.scanPaused {
		log.Warnf("[%s] scan backpressure released. queue_len=[%d], scan_high_queue_len=[%d], scan_low_queue_len=[%d], db=[%d]",
			r.stat.Name, r.getQueueLen(), high, low, dbId)
		r.scanPaused = false
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
	return r.scanPaused || r.getQueueLen() >= r.getScanHighQueueLen()
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

func (r *scanStandaloneReader) reconnectSource(c *client.Redis, reason error) bool {
	if !config.Opt.Advanced.IOReconnect || !client.IsReconnectableIOError(reason) {
		return false
	}
	delay := time.Duration(config.Opt.Advanced.IOReconnectDelayMs) * time.Millisecond
	for attempt := 1; attempt <= config.Opt.Advanced.IOReconnectMaxTimes; attempt++ {
		log.Warnf("[%s] reconnecting source redis. attempt=[%d/%d], delay=[%s], error=[%v]",
			r.stat.Name, attempt, config.Opt.Advanced.IOReconnectMaxTimes, delay, reason)
		time.Sleep(delay)
		reconnectFn := r.reconnectFn
		if reconnectFn == nil {
			reconnectFn = func(rc *client.Redis) error { return rc.Reconnect() }
		}
		if err := reconnectFn(c); err == nil {
			log.Warnf("[%s] reconnected source redis", r.stat.Name)
			return true
		}
	}
	return false
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

func (r *scanStandaloneReader) subscribe() {
	c := client.NewRedisClient(r.ctx, r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls, r.opts.TlsConfig, r.opts.PreferReplica)
	log.Infof("[%s] scanStandaloneReader subscribe started. dbs=[%v]", r.stat.Name, r.dbs)
	subscribe := func() {
		if len(r.dbs) == 0 {
			c.Send("psubscribe", "__keyevent@*__:*")
			_, err := c.Receive()
			if err != nil {
				log.Panicf(err.Error())
			}
			return
		}
		args := []interface{}{"psubscribe"}
		for _, db := range r.dbs {
			args = append(args, fmt.Sprintf("__keyevent@%v__:*", db))
		}
		c.Send(args...)
		for range r.dbs {
			_, err := c.Receive()
			if err != nil {
				log.Panicf(err.Error())
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
		}
	}
}

func (r *scanStandaloneReader) scan() {
	c := client.NewRedisClient(r.ctx, r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls, r.opts.TlsConfig, r.opts.PreferReplica)
	defer c.Close()
	dbs := r.dbs
	if len(r.dbs) == 0 {
		c.Send("info", "keyspace")
		info, err := c.Receive()
		if err != nil {
			log.Panicf(err.Error())
		}
		dbs = utils.ParseDBs(info.(string))
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
			}

			// stat
			r.stat.ScanCursor = cursor
			r.stat.ScanDbId = dbId
			r.stat.ScanPercentByDbId = fmt.Sprintf("%.2f%%", float64(bits.Reverse64(cursor))/float64(^uint(0))*100)

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
		dbId := item.(dbKey).db
		key := item.(dbKey).key
		if nowDbId != dbId {
			nowDbId = dbId
		}
		r.sendDumpSequence(dbId, key)
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
			// type in SkipUnknownType
			skip := false
			for _, skipType := range r.opts.SkipUnknownType {
				if strings.EqualFold(typeStr, skipType) {
					skip = true
				}
			}
			if skip {
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
		return fmt.Sprintf("synced_key_count=[%d], synced_key_ops=[%.2f], need_update_count=[%d]",
			r.stat.SyncedKeyCount, r.stat.SyncedKeyOps, r.stat.NeedUpdateCount)
	}
	return fmt.Sprintf("scan_dbid=[%d], scan_percent=[%s], synced_key_count=[%d], synced_key_ops=[%.2f], need_update_count=[%d]",
		r.stat.ScanDbId, r.stat.ScanPercentByDbId, r.stat.SyncedKeyCount, r.stat.SyncedKeyOps, r.stat.NeedUpdateCount)
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
