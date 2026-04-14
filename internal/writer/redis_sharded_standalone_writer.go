package writer

import (
	"context"
	"fmt"
	"hash/fnv"
	"runtime"
	"sync"

	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
)

type redisShardedStandaloneWriter struct {
	shardCount int
	shards     []*redisStandaloneWriter
	ch         chan *entry.Entry
	wg         sync.WaitGroup
	stat       struct {
		Name string `json:"name"`
	}
}

func resolveStandaloneWriterShards() int {
	if config.Opt.Advanced.TargetRedisWriterShards > 0 {
		return config.Opt.Advanced.TargetRedisWriterShards
	}
	workers := runtime.GOMAXPROCS(0)
	if config.Opt.Advanced.Ncpu > 0 {
		workers = config.Opt.Advanced.Ncpu
	}
	if workers <= 1 {
		return 1
	}

	maxByPipeline := int(config.Opt.Advanced.PipelineCountLimit / 256)
	if maxByPipeline < 1 {
		maxByPipeline = 1
	}
	maxByQPS := config.Opt.Advanced.TargetRedisMaxQPS / 50000
	if maxByQPS < 1 {
		maxByQPS = 1
	}

	shards := workers
	if shards > maxByPipeline {
		shards = maxByPipeline
	}
	if shards > maxByQPS {
		shards = maxByQPS
	}
	if shards < 1 {
		shards = 1
	}
	if shards > 16 {
		shards = 16
	}
	return shards
}

func newRedisShardedStandaloneWriter(ctx context.Context, opts *RedisWriterOptions, shards int) Writer {
	if shards <= 1 {
		return newRedisStandaloneWriterWithLimits(ctx, opts, config.Opt.Advanced.TargetRedisMaxQPS, config.Opt.Advanced.PipelineCountLimit)
	}
	w := &redisShardedStandaloneWriter{
		shardCount: shards,
		ch:         make(chan *entry.Entry, int(config.Opt.Advanced.PipelineCountLimit)),
	}
	w.stat.Name = "writer_sharded_" + opts.Address

	totalQPS := config.Opt.Advanced.TargetRedisMaxQPS
	totalPipe := config.Opt.Advanced.PipelineCountLimit
	perQPS := (totalQPS + shards - 1) / shards
	perPipe := totalPipe / uint64(shards)
	if perPipe < 64 {
		perPipe = 64
	}
	for i := 0; i < shards; i++ {
		shardWriter := newRedisStandaloneWriterWithLimits(ctx, opts, perQPS, perPipe)
		shardWriter.stat.Name = fmt.Sprintf("%s_shard_%d", shardWriter.stat.Name, i)
		w.shards = append(w.shards, shardWriter)
	}
	log.Infof("redis writer sharding enabled. cluster=[%v], address=[%s], shards=[%d], per_shard_qps=[%d], per_shard_pipeline=[%d]",
		opts.Cluster, opts.Address, shards, perQPS, perPipe)
	return w
}

func (w *redisShardedStandaloneWriter) StartWrite(ctx context.Context) chan *entry.Entry {
	for _, shard := range w.shards {
		shard.StartWrite(ctx)
	}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for e := range w.ch {
			idx := w.route(e)
			w.shards[idx].Write(e)
		}
	}()
	return w.ch
}

func (w *redisShardedStandaloneWriter) Write(e *entry.Entry) {
	w.ch <- e
}

func (w *redisShardedStandaloneWriter) route(e *entry.Entry) int {
	if e == nil || w.shardCount <= 1 {
		return 0
	}
	if len(e.Slots) > 0 {
		slot := e.Slots[0]
		if slot < 0 {
			slot = -slot
		}
		return slot % w.shardCount
	}
	if len(e.Keys) == 0 && len(e.Argv) > 0 {
		e.Parse()
	}
	if len(e.Keys) == 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(e.Keys[0]))
	return int(h.Sum32() % uint32(w.shardCount))
}

func (w *redisShardedStandaloneWriter) Close() {
	close(w.ch)
	w.wg.Wait()
	for _, shard := range w.shards {
		shard.Close()
	}
}

func (w *redisShardedStandaloneWriter) Status() interface{} {
	stats := make([]interface{}, 0, len(w.shards)+1)
	stats = append(stats, map[string]interface{}{
		"name":   w.stat.Name,
		"shards": w.shardCount,
	})
	for _, shard := range w.shards {
		stats = append(stats, shard.Status())
	}
	return stats
}

func (w *redisShardedStandaloneWriter) StatusString() string {
	return fmt.Sprintf("[%s]: shards=%d", w.stat.Name, w.shardCount)
}

func (w *redisShardedStandaloneWriter) StatusConsistent() bool {
	for _, shard := range w.shards {
		if !shard.StatusConsistent() {
			return false
		}
	}
	return true
}
