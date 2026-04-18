package writer

import (
	"context"
	"fmt"
	"sync"

	"RedisShake/internal/client"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"RedisShake/internal/utils"
)

const KeySlots = 16384

type RedisClusterWriter struct {
	ctx       context.Context
	opts      RedisWriterOptions
	addresses []string
	writers   []clusterManagedWriter
	router    [KeySlots]clusterManagedWriter
	ch        chan *entry.Entry
	chWg      sync.WaitGroup
	stat      []interface{}
	mu        sync.RWMutex

	getClusterNodes func(ctx context.Context, address string, username string, password string, tls bool, tlsConfig client.TlsConfig, preferReplica bool) ([]string, [][]int)
	newWriter       func(ctx context.Context, opts *RedisWriterOptions) clusterManagedWriter
}

func NewRedisClusterWriter(ctx context.Context, opts *RedisWriterOptions) Writer {
	rw := &RedisClusterWriter{
		ctx:             ctx,
		opts:            *opts,
		getClusterNodes: utils.GetRedisClusterNodes,
		newWriter: func(ctx context.Context, opts *RedisWriterOptions) clusterManagedWriter {
			return NewRedisStandaloneWriter(ctx, opts).(clusterManagedWriter)
		},
	}
	rw.loadClusterNodes()
	rw.ch = make(chan *entry.Entry, 1024)
	log.Infof("redisClusterWriter connected to redis cluster successful. addresses=%v", rw.addresses)
	return rw
}

func (r *RedisClusterWriter) Close() {
	r.chWg.Wait()
	close(r.ch)
	for _, writer := range r.writers {
		writer.Close()
	}
}

func (r *RedisClusterWriter) loadClusterNodes() {
	addresses, slots := r.getClusterNodes(r.ctx, r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls, r.opts.TlsConfig, false)
	r.addresses = addresses
	r.writers = nil
	for i, address := range addresses {
		theOpts := r.opts
		theOpts.Address = address
		redisWriter := r.newWriter(r.ctx, &theOpts)
		r.bindReconnect(redisWriter)
		r.writers = append(r.writers, redisWriter)
		for _, s := range slots[i] {
			if r.router[s] != nil {
				log.Panicf("redisClusterWriter: slot %d already occupied", s)
			}
			r.router[s] = redisWriter
		}
	}

	for i := 0; i < KeySlots; i++ {
		if r.router[i] == nil {
			log.Panicf("redisClusterWriter: slot %d not occupied", i)
		}
	}
}

func (r *RedisClusterWriter) bindReconnect(w clusterManagedWriter) {
	w.SetReconnectFn(func() error {
		return r.refreshTopologyAndReconnect(w)
	})
}

func (r *RedisClusterWriter) refreshTopologyAndReconnect(failedWriter clusterManagedWriter) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	addresses, slots := r.getClusterNodes(r.ctx, r.opts.Address, r.opts.Username, r.opts.Password, r.opts.Tls, r.opts.TlsConfig, false)
	var newRouter [KeySlots]clusterManagedWriter
	newWriters := make([]clusterManagedWriter, 0, len(addresses))
	used := make(map[clusterManagedWriter]bool, len(r.writers))

	for i, slotGroup := range slots {
		if len(slotGroup) == 0 {
			return fmt.Errorf("empty slot group for refreshed cluster node address=%s", addresses[i])
		}
		owner := r.router[slotGroup[0]]
		if owner == nil {
			return fmt.Errorf("cannot map refreshed cluster node address=%s slot=%d to existing writer", addresses[i], slotGroup[0])
		}
		for _, slot := range slotGroup[1:] {
			if r.router[slot] != owner {
				return fmt.Errorf("cluster topology changed beyond failover for refreshed address=%s slot=%d", addresses[i], slot)
			}
		}
		if used[owner] {
			return fmt.Errorf("refreshed cluster topology maps multiple masters to writer address=%s", owner.Address())
		}
		used[owner] = true

		nextOpts := r.opts
		nextOpts.Address = addresses[i]
		shouldReconnect := owner == failedWriter || owner.Address() != addresses[i]
		if shouldReconnect {
			if err := owner.UpdateTarget(r.ctx, &nextOpts); err != nil {
				return err
			}
			r.bindReconnect(owner)
		}

		newWriters = append(newWriters, owner)
		for _, slot := range slotGroup {
			if newRouter[slot] != nil {
				return fmt.Errorf("duplicate slot %d in refreshed topology", slot)
			}
			newRouter[slot] = owner
		}
	}

	for slot := 0; slot < KeySlots; slot++ {
		if newRouter[slot] == nil {
			return fmt.Errorf("slot %d missing after topology refresh", slot)
		}
	}

	r.addresses = addresses
	r.writers = newWriters
	r.router = newRouter
	log.Warnf("redis cluster topology refreshed. addresses=%v", addresses)
	return nil
}

func (r *RedisClusterWriter) StartWrite(ctx context.Context) chan *entry.Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, w := range r.writers {
		w.StartWrite(ctx)
	}
	return nil
}

func (r *RedisClusterWriter) Write(entry *entry.Entry) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(entry.Slots) == 0 {
		for _, writer := range r.writers {
			writer.Write(entry)
		}
		return
	}
	lastSlot := -1
	for _, slot := range entry.Slots {
		if lastSlot == -1 {
			lastSlot = slot
		}
		if slot != lastSlot {
			log.Panicf("CROSSSLOT Keys in request don't hash to the same slot. argv=%v", entry.Argv)
		}
	}
	r.router[lastSlot].Write(entry)
}

func (r *RedisClusterWriter) Consistent() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, writer := range r.writers {
		if !writer.StatusConsistent() {
			return false
		}
	}
	return true
}

func (r *RedisClusterWriter) Status() interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.stat = make([]interface{}, 0)
	for _, writer := range r.writers {
		r.stat = append(r.stat, writer.Status())
	}
	return r.stat
}

func (r *RedisClusterWriter) StatusString() string {
	return "[redis_cluster_writer] writing to redis cluster"
}

func (r *RedisClusterWriter) StatusConsistent() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, writer := range r.writers {
		if !writer.StatusConsistent() {
			return false
		}
	}
	return true
}
