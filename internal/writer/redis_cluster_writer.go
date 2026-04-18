package writer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"RedisShake/internal/client"
	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"RedisShake/internal/utils"
)

const KeySlots = 16384

type RedisClusterWriter struct {
	ctx        context.Context
	opts       RedisWriterOptions
	addresses  []string
	knownNodes []string
	writers    []clusterManagedWriter
	router     [KeySlots]clusterManagedWriter
	ch         chan *entry.Entry
	chWg       sync.WaitGroup
	stat       []interface{}
	mu         sync.RWMutex

	getClusterNodes           func(ctx context.Context, address string, username string, password string, tls bool, tlsConfig client.TlsConfig, preferReplica bool) ([]string, [][]int)
	getClusterSnapshot        func(ctx context.Context, address string, username string, password string, tls bool, tlsConfig client.TlsConfig, preferReplica bool) (utils.ClusterNodesSnapshot, error)
	getClusterSnapshotFromAny func(ctx context.Context, addresses []string, username string, password string, tls bool, tlsConfig client.TlsConfig, preferReplica bool) utils.ClusterNodesSnapshot
	newWriter                 func(ctx context.Context, opts *RedisWriterOptions) clusterManagedWriter
}

func NewRedisClusterWriter(ctx context.Context, opts *RedisWriterOptions) Writer {
	rw := &RedisClusterWriter{
		ctx:                       ctx,
		opts:                      *opts,
		getClusterNodes:           utils.GetRedisClusterNodes,
		getClusterSnapshot:        utils.GetRedisClusterSnapshotWithError,
		getClusterSnapshotFromAny: utils.GetRedisClusterSnapshotFromAnyAddress,
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
	log.Warnf("redisClusterWriter loading initial cluster topology. config_address=[%s]", r.opts.Address)
	snapshot := r.getClusterSnapshotFromAny(r.ctx, []string{r.opts.Address}, r.opts.Username, r.opts.Password, r.opts.Tls, r.opts.TlsConfig, false)
	addresses, slots := snapshot.Addresses, snapshot.Slots
	r.addresses = addresses
	r.knownNodes = dedupeClusterAddresses(snapshot.AllNodes)
	log.Warnf("redisClusterWriter initial topology loaded. masters=%v, all_nodes=%v, known_nodes=%v", addresses, snapshot.AllNodes, r.knownNodes)
	r.writers = nil
	for i, address := range addresses {
		slotCount := len(slots[i])
		slotFirst, slotLast := -1, -1
		if slotCount > 0 {
			slotFirst, slotLast = slots[i][0], slots[i][slotCount-1]
		}
		log.Warnf("redisClusterWriter creating writer for master. address=[%s], slot_count=[%d], slot_range=[%d-%d]", address, slotCount, slotFirst, slotLast)
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

	currentAddresses := make([]string, 0, len(r.writers))
	for _, w := range r.writers {
		currentAddresses = append(currentAddresses, w.Address())
	}
	log.Warnf("cluster topology refresh triggered. failed_writer=[%s], current_writers=%v, known_nodes=%v, config_address=[%s]",
		failedWriter.Address(), currentAddresses, r.knownNodes, r.opts.Address)

	delay := time.Duration(config.Opt.Advanced.IOReconnectDelayMs) * time.Millisecond
	if delay <= 0 {
		delay = 100 * time.Millisecond
	}

	for {
		candidates := r.topologyCandidatesLocked(failedWriter)
		snapshot, snapshotErr := r.fetchLatestTopologyLocked(candidates)
		if snapshotErr != nil {
			select {
			case <-r.ctx.Done():
				return fmt.Errorf("cluster topology refresh canceled while waiting for metadata recovery: %w", snapshotErr)
			case <-time.After(delay):
			}
			continue
		}
		retryRefresh, applyErr := r.applyRefreshedTopologyLocked(snapshot, failedWriter)
		if applyErr == nil {
			return nil
		}
		if !retryRefresh {
			return applyErr
		}
		select {
		case <-r.ctx.Done():
			return fmt.Errorf("cluster topology refresh canceled while waiting for failed writer recovery: %w", applyErr)
		case <-time.After(delay):
		}
	}
}

func (r *RedisClusterWriter) fetchLatestTopologyLocked(candidates []string) (utils.ClusterNodesSnapshot, error) {
	log.Warnf("fetching cluster topology from candidates. candidates=%v", candidates)
	if len(candidates) == 0 {
		return utils.ClusterNodesSnapshot{}, fmt.Errorf("no topology candidates available")
	}

	type result struct {
		address  string
		snapshot utils.ClusterNodesSnapshot
		err      error
	}

	ctx, cancel := context.WithCancel(r.ctx)
	defer cancel()

	results := make(chan result, len(candidates))
	for _, address := range candidates {
		address := address
		go func() {
			snapshot, err := r.getClusterSnapshot(ctx, address, r.opts.Username, r.opts.Password, r.opts.Tls, r.opts.TlsConfig, false)
			if err != nil {
				log.Warnf("cluster topology candidate failed. address=[%s], err=[%v]", address, err)
			} else {
				log.Warnf("cluster topology candidate succeeded. address=[%s], masters=%v, all_nodes=%v", address, snapshot.Addresses, snapshot.AllNodes)
			}
			results <- result{address: address, snapshot: snapshot, err: err}
		}()
	}

	var lastErr error
	for range candidates {
		res := <-results
		if res.err == nil {
			cancel()
			return res.snapshot, nil
		}
		lastErr = res.err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no topology candidates available")
	}
	log.Warnf("all cluster topology candidates failed. candidates=%v, last_err=[%v]", candidates, lastErr)
	return utils.ClusterNodesSnapshot{}, lastErr
}

func (r *RedisClusterWriter) applyRefreshedTopologyLocked(snapshot utils.ClusterNodesSnapshot, failedWriter clusterManagedWriter) (bool, error) {
	addresses, slots := snapshot.Addresses, snapshot.Slots
	log.Warnf("applying refreshed cluster topology. new_masters=%v, failed_writer=[%s]", addresses, failedWriter.Address())
	var newRouter [KeySlots]clusterManagedWriter
	newWriters := make([]clusterManagedWriter, 0, len(addresses))
	used := make(map[clusterManagedWriter]bool, len(r.writers))

	for i, slotGroup := range slots {
		if len(slotGroup) == 0 {
			return false, fmt.Errorf("empty slot group for refreshed cluster node address=%s", addresses[i])
		}
		owner := r.router[slotGroup[0]]
		if owner == nil {
			return false, fmt.Errorf("cannot map refreshed cluster node address=%s slot=%d to existing writer", addresses[i], slotGroup[0])
		}
		for _, slot := range slotGroup[1:] {
			if r.router[slot] != owner {
				return false, fmt.Errorf("cluster topology changed beyond failover for refreshed address=%s slot=%d", addresses[i], slot)
			}
		}
		if used[owner] {
			return false, fmt.Errorf("refreshed cluster topology maps multiple masters to writer address=%s", owner.Address())
		}
		used[owner] = true

		nextOpts := r.opts
		nextOpts.Address = addresses[i]
		shouldReconnect := owner == failedWriter || owner.Address() != addresses[i]
		if shouldReconnect {
			log.Warnf("reconnecting cluster writer. current_address=[%s], new_address=[%s], is_failed_writer=[%v]",
				owner.Address(), addresses[i], owner == failedWriter)
			if err := owner.UpdateTarget(r.ctx, &nextOpts); err != nil {
				log.Warnf("cluster writer UpdateTarget failed. address=[%s], err=[%v], will_retry=[true]", addresses[i], err)
				return true, err
			}
			r.bindReconnect(owner)
		} else {
			log.Warnf("cluster writer address unchanged. address=[%s]", owner.Address())
		}

		newWriters = append(newWriters, owner)
		for _, slot := range slotGroup {
			if newRouter[slot] != nil {
				return false, fmt.Errorf("duplicate slot %d in refreshed topology", slot)
			}
			newRouter[slot] = owner
		}
	}

	for slot := 0; slot < KeySlots; slot++ {
		if newRouter[slot] == nil {
			return false, fmt.Errorf("slot %d missing after topology refresh", slot)
		}
	}

	r.addresses = addresses
	r.knownNodes = dedupeClusterAddresses(snapshot.AllNodes)
	r.writers = newWriters
	r.router = newRouter
	log.Warnf("redis cluster topology refreshed. addresses=%v, known_nodes=%v", addresses, r.knownNodes)
	return false, nil
}

func (r *RedisClusterWriter) topologyCandidatesLocked(failedWriter clusterManagedWriter) []string {
	candidates := make([]string, 0, len(r.knownNodes)+len(r.addresses)+2)
	if failedWriter != nil {
		candidates = append(candidates, failedWriter.Address())
	}
	candidates = append(candidates, r.knownNodes...)
	candidates = append(candidates, r.addresses...)
	candidates = append(candidates, r.opts.Address)
	return dedupeClusterAddresses(candidates)
}

func dedupeClusterAddresses(addresses []string) []string {
	out := make([]string, 0, len(addresses))
	seen := make(map[string]struct{}, len(addresses))
	for _, address := range addresses {
		if address == "" {
			continue
		}
		if _, ok := seen[address]; ok {
			continue
		}
		seen[address] = struct{}{}
		out = append(out, address)
	}
	return out
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
