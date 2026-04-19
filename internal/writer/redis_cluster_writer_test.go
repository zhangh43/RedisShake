package writer

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"RedisShake/internal/client"
	"RedisShake/internal/entry"
	"RedisShake/internal/utils"

	"github.com/stretchr/testify/require"
)

type fakeClusterManagedWriter struct {
	address          string
	updateCalls      []string
	reconnectFn      func() error
	statusConsistent bool
}

func (w *fakeClusterManagedWriter) Write(*entry.Entry) {}

func (w *fakeClusterManagedWriter) StartWrite(context.Context) chan *entry.Entry { return nil }

func (w *fakeClusterManagedWriter) Close() {}

func (w *fakeClusterManagedWriter) Status() interface{} {
	return map[string]string{"address": w.address}
}

func (w *fakeClusterManagedWriter) StatusString() string { return w.address }

func (w *fakeClusterManagedWriter) StatusConsistent() bool { return w.statusConsistent }

func (w *fakeClusterManagedWriter) Address() string { return w.address }

func (w *fakeClusterManagedWriter) SetReconnectFn(fn func() error) { w.reconnectFn = fn }

func (w *fakeClusterManagedWriter) UpdateTarget(_ context.Context, opts *RedisWriterOptions) error {
	w.address = opts.Address
	w.updateCalls = append(w.updateCalls, opts.Address)
	return nil
}

// blockingFakeWriter is a fakeClusterManagedWriter whose Write sends to a real
// buffered channel. This allows tests to exercise the case where Write() blocks
// on a full channel, which is required to reproduce the deadlock between
// ClusterWriter.Write (holding r.mu.RLock) and refreshTopologyAndReconnect
// (waiting for r.mu.Lock).
type blockingFakeWriter struct {
	fakeClusterManagedWriter
	ch chan *entry.Entry
}

func (w *blockingFakeWriter) Write(e *entry.Entry) { w.ch <- e }

func TestRedisClusterWriterRefreshTopologyAndReconnect(t *testing.T) {
	ctx := context.Background()
	writerA := &fakeClusterManagedWriter{address: "10.0.0.1:6379", statusConsistent: true}
	writerB := &fakeClusterManagedWriter{address: "10.0.0.2:6379", statusConsistent: true}
	r := &RedisClusterWriter{
		ctx: ctx,
		opts: RedisWriterOptions{
			Address: "10.0.0.1:6379",
		},
		addresses:  []string{"10.0.0.1:6379", "10.0.0.2:6379"},
		knownNodes: []string{"10.0.0.1:6379", "10.0.0.10:6379", "10.0.0.2:6379", "10.0.0.20:6379"},
		writers:    []clusterManagedWriter{writerA, writerB},
		getClusterNodes: func(context.Context, string, string, string, bool, client.TlsConfig, bool) ([]string, [][]int) {
			return []string{"10.0.0.3:6379", "10.0.0.2:6379"}, [][]int{
				buildSlotRange(0, 8191),
				buildSlotRange(8192, 16383),
			}
		},
		getClusterSnapshotFromAny: func(context.Context, []string, string, string, bool, client.TlsConfig, bool) utils.ClusterNodesSnapshot {
			return utils.ClusterNodesSnapshot{
				Addresses: []string{"10.0.0.3:6379", "10.0.0.2:6379"},
				Slots: [][]int{
					buildSlotRange(0, 8191),
					buildSlotRange(8192, 16383),
				},
				AllNodes: []string{"10.0.0.3:6379", "10.0.0.30:6379", "10.0.0.2:6379", "10.0.0.20:6379"},
			}
		},
		getClusterSnapshot: func(context.Context, string, string, string, bool, client.TlsConfig, bool) (utils.ClusterNodesSnapshot, error) {
			return utils.ClusterNodesSnapshot{
				Addresses: []string{"10.0.0.3:6379", "10.0.0.2:6379"},
				Slots: [][]int{
					buildSlotRange(0, 8191),
					buildSlotRange(8192, 16383),
				},
				AllNodes: []string{"10.0.0.3:6379", "10.0.0.30:6379", "10.0.0.2:6379", "10.0.0.20:6379"},
			}, nil
		},
	}
	for slot := 0; slot <= 8191; slot++ {
		r.router[slot] = writerA
	}
	for slot := 8192; slot < KeySlots; slot++ {
		r.router[slot] = writerB
	}

	err := r.refreshTopologyAndReconnect(writerA)

	require.NoError(t, err)
	require.Equal(t, []string{"10.0.0.3:6379"}, writerA.updateCalls)
	require.Empty(t, writerB.updateCalls)
	require.Equal(t, "10.0.0.3:6379", writerA.Address())
	require.Equal(t, writerA, r.router[0])
	require.Equal(t, writerB, r.router[16383])
	require.Equal(t, []string{"10.0.0.3:6379", "10.0.0.2:6379"}, r.addresses)
	require.NotNil(t, writerA.reconnectFn)
}

func TestRedisClusterWriterRefreshTopologyRejectsSlotMigration(t *testing.T) {
	ctx := context.Background()
	writerA := &fakeClusterManagedWriter{address: "10.0.0.1:6379", statusConsistent: true}
	writerB := &fakeClusterManagedWriter{address: "10.0.0.2:6379", statusConsistent: true}
	r := &RedisClusterWriter{
		ctx: ctx,
		opts: RedisWriterOptions{
			Address: "10.0.0.1:6379",
		},
		addresses:  []string{"10.0.0.1:6379", "10.0.0.2:6379"},
		knownNodes: []string{"10.0.0.1:6379", "10.0.0.10:6379", "10.0.0.2:6379"},
		writers:    []clusterManagedWriter{writerA, writerB},
		getClusterNodes: func(context.Context, string, string, string, bool, client.TlsConfig, bool) ([]string, [][]int) {
			return []string{"10.0.0.3:6379"}, [][]int{
				{8191, 8192},
			}
		},
		getClusterSnapshotFromAny: func(context.Context, []string, string, string, bool, client.TlsConfig, bool) utils.ClusterNodesSnapshot {
			return utils.ClusterNodesSnapshot{
				Addresses: []string{"10.0.0.3:6379"},
				Slots: [][]int{
					{8191, 8192},
				},
				AllNodes: []string{"10.0.0.3:6379", "10.0.0.30:6379"},
			}
		},
		getClusterSnapshot: func(context.Context, string, string, string, bool, client.TlsConfig, bool) (utils.ClusterNodesSnapshot, error) {
			return utils.ClusterNodesSnapshot{
				Addresses: []string{"10.0.0.3:6379"},
				Slots: [][]int{
					{8191, 8192},
				},
				AllNodes: []string{"10.0.0.3:6379", "10.0.0.30:6379"},
			}, nil
		},
	}
	r.router[8191] = writerA
	r.router[8192] = writerB

	err := r.refreshTopologyAndReconnect(writerA)

	require.Error(t, err)
	require.Contains(t, err.Error(), "beyond failover")
}

func TestRedisClusterWriterBindReconnectUsesRefresh(t *testing.T) {
	ctx := context.Background()
	writerA := &fakeClusterManagedWriter{address: "10.0.0.1:6379", statusConsistent: true}
	r := &RedisClusterWriter{
		ctx: ctx,
		opts: RedisWriterOptions{
			Address: "10.0.0.1:6379",
		},
		addresses:  []string{"10.0.0.1:6379"},
		knownNodes: []string{"10.0.0.1:6379", "10.0.0.10:6379"},
		writers:    []clusterManagedWriter{writerA},
		getClusterNodes: func(context.Context, string, string, string, bool, client.TlsConfig, bool) ([]string, [][]int) {
			return []string{"10.0.0.4:6379"}, [][]int{buildSlotRange(0, 16383)}
		},
		getClusterSnapshotFromAny: func(context.Context, []string, string, string, bool, client.TlsConfig, bool) utils.ClusterNodesSnapshot {
			return utils.ClusterNodesSnapshot{
				Addresses: []string{"10.0.0.4:6379"},
				Slots:     [][]int{buildSlotRange(0, 16383)},
				AllNodes:  []string{"10.0.0.4:6379", "10.0.0.40:6379"},
			}
		},
		getClusterSnapshot: func(context.Context, string, string, string, bool, client.TlsConfig, bool) (utils.ClusterNodesSnapshot, error) {
			return utils.ClusterNodesSnapshot{
				Addresses: []string{"10.0.0.4:6379"},
				Slots:     [][]int{buildSlotRange(0, 16383)},
				AllNodes:  []string{"10.0.0.4:6379", "10.0.0.40:6379"},
			}, nil
		},
	}
	for slot := 0; slot < KeySlots; slot++ {
		r.router[slot] = writerA
	}

	r.bindReconnect(writerA)
	err := writerA.reconnectFn()

	require.NoError(t, err)
	require.Equal(t, []string{"10.0.0.4:6379"}, writerA.updateCalls)
}

func TestRedisClusterWriterFetchLatestTopologyReturnsFirstSuccessfulResult(t *testing.T) {
	ctx := context.Background()
	blockedStarted := make(chan struct{}, 1)
	blockedFinished := make(chan struct{}, 1)
	r := &RedisClusterWriter{
		ctx: ctx,
		opts: RedisWriterOptions{
			Address: "seed:6379",
		},
		getClusterSnapshot: func(ctx context.Context, address string, _ string, _ string, _ bool, _ client.TlsConfig, _ bool) (utils.ClusterNodesSnapshot, error) {
			if address == "10.0.0.1:6379" {
				blockedStarted <- struct{}{}
				<-ctx.Done()
				blockedFinished <- struct{}{}
				return utils.ClusterNodesSnapshot{}, ctx.Err()
			}
			return utils.ClusterNodesSnapshot{
				Addresses: []string{"10.0.0.3:6379"},
				Slots:     [][]int{buildSlotRange(0, 16383)},
				AllNodes:  []string{"10.0.0.3:6379", "10.0.0.33:6379"},
			}, nil
		},
	}

	start := time.Now()
	snapshot, err := r.fetchLatestTopologyLocked([]string{"10.0.0.1:6379", "10.0.0.11:6379"})
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Equal(t, []string{"10.0.0.3:6379"}, snapshot.Addresses)
	require.Less(t, elapsed, 100*time.Millisecond)
	select {
	case <-blockedStarted:
	default:
		t.Fatalf("blocked candidate was not started")
	}
	select {
	case <-blockedFinished:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("blocked candidate was not canceled after successful topology fetch")
	}
}

// TestRedisClusterWriterRefreshTopologyQueriesAllCandidates verifies that the
// topology refresh queries ALL known nodes in parallel — including the failedWriter
// itself. Previously the failedWriter was excluded, which broke 2-node clusters
// where the failedWriter becomes the new master after the old master dies.
func TestRedisClusterWriterRefreshTopologyQueriesAllCandidates(t *testing.T) {
	ctx := context.Background()
	writerA := &fakeClusterManagedWriter{address: "10.0.0.1:6379", statusConsistent: true}
	writerB := &fakeClusterManagedWriter{address: "10.0.0.2:6379", statusConsistent: true}
	// 5 unique candidates after dedup: knownNodes(4) + opts.Address(1 new)
	calledCh := make(chan string, 5)
	r := &RedisClusterWriter{
		ctx: ctx,
		opts: RedisWriterOptions{
			Address: "seed:6379",
		},
		addresses:  []string{"10.0.0.1:6379", "10.0.0.2:6379"},
		knownNodes: []string{"10.0.0.1:6379", "10.0.0.11:6379", "10.0.0.2:6379", "10.0.0.22:6379"},
		writers:    []clusterManagedWriter{writerA, writerB},
		getClusterSnapshotFromAny: func(_ context.Context, addresses []string, _ string, _ string, _ bool, _ client.TlsConfig, _ bool) utils.ClusterNodesSnapshot {
			return utils.ClusterNodesSnapshot{
				Addresses: []string{"10.0.0.3:6379", "10.0.0.2:6379"},
				Slots: [][]int{
					buildSlotRange(0, 8191),
					buildSlotRange(8192, 16383),
				},
				AllNodes: []string{"10.0.0.3:6379", "10.0.0.33:6379", "10.0.0.2:6379", "10.0.0.22:6379"},
			}
		},
		getClusterSnapshot: func(_ context.Context, address string, _ string, _ string, _ bool, _ client.TlsConfig, _ bool) (utils.ClusterNodesSnapshot, error) {
			calledCh <- address
			if address == "10.0.0.1:6379" {
				// failedWriter is slow — another candidate should win the race
				time.Sleep(50 * time.Millisecond)
				return utils.ClusterNodesSnapshot{}, context.DeadlineExceeded
			}
			return utils.ClusterNodesSnapshot{
				Addresses: []string{"10.0.0.3:6379", "10.0.0.2:6379"},
				Slots: [][]int{
					buildSlotRange(0, 8191),
					buildSlotRange(8192, 16383),
				},
				AllNodes: []string{"10.0.0.3:6379", "10.0.0.33:6379", "10.0.0.2:6379", "10.0.0.22:6379"},
			}, nil
		},
	}
	for slot := 0; slot <= 8191; slot++ {
		r.router[slot] = writerA
	}
	for slot := 8192; slot < KeySlots; slot++ {
		r.router[slot] = writerB
	}

	err := r.refreshTopologyAndReconnect(writerA)

	require.NoError(t, err)
	// Collect all 5 queried addresses (one per candidate, sent before any blocking)
	queried := make([]string, 5)
	for i := range queried {
		queried[i] = <-calledCh
	}
	// failedWriter IS now included as a candidate (unlike the old behavior)
	require.True(t, containsString(queried, "10.0.0.1:6379"), "failedWriter must be queried")
	// at least one other candidate was also queried
	require.True(t, containsString(queried, "10.0.0.11:6379") || containsString(queried, "10.0.0.2:6379") || containsString(queried, "10.0.0.22:6379"))
	require.Equal(t, []string{"10.0.0.3:6379"}, writerA.updateCalls)
	require.Equal(t, []string{"10.0.0.3:6379", "10.0.0.33:6379", "10.0.0.2:6379", "10.0.0.22:6379"}, r.knownNodes)
}

// TestRedisClusterWriterRefreshTopologyWorksWhenOnlyFailedWriterIsAvailable covers
// the 2-node cluster production scenario: old master dies, failedWriter becomes the
// new master, but all OTHER topology candidates are also unreachable. The refresh
// must query the failedWriter itself to discover the new topology.
func TestRedisClusterWriterRefreshTopologyWorksWhenOnlyFailedWriterIsAvailable(t *testing.T) {
	ctx := context.Background()
	writerA := &fakeClusterManagedWriter{address: "10.0.0.1:6379", statusConsistent: true}
	r := &RedisClusterWriter{
		ctx: ctx,
		opts: RedisWriterOptions{
			Address: "10.0.0.1:6379", // seed = failedWriter (2-node cluster, only node left)
		},
		addresses:  []string{"10.0.0.1:6379"},
		knownNodes: []string{"10.0.0.1:6379", "10.0.0.2:6379"}, // 10.0.0.2 was old master, now dead
		writers:    []clusterManagedWriter{writerA},
		getClusterSnapshot: func(_ context.Context, address string, _ string, _ string, _ bool, _ client.TlsConfig, _ bool) (utils.ClusterNodesSnapshot, error) {
			if address == "10.0.0.2:6379" {
				return utils.ClusterNodesSnapshot{}, fmt.Errorf("connection refused")
			}
			// 10.0.0.1 is the new master — returns correct topology
			return utils.ClusterNodesSnapshot{
				Addresses: []string{"10.0.0.1:6379"},
				Slots:     [][]int{buildSlotRange(0, 16383)},
				AllNodes:  []string{"10.0.0.1:6379", "10.0.0.2:6379"},
			}, nil
		},
	}
	for slot := 0; slot < KeySlots; slot++ {
		r.router[slot] = writerA
	}

	err := r.refreshTopologyAndReconnect(writerA)

	require.NoError(t, err)
	// writerA reconnected to itself (it's now the master)
	require.Equal(t, []string{"10.0.0.1:6379"}, writerA.updateCalls)
}

func TestRedisClusterWriterRefreshTopologyCarriesAllKnownNodesAcrossRefreshes(t *testing.T) {
	ctx := context.Background()
	writerA := &fakeClusterManagedWriter{address: "10.0.0.1:6379", statusConsistent: true}
	writerB := &fakeClusterManagedWriter{address: "10.0.0.2:6379", statusConsistent: true}
	var (
		mu        sync.Mutex
		called    []string
		refreshes = make(map[string]int)
	)
	r := &RedisClusterWriter{
		ctx: ctx,
		opts: RedisWriterOptions{
			Address: "seed:6379",
		},
		addresses:  []string{"10.0.0.1:6379", "10.0.0.2:6379"},
		knownNodes: []string{"10.0.0.1:6379", "10.0.0.11:6379", "10.0.0.2:6379", "10.0.0.22:6379"},
		writers:    []clusterManagedWriter{writerA, writerB},
		getClusterSnapshot: func(_ context.Context, address string, _ string, _ string, _ bool, _ client.TlsConfig, _ bool) (utils.ClusterNodesSnapshot, error) {
			mu.Lock()
			called = append(called, address)
			refreshes[address]++
			count := refreshes[address]
			hasNewMaster := false
			for _, addr := range called {
				if addr == "10.0.0.3:6379" {
					hasNewMaster = true
					break
				}
			}
			mu.Unlock()
			if hasNewMaster && address != "10.0.0.3:6379" {
				return utils.ClusterNodesSnapshot{}, context.DeadlineExceeded
			}
			if address == "10.0.0.1:6379" && count == 1 {
				return utils.ClusterNodesSnapshot{}, context.DeadlineExceeded
			}
			if address == "10.0.0.11:6379" && count == 1 {
				return utils.ClusterNodesSnapshot{
					Addresses: []string{"10.0.0.3:6379", "10.0.0.2:6379"},
					Slots: [][]int{
						buildSlotRange(0, 8191),
						buildSlotRange(8192, 16383),
					},
					AllNodes: []string{"10.0.0.3:6379", "10.0.0.33:6379", "10.0.0.2:6379", "10.0.0.22:6379"},
				}, nil
			}
			return utils.ClusterNodesSnapshot{
				Addresses: []string{"10.0.0.33:6379", "10.0.0.2:6379"},
				Slots: [][]int{
					buildSlotRange(0, 8191),
					buildSlotRange(8192, 16383),
				},
				AllNodes: []string{"10.0.0.33:6379", "10.0.0.3:6379", "10.0.0.2:6379", "10.0.0.22:6379"},
			}, nil
		},
	}
	for slot := 0; slot <= 8191; slot++ {
		r.router[slot] = writerA
	}
	for slot := 8192; slot < KeySlots; slot++ {
		r.router[slot] = writerB
	}

	err := r.refreshTopologyAndReconnect(writerA)
	require.NoError(t, err)
	firstAddress := writerA.Address()
	require.Contains(t, []string{"10.0.0.3:6379", "10.0.0.33:6379"}, firstAddress)

	err = r.refreshTopologyAndReconnect(writerA)
	require.NoError(t, err)
	require.Equal(t, "10.0.0.33:6379", writerA.Address())
	// Snapshot called under mu to avoid a data race with goroutines from
	// fetchLatestTopologyLocked that may still be writing after cancel().
	mu.Lock()
	calledSnapshot := append([]string(nil), called...)
	mu.Unlock()
	require.Contains(t, calledSnapshot, "10.0.0.11:6379")
	require.Contains(t, calledSnapshot, "10.0.0.3:6379")
}

func buildSlotRange(start, end int) []int {
	slots := make([]int, 0, end-start+1)
	for slot := start; slot <= end; slot++ {
		slots = append(slots, slot)
	}
	return slots
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// TestClusterWriterWriteDoesNotDeadlockDuringReconnect verifies that
// ClusterWriter.Write() releases r.mu.RLock before blocking on the downstream
// channel send. Without the fix, Write() would block on a full channel while
// still holding r.mu.RLock, which prevents refreshTopologyAndReconnect from
// acquiring r.mu.Lock — a deadlock.
//
// The test uses -timeout (via `go test -timeout 30s`) as the deadlock detector:
// if the test hangs, go test will kill it and print all goroutine stacks.
func TestClusterWriterWriteDoesNotDeadlockDuringReconnect(t *testing.T) {
	const chanSize = 1
	ctx := context.Background()

	ch := make(chan *entry.Entry, chanSize)
	writer := &blockingFakeWriter{
		fakeClusterManagedWriter: fakeClusterManagedWriter{
			address:          "10.0.0.1:6379",
			statusConsistent: true,
		},
		ch: ch,
	}

	r := &RedisClusterWriter{
		ctx:  ctx,
		opts: RedisWriterOptions{Address: "10.0.0.2:6379"},
		getClusterSnapshot: func(_ context.Context, _ string, _ string, _ string, _ bool, _ client.TlsConfig, _ bool) (utils.ClusterNodesSnapshot, error) {
			return utils.ClusterNodesSnapshot{
				Addresses: []string{"10.0.0.2:6379"},
				Slots:     [][]int{buildSlotRange(0, 16383)},
				AllNodes:  []string{"10.0.0.2:6379"},
			}, nil
		},
		addresses:  []string{"10.0.0.1:6379"},
		knownNodes: []string{"10.0.0.1:6379"},
		writers:    []clusterManagedWriter{writer},
	}
	for slot := 0; slot < KeySlots; slot++ {
		r.router[slot] = writer
	}

	e := &entry.Entry{Slots: []int{0}}

	// Fill the channel to capacity so the next Write will block.
	ch <- e

	// Start a Write that will block on the full channel.
	writeBlocking := make(chan struct{})
	writeDone := make(chan struct{})
	go func() {
		close(writeBlocking)
		r.Write(e)
		close(writeDone)
	}()
	<-writeBlocking
	// Give Write() a moment to reach the channel send and block there.
	time.Sleep(20 * time.Millisecond)

	// refreshTopologyAndReconnect must complete within the timeout.
	// With the old code (defer RUnlock inside Write), it would deadlock because
	// Write holds r.mu.RLock while blocked on the channel, preventing Lock.
	reconnectDone := make(chan struct{})
	go func() {
		_ = r.refreshTopologyAndReconnect(writer)
		close(reconnectDone)
	}()

	const deadline = 2 * time.Second
	select {
	case <-reconnectDone:
		// success
	case <-time.After(deadline):
		t.Fatalf("deadlock: refreshTopologyAndReconnect did not complete within %s — "+
			"Write() is likely holding r.mu.RLock while blocked on the channel send", deadline)
	}

	// Drain the channel so the blocked Write() goroutine can finish.
	<-ch
	select {
	case <-writeDone:
	case <-time.After(time.Second):
		t.Fatal("Write() did not complete after channel was drained")
	}
}
