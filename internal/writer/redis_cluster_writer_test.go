package writer

import (
	"context"
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

func TestRedisClusterWriterRefreshTopologyAndReconnect(t *testing.T) {
	ctx := context.Background()
	writerA := &fakeClusterManagedWriter{address: "10.0.0.1:6379", statusConsistent: true}
	writerB := &fakeClusterManagedWriter{address: "10.0.0.2:6379", statusConsistent: true}
	r := &RedisClusterWriter{
		ctx: ctx,
		opts: RedisWriterOptions{
			Address: "10.0.0.1:6379",
		},
		writers: []clusterManagedWriter{writerA, writerB},
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
		writers: []clusterManagedWriter{writerA, writerB},
		getClusterNodes: func(context.Context, string, string, string, bool, client.TlsConfig, bool) ([]string, [][]int) {
			return []string{"10.0.0.3:6379"}, [][]int{
				[]int{8191, 8192},
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
		writers: []clusterManagedWriter{writerA},
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

func TestRedisClusterWriterRefreshTopologyUsesFallbackCandidate(t *testing.T) {
	ctx := context.Background()
	writerA := &fakeClusterManagedWriter{address: "10.0.0.1:6379", statusConsistent: true}
	writerB := &fakeClusterManagedWriter{address: "10.0.0.2:6379", statusConsistent: true}
	called := make(chan string, 5)
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
			called <- address
			if address == "10.0.0.1:6379" {
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
	got := []string{<-called, <-called}
	require.True(t, containsString(got, "10.0.0.11:6379") || containsString(got, "10.0.0.2:6379") || containsString(got, "10.0.0.22:6379"))
	require.Equal(t, []string{"10.0.0.3:6379"}, writerA.updateCalls)
	require.Equal(t, []string{"10.0.0.3:6379", "10.0.0.33:6379", "10.0.0.2:6379", "10.0.0.22:6379"}, r.knownNodes)
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
	require.Contains(t, called, "10.0.0.11:6379")
	require.Contains(t, called, "10.0.0.3:6379")
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
