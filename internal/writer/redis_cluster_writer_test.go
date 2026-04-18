package writer

import (
	"context"
	"testing"

	"RedisShake/internal/client"
	"RedisShake/internal/entry"

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
	}
	for slot := 0; slot < KeySlots; slot++ {
		r.router[slot] = writerA
	}

	r.bindReconnect(writerA)
	err := writerA.reconnectFn()

	require.NoError(t, err)
	require.Equal(t, []string{"10.0.0.4:6379"}, writerA.updateCalls)
}

func buildSlotRange(start, end int) []int {
	slots := make([]int, 0, end-start+1)
	for slot := start; slot <= end; slot++ {
		slots = append(slots, slot)
	}
	return slots
}
