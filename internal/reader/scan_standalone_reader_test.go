package reader

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"RedisShake/internal/client"
	"RedisShake/internal/config"
	"github.com/stretchr/testify/require"
)

func Test_scanStandaloneReader_reconnectSource(t *testing.T) {
	orig := config.Opt.Advanced
	defer func() {
		config.Opt.Advanced = orig
	}()

	config.Opt.Advanced.IOReconnect = true
	config.Opt.Advanced.IOReconnectMaxTimes = 3
	config.Opt.Advanced.IOReconnectDelayMs = 0

	attempts := 0
	r := &scanStandaloneReader{
		reconnectFn: func(_ *client.Redis) error {
			attempts++
			if attempts < 2 {
				return errors.New("unexpected EOF")
			}
			return nil
		},
	}

	require.True(t, r.reconnectSource(nil, errors.New("unexpected EOF")))
	require.Equal(t, 2, attempts)
}

func Test_scanStandaloneReader_waitForScanQueueCapacity(t *testing.T) {
	var queueLen atomic.Int64
	queueLen.Store(3)

	r := &scanStandaloneReader{
		ctx: context.Background(),
		opts: &ScanReaderOptions{
			ScanHighQueueLen: 2,
		},
		queueLen: func() int {
			return int(queueLen.Load())
		},
	}

	done := make(chan bool, 1)
	go func() {
		done <- r.waitForScanQueueCapacity(10)
	}()

	select {
	case <-done:
		t.Fatal("waitForScanQueueCapacity returned before queue length dropped below threshold")
	case <-time.After(50 * time.Millisecond):
	}

	queueLen.Store(1)

	select {
	case ok := <-done:
		require.True(t, ok)
		require.False(t, r.scanPaused.Load())
	case <-time.After(300 * time.Millisecond):
		t.Fatal("waitForScanQueueCapacity did not resume after queue length dropped below threshold")
	}
}

func Test_scanStandaloneReader_waitForScanQueueCapacity_ContextDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := &scanStandaloneReader{
		ctx: ctx,
		opts: &ScanReaderOptions{
			ScanHighQueueLen: 1,
		},
		queueLen: func() int {
			return 2
		},
	}

	require.False(t, r.waitForScanQueueCapacity(10))
}

func Test_scanStandaloneReader_waitForScanQueueCapacity_HighLowWatermark(t *testing.T) {
	var queueLen atomic.Int64
	queueLen.Store(10)

	r := &scanStandaloneReader{
		ctx: context.Background(),
		opts: &ScanReaderOptions{
			ScanHighQueueLen: 10,
			ScanLowQueueLen:  5,
		},
		queueLen: func() int {
			return int(queueLen.Load())
		},
	}

	done := make(chan bool, 1)
	go func() {
		done <- r.waitForScanQueueCapacity(1)
	}()

	select {
	case <-done:
		t.Fatal("waitForScanQueueCapacity returned before queue length dropped below low watermark")
	case <-time.After(50 * time.Millisecond):
		require.True(t, r.scanPaused.Load())
	}

	queueLen.Store(6)
	select {
	case <-done:
		t.Fatal("waitForScanQueueCapacity returned before queue length dropped below low watermark")
	case <-time.After(50 * time.Millisecond):
	}

	queueLen.Store(5)
	select {
	case ok := <-done:
		require.True(t, ok)
		require.False(t, r.scanPaused.Load())
	case <-time.After(300 * time.Millisecond):
		t.Fatal("waitForScanQueueCapacity did not resume after queue length dropped to low watermark")
	}
}

func Test_scanStandaloneReader_shouldDropKSNOnBackpressure(t *testing.T) {
	r := &scanStandaloneReader{
		ctx: context.Background(),
		opts: &ScanReaderOptions{
			ScanHighQueueLen:      10,
			DropKSNOnBackpressure: true,
		},
		queueLen: func() int {
			return 10
		},
	}

	require.True(t, r.shouldDropKSNOnBackpressure())

	r = &scanStandaloneReader{
		ctx: context.Background(),
		opts: &ScanReaderOptions{
			ScanHighQueueLen:      10,
			ScanLowQueueLen:       5,
			DropKSNOnBackpressure: true,
		},
		queueLen: func() int {
			return 6
		},
	}
	r.scanPaused.Store(true)

	require.True(t, r.shouldDropKSNOnBackpressure())
}

func Test_scanStandaloneReader_shouldNotDropKSNWithoutBackpressure(t *testing.T) {
	tests := []struct {
		name string
		opts *ScanReaderOptions
		len  int
	}{
		{
			name: "disabled",
			opts: &ScanReaderOptions{
				ScanHighQueueLen:      10,
				DropKSNOnBackpressure: false,
			},
			len: 10,
		},
		{
			name: "threshold disabled",
			opts: &ScanReaderOptions{
				ScanHighQueueLen:      0,
				DropKSNOnBackpressure: true,
			},
			len: 100,
		},
		{
			name: "below threshold",
			opts: &ScanReaderOptions{
				ScanHighQueueLen:      10,
				DropKSNOnBackpressure: true,
			},
			len: 9,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &scanStandaloneReader{
				ctx:  context.Background(),
				opts: tt.opts,
				queueLen: func() int {
					return tt.len
				},
			}

			require.False(t, r.shouldDropKSNOnBackpressure())
		})
	}
}

func Test_scanStandaloneReader_StatusIncludesSyncedKeyCount(t *testing.T) {
	r := &scanStandaloneReader{opts: &ScanReaderOptions{Scan: true}}
	r.stat.Name = "reader_test"
	r.stat.ScanDbId = 10
	r.stat.ScanPercentByDbId = "12.34%"
	r.stat.SyncedKeyCount = 123
	r.stat.NeedUpdateCount = 45
	r.stat.lastSyncedKeyCount = 100
	r.stat.lastSyncedKeyUpdateTSSecond = float64(time.Now().Add(-1*time.Second).UnixNano()) / 1e9
	r.updateSyncedKeyOPS()

	got, err := json.Marshal(r.Status())
	require.NoError(t, err)
	require.Contains(t, string(got), `"synced_key_count":123`)
	require.Contains(t, string(got), `"synced_key_ops":`)
	require.Contains(t, r.StatusString(), "synced_key_count=[123]")
	require.Contains(t, r.StatusString(), "synced_key_ops=[")
}

func Test_scanStandaloneReader_StatusString_KSNOnlyHidesScanFields(t *testing.T) {
	r := &scanStandaloneReader{opts: &ScanReaderOptions{Scan: false}}
	r.stat.ScanDbId = 10
	r.stat.ScanPercentByDbId = "12.34%"
	r.stat.SyncedKeyCount = 123
	r.stat.NeedUpdateCount = 45
	r.stat.lastSyncedKeyCount = 100
	r.stat.lastSyncedKeyUpdateTSSecond = float64(time.Now().Add(-1*time.Second).UnixNano()) / 1e9
	r.updateSyncedKeyOPS()

	status := r.StatusString()
	require.NotContains(t, status, "scan_dbid=")
	require.NotContains(t, status, "scan_percent=")
	require.Contains(t, status, "synced_key_count=[123]")
	require.Contains(t, status, "synced_key_ops=[")
}

func Test_formatSampleReply(t *testing.T) {
	require.Equal(t, "abc", formatSampleReply("abc"))
	require.Equal(t, "[a, 1, [b, c]]", formatSampleReply([]interface{}{"a", int64(1), []interface{}{"b", "c"}}))
}

func Test_truncateSampleValue(t *testing.T) {
	require.Equal(t, "abc", truncateSampleValue("abc", 10))
	require.Equal(t, "ab...", truncateSampleValue("abcdefg", 5))
	require.Equal(t, "ab", truncateSampleValue("abcdefg", 2))
	require.Equal(t, "abc", truncateSampleValue("abcdefg", 3))
}
