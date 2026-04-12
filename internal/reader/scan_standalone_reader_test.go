package reader

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_scanStandaloneReader_waitForScanQueueCapacity(t *testing.T) {
	var queueLen atomic.Int64
	queueLen.Store(3)

	r := &scanStandaloneReader{
		ctx: context.Background(),
		opts: &ScanReaderOptions{
			ScanMaxQueueLen: 2,
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
			ScanMaxQueueLen: 1,
		},
		queueLen: func() int {
			return 1
		},
	}

	require.False(t, r.waitForScanQueueCapacity(10))
}

func Test_scanStandaloneReader_shouldDropKSNOnBackpressure(t *testing.T) {
	r := &scanStandaloneReader{
		ctx: context.Background(),
		opts: &ScanReaderOptions{
			ScanMaxQueueLen:       10,
			DropKSNOnBackpressure: true,
		},
		queueLen: func() int {
			return 10
		},
	}

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
				ScanMaxQueueLen:       10,
				DropKSNOnBackpressure: false,
			},
			len: 10,
		},
		{
			name: "threshold disabled",
			opts: &ScanReaderOptions{
				ScanMaxQueueLen:       0,
				DropKSNOnBackpressure: true,
			},
			len: 100,
		},
		{
			name: "below threshold",
			opts: &ScanReaderOptions{
				ScanMaxQueueLen:       10,
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
