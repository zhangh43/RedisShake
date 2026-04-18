package writer

import "context"

type clusterManagedWriter interface {
	Writer
	Address() string
	SetReconnectFn(fn func() error)
	UpdateTarget(ctx context.Context, opts *RedisWriterOptions) error
}
