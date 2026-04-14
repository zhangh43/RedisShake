package writer

import (
	"testing"

	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"github.com/stretchr/testify/require"
)

func Test_resolveStandaloneWriterShards_AutoAndConfigured(t *testing.T) {
	orig := config.Opt.Advanced
	defer func() {
		config.Opt.Advanced = orig
	}()

	config.Opt.Advanced.TargetRedisWriterShards = 4
	require.Equal(t, 4, resolveStandaloneWriterShards())

	config.Opt.Advanced.TargetRedisWriterShards = 0
	config.Opt.Advanced.Ncpu = 4
	config.Opt.Advanced.PipelineCountLimit = 1024
	config.Opt.Advanced.TargetRedisMaxQPS = 200000
	require.Equal(t, 4, resolveStandaloneWriterShards())

	config.Opt.Advanced.Ncpu = 1
	require.Equal(t, 1, resolveStandaloneWriterShards())
}

func Test_redisShardedStandaloneWriter_routeStableByKey(t *testing.T) {
	w := &redisShardedStandaloneWriter{shardCount: 4}
	e1 := &entry.Entry{Argv: []string{"set", "foo", "1"}}
	e2 := &entry.Entry{Argv: []string{"set", "foo", "2"}}
	e3 := &entry.Entry{Argv: []string{"set", "bar", "2"}}

	r1 := w.route(e1)
	r2 := w.route(e2)
	r3 := w.route(e3)

	require.Equal(t, r1, r2)
	require.GreaterOrEqual(t, r1, 0)
	require.Less(t, r1, 4)
	require.GreaterOrEqual(t, r3, 0)
	require.Less(t, r3, 4)
}

func Test_redisShardedStandaloneWriter_routeBySlot(t *testing.T) {
	w := &redisShardedStandaloneWriter{shardCount: 8}
	e := &entry.Entry{Slots: []int{13}}
	require.Equal(t, 13%8, w.route(e))
}
