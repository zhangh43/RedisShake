package writer

import (
	"errors"
	"testing"

	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"github.com/stretchr/testify/require"
)

func Test_redisStandaloneWriter_reconnectIO(t *testing.T) {
	orig := config.Opt.Advanced
	defer func() {
		config.Opt.Advanced = orig
	}()

	config.Opt.Advanced.IOReconnect = true
	config.Opt.Advanced.IOReconnectMaxTimes = 3
	config.Opt.Advanced.IOReconnectDelayMs = 0

	attempts := 0
	w := &redisStandaloneWriter{
		reconnectFn: func() error {
			attempts++
			if attempts < 2 {
				return errors.New("unexpected EOF")
			}
			return nil
		},
	}

	require.True(t, w.reconnectIO())
	require.Equal(t, 2, attempts)
}

func Test_redisStandaloneWriter_resetInflight(t *testing.T) {
	w := &redisStandaloneWriter{}
	e1 := &entry.Entry{CmdName: "set", SerializedSize: 10}
	e2 := &entry.Entry{CmdName: "restore", SerializedSize: 20}
	w.pending = []*entry.Entry{e1, e2}
	w.replyEpoch = 7
	w.stat.UnansweredBytes = 30
	w.stat.UnansweredEntries = 2

	pending := w.resetInflight()

	require.Equal(t, []*entry.Entry{e1, e2}, pending)
	require.Empty(t, w.pending)
	require.EqualValues(t, 8, w.replyEpoch)
	require.EqualValues(t, 0, w.stat.UnansweredBytes)
	require.EqualValues(t, 0, w.stat.UnansweredEntries)
}

func Test_isTargetRedisOOMError(t *testing.T) {
	require.True(t, isTargetRedisOOMError(errors.New("Transaction failed due to out of memory.")))
	require.True(t, isTargetRedisOOMError(errors.New("OOM command not allowed when used memory > 'maxmemory'.")))
	require.False(t, isTargetRedisOOMError(errors.New("ERR unknown command `xadd`")))
}

func Test_redisStandaloneWriter_shouldRequeueOnOOM(t *testing.T) {
	orig := config.Opt.Advanced
	defer func() {
		config.Opt.Advanced = orig
	}()

	config.Opt.Advanced.TargetRedisOOMRequeue = true
	config.Opt.Advanced.TargetRedisOOMRequeueMaxTimes = 3

	w := &redisStandaloneWriter{}
	e := &entry.Entry{
		CmdName:    "set",
		RetryCount: 2,
	}

	require.True(t, w.shouldRequeueOnOOM(errors.New("Transaction failed due to out of memory."), e))

	e.RetryCount = 3
	require.False(t, w.shouldRequeueOnOOM(errors.New("Transaction failed due to out of memory."), e))
	require.False(t, w.shouldRequeueOnOOM(errors.New("ERR unknown command"), &entry.Entry{CmdName: "set"}))
	require.False(t, w.shouldRequeueOnOOM(errors.New("Transaction failed due to out of memory."), &entry.Entry{CmdName: "select"}))

	config.Opt.Advanced.TargetRedisOOMRequeue = false
	require.False(t, w.shouldRequeueOnOOM(errors.New("Transaction failed due to out of memory."), &entry.Entry{CmdName: "set"}))
}
