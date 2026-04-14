package types

import (
	"RedisShake/internal/config"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListObject_emitRPushBatch(t *testing.T) {
	o := &ListObject{
		key:  "k",
		cmdC: make(chan RedisCmd, 2),
	}

	elements := []string{"a", "b", "c"}
	o.emitRPushBatch(elements)

	cmd := <-o.cmdC
	require.Equal(t, RedisCmd{"rpush", "k", "a", "b", "c"}, cmd)
}

func TestSetObject_emitSAddBatch(t *testing.T) {
	o := &SetObject{
		key:  "k",
		cmdC: make(chan RedisCmd, 2),
	}

	elements := []string{"a", "b", "c"}
	o.emitSAddBatch(elements)

	cmd := <-o.cmdC
	require.Equal(t, RedisCmd{"sadd", "k", "a", "b", "c"}, cmd)
}

func TestListObject_emitRPushBatch_LargePayload(t *testing.T) {
	o := &ListObject{
		key:  "k",
		cmdC: make(chan RedisCmd, 1),
	}

	elements := make([]string, defaultRewriteCollectionBatchSize)
	for i := range elements {
		elements[i] = strconv.Itoa(i)
	}
	o.emitRPushBatch(elements)

	cmd := <-o.cmdC
	require.Equal(t, 2+defaultRewriteCollectionBatchSize, len(cmd))
	require.Equal(t, "rpush", cmd[0])
	require.Equal(t, "k", cmd[1])
}

func TestHashObject_emitHSetBatch(t *testing.T) {
	o := &HashObject{
		key:  "k",
		cmdC: make(chan RedisCmd, 1),
	}
	o.emitHSetBatch([]string{"f1", "v1", "f2", "v2"})
	cmd := <-o.cmdC
	require.Equal(t, RedisCmd{"hset", "k", "f1", "v1", "f2", "v2"}, cmd)
}

func TestZsetObject_emitZAddBatch(t *testing.T) {
	o := &ZsetObject{
		key:  "k",
		cmdC: make(chan RedisCmd, 1),
	}
	o.emitZAddBatch([]string{"1", "m1", "2", "m2"})
	cmd := <-o.cmdC
	require.Equal(t, RedisCmd{"zadd", "k", "1", "m1", "2", "m2"}, cmd)
}

func TestGetRewriteCollectionBatchSize_DefaultAndOverride(t *testing.T) {
	orig := config.Opt.Advanced.RewriteCollectionBatchSize
	defer func() {
		config.Opt.Advanced.RewriteCollectionBatchSize = orig
	}()

	config.Opt.Advanced.RewriteCollectionBatchSize = 0
	require.Equal(t, defaultRewriteCollectionBatchSize, getRewriteCollectionBatchSize())

	config.Opt.Advanced.RewriteCollectionBatchSize = 256
	require.Equal(t, 256, getRewriteCollectionBatchSize())
}
