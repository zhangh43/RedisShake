package types

import (
	"RedisShake/internal/config"
	"strconv"
	"testing"
)

func BenchmarkRewriteBatch_List(b *testing.B) {
	benchmarkBatchSizes := []int{1, 16, 64, 128, 256, 512}
	elementCount := 20000
	elements := make([]string, elementCount)
	for i := 0; i < elementCount; i++ {
		elements[i] = "v" + strconv.Itoa(i)
	}

	orig := config.Opt.Advanced.RewriteCollectionBatchSize
	defer func() {
		config.Opt.Advanced.RewriteCollectionBatchSize = orig
	}()

	for _, batchSize := range benchmarkBatchSizes {
		b.Run("batch_"+strconv.Itoa(batchSize), func(b *testing.B) {
			config.Opt.Advanced.RewriteCollectionBatchSize = batchSize
			cmdCap := elementCount/batchSize + 16
			if cmdCap < 1024 {
				cmdCap = 1024
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				o := &ListObject{
					key:  "bench:list",
					cmdC: make(chan RedisCmd, cmdCap),
				}
				for pos := 0; pos < len(elements); pos += batchSize {
					end := pos + batchSize
					if end > len(elements) {
						end = len(elements)
					}
					o.emitRPushBatch(elements[pos:end])
				}
				close(o.cmdC)
				for range o.cmdC {
				}
			}
		})
	}
}

func BenchmarkRewriteBatch_ZSet(b *testing.B) {
	benchmarkBatchSizes := []int{1, 16, 64, 128, 256, 512}
	memberCount := 20000
	scoreMembers := make([]string, memberCount*2)
	for i := 0; i < memberCount; i++ {
		scoreMembers[i*2] = strconv.Itoa(i)
		scoreMembers[i*2+1] = "m" + strconv.Itoa(i)
	}

	orig := config.Opt.Advanced.RewriteCollectionBatchSize
	defer func() {
		config.Opt.Advanced.RewriteCollectionBatchSize = orig
	}()

	for _, batchSize := range benchmarkBatchSizes {
		b.Run("batch_"+strconv.Itoa(batchSize), func(b *testing.B) {
			config.Opt.Advanced.RewriteCollectionBatchSize = batchSize
			cmdCap := memberCount/batchSize + 16
			if cmdCap < 1024 {
				cmdCap = 1024
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				o := &ZsetObject{
					key:  "bench:zset",
					cmdC: make(chan RedisCmd, cmdCap),
				}
				for pos := 0; pos < len(scoreMembers); pos += batchSize * 2 {
					end := pos + batchSize*2
					if end > len(scoreMembers) {
						end = len(scoreMembers)
					}
					o.emitZAddBatch(scoreMembers[pos:end])
				}
				close(o.cmdC)
				for range o.cmdC {
				}
			}
		})
	}
}
