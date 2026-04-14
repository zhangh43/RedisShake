package types

import "RedisShake/internal/config"

const defaultRewriteCollectionBatchSize = 128

func getRewriteCollectionBatchSize() int {
	size := config.Opt.Advanced.RewriteCollectionBatchSize
	if size <= 0 {
		return defaultRewriteCollectionBatchSize
	}
	return size
}
