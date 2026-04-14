package types

import (
	"fmt"
	"io"

	"RedisShake/internal/log"
	"RedisShake/internal/rdb/structure"
)

type ZsetObject struct {
	key      string
	typeByte byte
	rd       io.Reader
	cmdC     chan RedisCmd
}

func (o *ZsetObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	o.typeByte = typeByte
	o.rd = rd
	o.cmdC = make(chan RedisCmd)
}

func (o *ZsetObject) Rewrite() <-chan RedisCmd {
	go func() {
		defer close(o.cmdC)
		o.cmdC <- RedisCmd{"del", o.key}
		switch o.typeByte {
		case rdbTypeZSet:
			o.readZset()
		case rdbTypeZSet2:
			o.readZset2()
		case rdbTypeZSetZiplist:
			o.readZsetZiplist()
		case rdbTypeZSetListpack:
			o.readZsetListpack()
		default:
			log.Panicf("unknown zset type. typeByte=[%d]", o.typeByte)
		}
	}()
	return o.cmdC
}

func (o *ZsetObject) readZset() {
	rd := o.rd
	size := int(structure.ReadLength(rd))
	batchSize := getRewriteCollectionBatchSize()
	pairs := make([]string, 0, batchSize*2)
	flush := func() {
		o.emitZAddBatch(pairs)
		pairs = pairs[:0]
	}
	for i := 0; i < size; i++ {
		member := structure.ReadString(rd)
		score := structure.ReadFloat(rd)
		pairs = append(pairs, fmt.Sprintf("%.17g", score), member)
		if len(pairs) >= batchSize*2 {
			flush()
		}
	}
	if len(pairs) > 0 {
		flush()
	}
}

func (o *ZsetObject) readZset2() {
	rd := o.rd
	size := int(structure.ReadLength(rd))
	batchSize := getRewriteCollectionBatchSize()
	pairs := make([]string, 0, batchSize*2)
	flush := func() {
		o.emitZAddBatch(pairs)
		pairs = pairs[:0]
	}
	for i := 0; i < size; i++ {
		member := structure.ReadString(rd)
		score := structure.ReadDouble(rd)
		pairs = append(pairs, fmt.Sprintf("%.17g", score), member)
		if len(pairs) >= batchSize*2 {
			flush()
		}
	}
	if len(pairs) > 0 {
		flush()
	}
}

func (o *ZsetObject) readZsetZiplist() {
	rd := o.rd
	list := structure.ReadZipList(rd)
	size := len(list)
	if size%2 != 0 {
		log.Panicf("zset listpack size is not even. size=[%d]", size)
	}
	batchSize := getRewriteCollectionBatchSize()
	for i := 0; i < size; i += batchSize * 2 {
		end := i + batchSize*2
		if end > size {
			end = size
		}
		pairs := make([]string, 0, end-i)
		for j := i; j < end; j += 2 {
			pairs = append(pairs, list[j+1], list[j])
		}
		o.emitZAddBatch(pairs)
	}
}

func (o *ZsetObject) readZsetListpack() {
	rd := o.rd
	list := structure.ReadListpack(rd)
	size := len(list)
	if size%2 != 0 {
		log.Panicf("zset listpack size is not even. size=[%d]", size)
	}
	batchSize := getRewriteCollectionBatchSize()
	for i := 0; i < size; i += batchSize * 2 {
		end := i + batchSize*2
		if end > size {
			end = size
		}
		pairs := make([]string, 0, end-i)
		for j := i; j < end; j += 2 {
			pairs = append(pairs, list[j+1], list[j])
		}
		o.emitZAddBatch(pairs)
	}
}

func (o *ZsetObject) emitZAddBatch(scoreMembers []string) {
	if len(scoreMembers) == 0 {
		return
	}
	cmd := make(RedisCmd, 2+len(scoreMembers))
	cmd[0] = "zadd"
	cmd[1] = o.key
	copy(cmd[2:], scoreMembers)
	o.cmdC <- cmd
}
