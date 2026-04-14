package types

import (
	"RedisShake/internal/log"
	"io"

	"RedisShake/internal/rdb/structure"
)

type SetObject struct {
	key      string
	typeByte byte
	rd       io.Reader
	cmdC     chan RedisCmd
}

func (o *SetObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	o.typeByte = typeByte
	o.rd = rd
	o.cmdC = make(chan RedisCmd)
}

func (o *SetObject) Rewrite() <-chan RedisCmd {
	go func() {
		defer close(o.cmdC)
		o.cmdC <- RedisCmd{"del", o.key}
		switch o.typeByte {
		case rdbTypeSet:
			o.readSet()
		case rdbTypeSetIntset:
			o.readIntset()
		case rdbTypeSetListpack:
			o.readListpack()
		default:
			log.Panicf("unknown set type. typeByte=[%d]", o.typeByte)
		}
	}()
	return o.cmdC
}

func (o *SetObject) readSet() {
	rd := o.rd
	size := int(structure.ReadLength(rd))
	batchSize := getRewriteCollectionBatchSize()
	batch := make([]string, 0, batchSize)
	flush := func() {
		o.emitSAddBatch(batch)
		batch = batch[:0]
	}
	for i := 0; i < size; i++ {
		val := structure.ReadString(rd)
		batch = append(batch, val)
		if len(batch) >= batchSize {
			flush()
		}
	}
	if len(batch) > 0 {
		flush()
	}
}

func (o *SetObject) readIntset() {
	elements := structure.ReadIntset(o.rd)
	batchSize := getRewriteCollectionBatchSize()
	for i := 0; i < len(elements); i += batchSize {
		end := i + batchSize
		if end > len(elements) {
			end = len(elements)
		}
		o.emitSAddBatch(elements[i:end])
	}
}

func (o *SetObject) readListpack() {
	elements := structure.ReadListpack(o.rd)
	batchSize := getRewriteCollectionBatchSize()
	for i := 0; i < len(elements); i += batchSize {
		end := i + batchSize
		if end > len(elements) {
			end = len(elements)
		}
		o.emitSAddBatch(elements[i:end])
	}
}

func (o *SetObject) emitSAddBatch(elements []string) {
	if len(elements) == 0 {
		return
	}
	cmd := make(RedisCmd, 2+len(elements))
	cmd[0] = "sadd"
	cmd[1] = o.key
	copy(cmd[2:], elements)
	o.cmdC <- cmd
}
