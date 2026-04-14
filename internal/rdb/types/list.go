package types

import (
	"io"

	"RedisShake/internal/log"
	"RedisShake/internal/rdb/structure"
)

// quicklist node container formats
const (
	quicklistNodeContainerPlain  = 1 // QUICKLIST_NODE_CONTAINER_PLAIN
	quicklistNodeContainerPacked = 2 // QUICKLIST_NODE_CONTAINER_PACKED
)

type ListObject struct {
	key      string
	typeByte byte
	rd       io.Reader
	cmdC     chan RedisCmd
}

func (o *ListObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	o.typeByte = typeByte
	o.rd = rd
	o.cmdC = make(chan RedisCmd)
}

func (o *ListObject) Rewrite() <-chan RedisCmd {
	go func() {
		defer close(o.cmdC)
		o.cmdC <- RedisCmd{"del", o.key}
		switch o.typeByte {
		case rdbTypeList:
			o.readList()
		case rdbTypeListZiplist:
			o.readZipList()
		case rdbTypeListQuicklist:
			o.readQuickList()
		case rdbTypeListQuicklist2:
			o.readQuickList2()
		default:
			log.Panicf("unknown list type %d", o.typeByte)
		}
	}()
	return o.cmdC
}

func (o *ListObject) readList() {
	rd := o.rd
	size := int(structure.ReadLength(rd))
	batchSize := getRewriteCollectionBatchSize()
	batch := make([]string, 0, batchSize)
	flush := func() {
		o.emitRPushBatch(batch)
		batch = batch[:0]
	}
	for i := 0; i < size; i++ {
		ele := structure.ReadString(rd)
		batch = append(batch, ele)
		if len(batch) >= batchSize {
			flush()
		}
	}
	if len(batch) > 0 {
		flush()
	}
}

func (o *ListObject) readZipList() {
	rd := o.rd
	elements := structure.ReadZipList(rd)
	batchSize := getRewriteCollectionBatchSize()
	for i := 0; i < len(elements); i += batchSize {
		end := i + batchSize
		if end > len(elements) {
			end = len(elements)
		}
		o.emitRPushBatch(elements[i:end])
	}
}

func (o *ListObject) readQuickList() {
	rd := o.rd
	size := int(structure.ReadLength(rd))
	batchSize := getRewriteCollectionBatchSize()
	batch := make([]string, 0, batchSize)
	flush := func() {
		o.emitRPushBatch(batch)
		batch = batch[:0]
	}
	for i := 0; i < size; i++ {
		ziplistElements := structure.ReadZipList(rd)
		for _, ele := range ziplistElements {
			batch = append(batch, ele)
			if len(batch) >= batchSize {
				flush()
			}
		}
	}
	if len(batch) > 0 {
		flush()
	}
}

func (o *ListObject) readQuickList2() {
	rd := o.rd
	size := int(structure.ReadLength(rd))
	batchSize := getRewriteCollectionBatchSize()
	batch := make([]string, 0, batchSize)
	flush := func() {
		o.emitRPushBatch(batch)
		batch = batch[:0]
	}
	for i := 0; i < size; i++ {
		container := structure.ReadLength(rd)
		if container == quicklistNodeContainerPlain {
			ele := structure.ReadString(rd)
			batch = append(batch, ele)
			if len(batch) >= batchSize {
				flush()
			}
		} else if container == quicklistNodeContainerPacked {
			listpackElements := structure.ReadListpack(rd)
			for _, ele := range listpackElements {
				batch = append(batch, ele)
				if len(batch) >= batchSize {
					flush()
				}
			}
		} else {
			log.Panicf("unknown quicklist container %d", container)
		}
	}
	if len(batch) > 0 {
		flush()
	}
}

func (o *ListObject) emitRPushBatch(elements []string) {
	if len(elements) == 0 {
		return
	}
	cmd := make(RedisCmd, 2+len(elements))
	cmd[0] = "rpush"
	cmd[1] = o.key
	copy(cmd[2:], elements)
	o.cmdC <- cmd
}
