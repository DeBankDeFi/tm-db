package db

import (
	"fmt"

	protodb "github.com/tendermint/tm-db/proto"
)

type batch struct {
	db  *RemoteDB
	ops []*protodb.Operation
	id  int32
}

var _ Batch = (*batch)(nil)

func newBatch(rdb *RemoteDB) *batch {
	return &batch{
		db:  rdb,
		ops: []*protodb.Operation{},
	}
}

// Set implements Batch.
func (b *batch) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if b.ops == nil {
		return errBatchClosed
	}
	if ReadOnly {
		return nil
	}
	op := &protodb.Operation{
		Entity: &protodb.Entity{Key: key, Value: value},
		Type:   protodb.Operation_SET,
	}
	b.ops = append(b.ops, op)
	return nil
}

// Delete implements Batch.
func (b *batch) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if b.ops == nil {
		return errBatchClosed
	}
	if ReadOnly {
		return nil
	}
	op := &protodb.Operation{
		Entity: &protodb.Entity{Key: key},
		Type:   protodb.Operation_DELETE,
	}
	b.ops = append(b.ops, op)
	return nil
}

// Write implements Batch.
func (b *batch) Write() error {
	if b.ops == nil {
		return errBatchClosed
	}
	if ReadOnly {
		return nil
	}
	_, err := b.db.dc.BatchWrite(b.db.ctx, &protodb.Batch{Id: b.id, Ops: b.ops})
	if err != nil {
		return fmt.Errorf("remoteDB.BatchWrite: %w", err)
	}
	// Make sure batch cannot be used afterwards. Callers should still call Close(), for errors.
	b.Close()
	return nil
}

// WriteSync implements Batch.
func (b *batch) WriteSync() error {
	if b.ops == nil {
		return errBatchClosed
	}
	if ReadOnly {
		return nil
	}
	_, err := b.db.dc.BatchWriteSync(b.db.ctx, &protodb.Batch{Id: b.id, Ops: b.ops})
	if err != nil {
		return fmt.Errorf("RemoteDB.BatchWriteSync: %w", err)
	}
	// Make sure batch cannot be used afterwards. Callers should still call Close(), for errors.
	return b.Close()
}

// Close implements Batch.
func (b *batch) Close() error {
	b.ops = nil
	return nil
}
