package db

import (
	"context"
	"errors"
	"fmt"
	"math"

	protodb "github.com/tendermint/tm-db/proto"
	"google.golang.org/grpc"
)

var RemoteDBAddr = ""

type RemoteDB struct {
	ctx context.Context
	dc  protodb.DBClient
	id  int32
}

// NewClient creates a gRPC client connected to the bound gRPC server at serverAddr.
// Use kind to set the level of security to either Secure or Insecure.
func NewClient(serverAddr string) (protodb.DBClient, error) {
	diaOpt := grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32), grpc.MaxCallSendMsgSize(math.MaxInt32))
	cc, err := grpc.Dial(serverAddr, grpc.WithInsecure(), diaOpt)
	if err != nil {
		return nil, err
	}
	return protodb.NewDBClient(cc), nil
}

func NewRemoteDB(serverAddr string) (*RemoteDB, error) {
	return newRemoteDB(NewClient(serverAddr))
}

func newRemoteDB(gdc protodb.DBClient, err error) (*RemoteDB, error) {
	if err != nil {
		return nil, err
	}
	return &RemoteDB{dc: gdc, ctx: context.Background()}, nil
}

type Init struct {
	Dir  string
	Name string
	Type string
}

func (rd *RemoteDB) InitRemote(in *Init) error {
	entry, err := rd.dc.Init(rd.ctx, &protodb.Init{Dir: in.Dir, Type: in.Type, Name: in.Name})
	if err != nil {
		return err
	}
	rd.id = entry.Id
	return err
}

var _ DB = (*RemoteDB)(nil)

// Close is a noop currently
func (rd *RemoteDB) Close() error {
	return nil
}

func (rd *RemoteDB) Delete(key []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if _, err := rd.dc.Delete(rd.ctx, &protodb.Entity{Id: rd.id, Key: key}); err != nil {
		return fmt.Errorf("remoteDB.Delete: %w", err)
	}
	return nil
}

func (rd *RemoteDB) DeleteSync(key []byte) error {
	if _, err := rd.dc.DeleteSync(rd.ctx, &protodb.Entity{Id: rd.id, Key: key}); err != nil {
		return fmt.Errorf("remoteDB.DeleteSync: %w", err)
	}
	return nil
}

func (rd *RemoteDB) Set(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if _, err := rd.dc.Set(rd.ctx, &protodb.Entity{Id: rd.id, Key: key, Value: value}); err != nil {
		return fmt.Errorf("remoteDB.Set: %w", err)
	}
	return nil
}

func (rd *RemoteDB) SetSync(key, value []byte) error {
	if len(key) == 0 {
		return errKeyEmpty
	}
	if value == nil {
		return errValueNil
	}
	if _, err := rd.dc.SetSync(rd.ctx, &protodb.Entity{Id: rd.id, Key: key, Value: value}); err != nil {
		return fmt.Errorf("remoteDB.SetSync: %w", err)
	}
	return nil
}

func (rd *RemoteDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errKeyEmpty
	}
	res, err := rd.dc.Get(rd.ctx, &protodb.Entity{Id: rd.id, Key: key})
	if err != nil {
		return nil, fmt.Errorf("remoteDB.Get error: %w", err)
	}
	if res.Exists {
		if res == nil {
			res.Value = []byte{}
		}
	}
	return res.Value, nil
}

func (rd *RemoteDB) Has(key []byte) (bool, error) {
	res, err := rd.dc.Has(rd.ctx, &protodb.Entity{Id: rd.id, Key: key})
	if err != nil {
		return false, err
	}
	return res.Exists, nil
}

func (rd *RemoteDB) ReverseIterator(start, end []byte) (Iterator, error) {
	dic, err := rd.dc.ReverseIterator(rd.ctx, &protodb.Entity{Id: rd.id, Start: start, End: end})
	if err != nil {
		return nil, fmt.Errorf("RemoteDB.Iterator error: %w", err)
	}
	return makeReverseIterator(dic), nil
}

func (rd *RemoteDB) NewBatch() Batch {
	return newBatch(rd)
}

// TODO: Implement Print when db.DB implements a method
// to print to a string and not db.Print to stdout.
func (rd *RemoteDB) Print() error {
	return errors.New("remoteDB.Print: unimplemented")
}

func (rd *RemoteDB) Stats() map[string]string {
	stats, err := rd.dc.Stats(rd.ctx, &protodb.Entity{Id: rd.id})
	if err != nil || stats == nil {
		return nil
	}
	return stats.Data
}

func (rd *RemoteDB) Iterator(start, end []byte) (Iterator, error) {
	fmt.Printf("RemoteDB.Iterator: from id %v",rd.id)
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, errKeyEmpty
	}
	dic, err := rd.dc.Iterator(rd.ctx, &protodb.Entity{Id: rd.id, Start: start, End: end})
	if err != nil {
		return nil, fmt.Errorf("RemoteDB.Iterator error: %w", err)
	}
	return makeIterator(dic), nil
}
