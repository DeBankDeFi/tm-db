package grpcdb

import (
	"google.golang.org/grpc"

	protodb "github.com/tendermint/tm-db/remotedb/proto"
)

// NewClient creates a gRPC client connected to the bound gRPC server at serverAddr.
// Use kind to set the level of security to either Secure or Insecure.
func NewClient(serverAddr string) (protodb.DBClient, error) {
	cc, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return protodb.NewDBClient(cc), nil
}
