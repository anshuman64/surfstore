package surfstore

import (
	context "context"
	"errors"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

/////////////////////
// Helper Functions
/////////////////////

func StartBlockStoreClient(blockStoreAddr string) (conn *grpc.ClientConn, blockstore_client BlockStoreClient, ctx context.Context, cancel context.CancelFunc, err error) {
	// Connect to server
	conn, err = grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, nil, nil, err
	}
	blockstore_client = NewBlockStoreClient(conn)

	// Perform RPC call
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)

	return conn, blockstore_client, ctx, cancel, nil
}

func StartRaftSurfstoreClient(addr string) (conn *grpc.ClientConn, raft_surfstore_client RaftSurfstoreClient, ctx context.Context, cancel context.CancelFunc, err error) {
	// Connect to server
	conn, err = grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, nil, nil, err
	}
	raft_surfstore_client = NewRaftSurfstoreClient(conn)

	// Perform RPC call
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)

	return conn, raft_surfstore_client, ctx, cancel, nil
}

///////////////////
// Blockstore Functions
///////////////////

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	conn, blockstore_client, ctx, cancel, err := StartBlockStoreClient(blockStoreAddr)
	if err != nil {
		return err
	}
	defer cancel()

	b, err := blockstore_client.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, metaStoreAddr string, succ *bool) error {
	conn, blockstore_client, ctx, cancel, err := StartBlockStoreClient(blockStoreAddr)
	if err != nil {
		return err
	}
	defer cancel()

	_, err = blockstore_client.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	// *succ = s.Flag

	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	conn, blockstore_client, ctx, cancel, err := StartBlockStoreClient(blockStoreAddr)
	if err != nil {
		return err
	}
	defer cancel()

	b, err := blockstore_client.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = b.Hashes

	return conn.Close()
}

///////////////////
// RaftSurfstore Functions
///////////////////

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, raft_surfstore_client, ctx, cancel, err := StartRaftSurfstoreClient(addr)
		if err != nil {
			continue
		}
		defer cancel()

		f, err := raft_surfstore_client.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			conn.Close()
			continue
		}
		*serverFileInfoMap = f.FileInfoMap

		return conn.Close()
	}

	return errors.New("couldn't connect to any server")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, raft_surfstore_client, ctx, cancel, err := StartRaftSurfstoreClient(addr)
		if err != nil {
			continue
		}
		defer cancel()

		v, err := raft_surfstore_client.UpdateFile(ctx, fileMetaData)
		if err != nil {
			conn.Close()
			continue
		}
		*latestVersion = v.Version

		return conn.Close()
	}

	return errors.New("couldn't connect to any server")
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, raft_surfstore_client, ctx, cancel, err := StartRaftSurfstoreClient(addr)
		if err != nil {
			continue
		}
		defer cancel()

		b, err := raft_surfstore_client.GetBlockStoreAddr(ctx, &emptypb.Empty{})
		if err != nil {
			conn.Close()
			continue
		}
		*blockStoreAddr = b.Addr

		return conn.Close()
	}

	return errors.New("couldn't connect to any server")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create a Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
