package surfstore

import (
	context "context"
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

func (surfClient *RPCClient) StartBlockStoreClient(blockStoreAddr string) (conn *grpc.ClientConn, blockstore_client BlockStoreClient, ctx context.Context, cancel context.CancelFunc, err error) {
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

func (surfClient *RPCClient) StartMetaStoreClient() (conn *grpc.ClientConn, metastore_client MetaStoreClient, ctx context.Context, cancel context.CancelFunc, err error) {
	// Connect to server
	// TODO: fix this
	conn, err = grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, nil, nil, err
	}
	metastore_client = NewRaftSurfstoreClient(conn)

	// Perform RPC call
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)

	return conn, metastore_client, ctx, cancel, nil
}

///////////////////
// Main Functions
///////////////////

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	conn, blockstore_client, ctx, cancel, err := surfClient.StartBlockStoreClient(blockStoreAddr)
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
	conn, blockstore_client, ctx, cancel, err := surfClient.StartBlockStoreClient(blockStoreAddr)
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
	conn, blockstore_client, ctx, cancel, err := surfClient.StartBlockStoreClient(blockStoreAddr)
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

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	conn, metastore_client, ctx, cancel, err := surfClient.StartMetaStoreClient()
	if err != nil {
		return err
	}
	defer cancel()

	f, err := metastore_client.GetFileInfoMap(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*serverFileInfoMap = f.FileInfoMap

	return conn.Close()
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	conn, metastore_client, ctx, cancel, err := surfClient.StartMetaStoreClient()
	if err != nil {
		return err
	}
	defer cancel()

	v, err := metastore_client.UpdateFile(ctx, fileMetaData)
	if err != nil {
		conn.Close()
		return err
	}
	*latestVersion = v.Version

	return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	conn, metastore_client, ctx, cancel, err := surfClient.StartMetaStoreClient()
	if err != nil {
		return err
	}
	defer cancel()

	b, err := metastore_client.GetBlockStoreAddr(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*blockStoreAddr = b.Addr

	return conn.Close()
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
