package surfstore

import (
	context "context"
	"errors"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (blockstore *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	block, ok := blockstore.BlockMap[blockHash.Hash]
	if !ok {
		return &Block{}, errors.New("blockHash not found in BlockStore")
	}

	return block, nil
}

func (blockstore *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	blockHash := GetBlockHashString(block.BlockData)

	if _, ok := blockstore.BlockMap[blockHash]; ok {
		return &Success{Flag: false}, errors.New("hash collision in BlockStore")
	}

	blockstore.BlockMap[blockHash] = block

	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (blockstore *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	hashes := make([]string, 0)
	for _, blockHash := range blockHashesIn.Hashes {
		if _, ok := blockstore.BlockMap[blockHash]; ok {
			hashes = append(hashes, blockHash)
		}
	}

	return &BlockHashes{Hashes: hashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
