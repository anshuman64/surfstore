package surfstore

import (
	"errors"
	"log"
	"os"
	"reflect"
	"strings"
)

var current_index = make(map[string]*FileMetaData)
var local_index = make(map[string]*FileMetaData)
var remote_index = make(map[string]*FileMetaData)

var blockStoreAddr string

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// Load local_index
	if err := CreateLocalIndexFile(client.BaseDir); err != nil {
		log.Fatal(err)
	}
	local_index, _ = LoadMetaFromMetaFile(client.BaseDir)

	// Create current_index
	if err := CreateCurrentIndex(client); err != nil {
		log.Fatal(err)
	}

	// Update current_index with changed files from local_index
	CompareCurrentLocalIndex()

	// Get remote_index & blockStoreAddr
	if err := client.GetFileInfoMap(&remote_index); err != nil {
		log.Fatal(err)
	}
	if err := client.GetBlockStoreAddr(&blockStoreAddr); err != nil {
		log.Fatal(err)
	}

	// Compare remote_index with local_index
	if err := CompareRemoteLocalIndex(client); err != nil {
		log.Fatal(err)
	}

	// Compare current_index with remote_index
	if err := CompareCurrentRemoteIndex(client); err != nil {
		log.Fatal(err)
	}

	if err := WriteMetaFile(local_index, client.BaseDir); err != nil {
		log.Fatal(err)
	}
}

//////////////////////
// Helper Functions
//////////////////////

func Min(x, y int) int {
	if x > y {
		return y
	}

	return x
}

func IsEqual(hashList1 []string, hashList2 []string) bool {
	if len(hashList1) == 0 && len(hashList2) == 0 {
		return true
	}

	return reflect.DeepEqual(hashList1, hashList2)
}

func GetTombstoneHashList() []string {
	return []string{"0"}
}

// Gets BlockHashList of file
func GetBlockHashList(client RPCClient, file_name string) (blockHashList []string, err error) {
	file_contents, err := os.ReadFile(ConcatPath(client.BaseDir, file_name))
	if err != nil {
		return nil, err
	}

	if len(file_contents) == 0 {
		return nil, nil
	}

	for i := 0; i < len(file_contents); i += client.BlockSize {
		block := file_contents[i:Min(i+client.BlockSize, len(file_contents))]
		blockHash := GetBlockHashString(block)
		blockHashList = append(blockHashList, blockHash)
	}

	return blockHashList, nil
}

//////////////////////
// Update File
//////////////////////

func UpdateFile(client RPCClient, remote_meta *FileMetaData) error {
	if IsEqual(remote_meta.BlockHashList, GetTombstoneHashList()) {
		// File deleted
		log.Println("Delete File")
		if err := DeleteFile(client, remote_meta); err != nil {
			return err
		}
	} else {
		// Download file
		log.Println("Download File")
		if err := DownloadFile(client, remote_meta); err != nil {
			return err
		}
	}

	// Update local_index
	local_index[remote_meta.Filename] = remote_meta

	return nil
}

// Downloads blocks & writes to file on disk
func DownloadFile(client RPCClient, remote_meta *FileMetaData) error {
	file_contents := make([]byte, 0)

	for _, blockHash := range remote_meta.BlockHashList {
		block := &Block{}
		client.GetBlock(blockHash, blockStoreAddr, block)
		file_contents = append(file_contents, block.BlockData...)
	}

	if err := os.WriteFile(ConcatPath(client.BaseDir, remote_meta.Filename), file_contents, 0644); err != nil {
		return err
	}

	return nil
}

// Delete file in local directory
func DeleteFile(client RPCClient, remote_meta *FileMetaData) error {
	file_path := ConcatPath(client.BaseDir, remote_meta.Filename)

	if _, err := os.Stat(file_path); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else {
		if err := os.Remove(file_path); err != nil {
			return err
		}
	}

	return nil
}

//////////////////////
// Update Blocks
//////////////////////

func UploadBlocks(client RPCClient, current_meta *FileMetaData) error {
	file_contents, err := os.ReadFile(ConcatPath(client.BaseDir, current_meta.Filename))
	if err != nil {
		return err
	}

	if len(file_contents) == 0 {
		return nil
	}

	has_block_hashes := make([]string, 0)
	client.HasBlocks(current_meta.BlockHashList, blockStoreAddr, &has_block_hashes)

	for i := 0; i < len(file_contents); i += client.BlockSize {
		block_data := file_contents[i:Min(i+client.BlockSize, len(file_contents))]

		if HasBlock(block_data, has_block_hashes) {
			continue
		}

		block := &Block{
			BlockData: block_data,
			BlockSize: int32(len(block_data)),
		}

		var success *bool
		if err := client.PutBlock(block, blockStoreAddr, success); err != nil {
			return err
		}
	}

	return nil
}

func HasBlock(block_data []byte, has_block_hashes []string) bool {
	block_hash := GetBlockHashString(block_data)

	for _, has_block_hash := range has_block_hashes {
		if block_hash == has_block_hash {
			return true
		}
	}

	return false
}

//////////////////////
// Create Index
//////////////////////

// Creates local_index if it does not exist
func CreateLocalIndexFile(baseDir string) error {
	index_path := ConcatPath(baseDir, DEFAULT_META_FILENAME)

	if _, err := os.Stat(index_path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log.Println("Create index.txt")
			if _, err = os.Create(index_path); err != nil {
				return errors.New("could not create local index")
			}
		} else {
			return errors.New("could not open local index")
		}
	}

	return nil
}

// Creates current_index by looping through files in directory
func CreateCurrentIndex(client RPCClient) error {
	files, err := os.ReadDir(client.BaseDir)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() || file.Name() == DEFAULT_META_FILENAME || strings.Contains(file.Name(), "/") || strings.Contains(file.Name(), ",") {
			continue
		}

		blockHashList, err := GetBlockHashList(client, file.Name())
		if err != nil {
			return err
		}

		current_index[file.Name()] = &FileMetaData{
			Filename:      file.Name(),
			Version:       1,
			BlockHashList: blockHashList,
		}
	}

	return nil
}

//////////////////////
// Compare Indices
//////////////////////

// Compares current_index with local_index to keep only changed files
func CompareCurrentLocalIndex() {
	// Check for deleted files
	for key, local_meta := range local_index {
		// If local_meta not in current_index, create tombstone record
		if _, ok := current_index[key]; !ok && !IsEqual(local_meta.BlockHashList, GetTombstoneHashList()) {
			log.Println(key + ": deleted file")
			current_index[key] = &FileMetaData{
				Filename:      local_meta.Filename,
				Version:       local_meta.Version + 1,
				BlockHashList: GetTombstoneHashList(),
			}
		}
	}

	// Only keep added/changed files
	for key, current_meta := range current_index {
		local_meta, ok := local_index[key]
		if ok {
			if IsEqual(current_meta.BlockHashList, local_meta.BlockHashList) {
				// If no change, delete key from current_index
				log.Println(key + ": no change")
				delete(current_index, key)
			} else {
				// If changed, increase current_index version
				log.Println(key + ": changed")
				current_index[key].Version = local_meta.Version + 1
			}
		} else {
			// If new file, set current_index version = 1
			log.Println(key + ": new file")
			current_index[key].Version = 1
		}
	}
}

func CompareRemoteLocalIndex(client RPCClient) error {
	for key, remote_meta := range remote_index {
		local_meta, ok := local_index[key]
		if !ok || !IsEqual(local_meta.BlockHashList, remote_meta.BlockHashList) {
			log.Println(key + ": download file")
			if err := UpdateFile(client, remote_meta); err != nil {
				return err
			}

			// Remove key from current_index
			delete(current_index, key)
		}
	}

	return nil
}

func CompareCurrentRemoteIndex(client RPCClient) error {
	for key, current_meta := range current_index {
		// Upload blocks
		UploadBlocks(client, current_meta)

		var version int32
		// Try to update remote_index
		log.Println(key + ": upload file")
		if err := client.UpdateFile(current_meta, &version); err != nil {
			// If fails, download latest version of file
			if err := client.GetFileInfoMap(&remote_index); err != nil {
				return err
			}
			if err := UpdateFile(client, remote_index[key]); err != nil {
				return err
			}
		} else {
			// If successful, update local_index
			local_index[key] = current_meta
		}
	}

	return nil
}
