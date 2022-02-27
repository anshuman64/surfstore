package surfstore

import (
	context "context"
	"errors"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	// Server Info
	ip          string
	ipList      []string
	serverId    int64
	surfClients []*RaftSurfstoreClient

	// General
	term      int64
	metaStore *MetaStore
	log       []*UpdateOperation

	// Log
	commitIndex int64
	lastApplied int64

	// Leader
	isLeader   bool
	nextIndex  []int64
	matchIndex []int64
	isLeaderMutex sync.RWMutex
	isLeaderCond  *sync.Cond

	// Chaos Monkey
	isCrashed      bool
	isCrashedMutex sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

////////////////////////
// MetaStore Functions
////////////////////////

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// Initial error checking
	if s.isCrashed {
		return nil, errors.New("isCrashed")
	}

	if !s.isLeader {
		return nil, errors.New("not isLeader")
	}

	channel := make(chan *AppendEntryOutput, len(s.ipList)-1)
	approval_count := 1

	for i := range s.ipList {
		if i != int(s.serverId) {
			go s.ApproveEntry(ctx, i, channel)
		}
	}

	for {
		output := <-channel
		if output != nil && output.Success {
			approval_count++
		}

		if approval_count > len(s.ipList)/2 {
			return s.metaStore.GetFileInfoMap(ctx, &emptypb.Empty{})
		}
	}
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	// Initial error checking
	if s.isCrashed {
		return nil, errors.New("isCrashed")
	}

	if !s.isLeader {
		return nil, errors.New("not isLeader")
	}

	channel := make(chan *AppendEntryOutput, len(s.ipList)-1)
	approval_count := 1

	for i := range s.ipList {
		if i != int(s.serverId) {
			go s.ApproveEntry(ctx, i, channel)
		}
	}

	for {
		output := <-channel
		if output != nil && output.Success {
			approval_count++
		}

		if approval_count > len(s.ipList)/2 {
			return s.metaStore.GetBlockStoreAddr(ctx, &emptypb.Empty{})
		}
	}
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// Initial error checking
	if s.isCrashed {
		return nil, errors.New("isCrashed")
	}

	if !s.isLeader {
		return nil, errors.New("not isLeader")
	}

	// Append entry to log
	update_operation := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}
	s.log = append(s.log, &update_operation)

	// Go routine to nodes for approval
	channel := make(chan *AppendEntryOutput, len(s.ipList)-1)
	approval_count := 1

	for i := range s.ipList {
		if i != int(s.serverId) {
			go s.ApproveEntry(ctx, i, channel)
		}
	}

	for {
		output := <-channel
		if output != nil && output.Success {
			approval_count++
		}

		if approval_count > len(s.ipList)/2 {
			s.lastApplied++
			s.commitIndex++
			return s.metaStore.UpdateFile(ctx, filemeta)
		}
	}
}

func (s *RaftSurfstore) ApproveEntry(ctx context.Context, serverIdx int, channel chan *AppendEntryOutput) {
	for {
		client := *s.surfClients[serverIdx]
		prev_log_index := s.nextIndex[serverIdx]-1
		entries := s.log[s.nextIndex[serverIdx]:]

		input := &AppendEntryInput{
			Term: s.term,
			PrevLogIndex: prev_log_index,
			PrevLogTerm: s.log[prev_log_index].Term,
			Entries: entries,
			LeaderCommit: s.commitIndex,
		}

		output, err := client.AppendEntries(ctx, input)
		if err != nil {
			return
		}

		if output.Success {
			channel <- output
			// If successful, update nextIndex and matchIndex
			s.nextIndex[serverIdx] += int64(len(entries))
			s.matchIndex[serverIdx] = output.MatchedIndex
			return
		} else {
			// If AppendEntries fails, decrement nextIndex and try again
			s.nextIndex[serverIdx]--
			s.ApproveEntry(ctx, serverIdx, channel)
			return
		}
	}
}

////////////////////////
// Raft Functions
////////////////////////

func (s *RaftSurfstore) StepDown() {
	s.isLeaderMutex.Lock()
	s.isLeader = false
	s.isLeaderCond.Broadcast()
	s.isLeaderMutex.Unlock()
}


// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
//    matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
//    terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//    of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {	
	// Initial error checking
	if s.isCrashed {
		return nil, errors.New("isCrashed")
	}

	// Step 0: Handle term conflict
	if s.term < input.Term {
		s.term = input.Term
		s.StepDown()
	}

	// Step 1 & 2
	if s.term > input.Term || s.log[input.PrevLogIndex].Term != input.PrevLogTerm || s.isLeader {
		output := &AppendEntryOutput {
			ServerId: s.serverId,
			Term: s.term,
			Success: false,
			MatchedIndex: s.lastApplied,
		}

		return output, nil
	}

	// Step 3
	for i := range input.Entries {
		cur_idx := int(input.PrevLogIndex)+i+1
		if len(s.log) <= cur_idx {
			break
		}

		if s.log[cur_idx].Term != input.Entries[i].Term {
			s.log = s.log[:cur_idx]
			break
		}
	}

	// Step 4
	// TODO: check if entries already in log?
	s.log = append(s.log, input.Entries...)

	// Step 5
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(Min(int(input.LeaderCommit), int(input.PrevLogIndex) + len(input.Entries)))

		// Apply log
		for s.commitIndex > s.lastApplied {
			s.lastApplied++
			s.metaStore.UpdateFile(ctx, s.log[s.lastApplied].FileMetaData)
		}
	}

	output := &AppendEntryOutput {
		ServerId: s.serverId,
		Term: s.term,
		Success: true,
		MatchedIndex: s.lastApplied,
	}

	return output, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// Check if isCrashed
	if s.isCrashed {
		return &Success{Flag: false}, errors.New("isCrashed") 
	}

	s.isLeaderMutex.Lock()

	// Increase term
	s.term++

	// Set as leader
	s.isLeader = true

	// Initialize leader variables
	for i := range s.ipList {
		s.nextIndex[i] = int64(len(s.log))
		s.matchIndex[i] = 0
	}

	// Send heartbeat
	// s.SendHeartbeat(ctx, &emptypb.Empty{})

	s.isLeaderMutex.Unlock()

	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// Check if isCrashed
	if s.isCrashed {
		return &Success{Flag: false}, errors.New("isCrashed") 
	}

	// Check if not isLeader
	if !s.isLeader {
		return &Success{Flag: false}, nil
	}

	// Update commitIndex
	for {
		N := s.commitIndex + 1
		if int64(len(s.log)) <= N {
			break
		}

		match_count := 1
		for i := range s.ipList {
			if s.matchIndex[i] >= N {
				match_count++
			}
		}

		if match_count > len(s.ipList)/2 && s.log[N].Term == s.term {
			s.commitIndex = N
		} else {
			break
		}
	}
	
	// Send heartbeat
	channel := make(chan *AppendEntryOutput, len(s.ipList)-1)
	for i := range s.ipList {
		if i != int(s.serverId) {
			go s.ApproveEntry(ctx, i, channel)
		}
	}

	return &Success{Flag: true}, nil
}

////////////////////////
// Chaos Monkey Functions
////////////////////////

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	return &CrashedState{IsCrashed: s.isCrashed}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
