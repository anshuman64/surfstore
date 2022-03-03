package surfstore

import (
	context "context"
	"sync"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	// Server Info
	ip       string
	ipList   []string
	serverId int64

	// General
	term      int64
	metaStore *MetaStore
	log       []*UpdateOperation

	// Log
	commitIndex int64
	lastApplied int64

	// Leader
	isLeader      bool
	nextIndex     []int64
	matchIndex    []int64
	isLeaderMutex *sync.RWMutex
	isLeaderCond  *sync.Cond

	// Chaos Monkey
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

////////////////////////
// MetaStore Functions
////////////////////////

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// Initial error checking
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}

	approval_chan := make(chan *AppendEntryOutput, len(s.ipList)-1)
	approval_count := 1
	return_count := 1

	for i := range s.ipList {
		if i != int(s.serverId) {
			go s.ApproveEntry(i, approval_chan)
		}
	}

	for {
		output := <-approval_chan
		return_count += 1

		if output != nil && output.Success {
			approval_count += 1
		}

		if return_count >= len(s.ipList) && approval_count > len(s.ipList)/2 {
			return s.metaStore.GetFileInfoMap(ctx, &emptypb.Empty{})
		}
	}
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	// Initial error checking
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}

	approval_chan := make(chan *AppendEntryOutput, len(s.ipList)-1)
	approval_count := 1
	return_count := 1

	for i := range s.ipList {
		if i != int(s.serverId) {
			go s.ApproveEntry(i, approval_chan)
		}
	}

	for {
		output := <-approval_chan
		return_count += 1

		if output != nil && output.Success {
			approval_count += 1
		}

		if return_count >= len(s.ipList) && approval_count > len(s.ipList)/2 {
			return s.metaStore.GetBlockStoreAddr(ctx, &emptypb.Empty{})
		}
	}
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	//log.Printf("UpdateFile %dA. Term=%d.", s.serverId, s.term)

	// Initial error checking
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}

	// Append entry to log
	update_operation := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}
	s.log = append(s.log, &update_operation)

	approval_chan := make(chan *AppendEntryOutput, len(s.ipList)-1)
	approval_count := 1
	return_count := 1

	//log.Printf("UpdateFile %dB. Term=%d.", s.serverId, s.term)

	for i := range s.ipList {
		if i != int(s.serverId) {
			go s.ApproveEntry(i, approval_chan)
		}
	}

	for {
		output := <-approval_chan
		return_count += 1

		if output != nil && output.Success {
			approval_count += 1
		}

		if return_count >= len(s.ipList) && approval_count > len(s.ipList)/2 {
			//log.Printf("UpdateFile %dE. Term=%d.", s.serverId, s.term)
			s.lastApplied += 1
			s.commitIndex += 1

			// Send heartbeat
			s.SendHeartbeat(ctx, &emptypb.Empty{})

			return s.metaStore.UpdateFile(ctx, filemeta)
		}
	}
}

////////////////////////
// ApproveEntry & AppendEntries
////////////////////////

func (s *RaftSurfstore) ApproveEntry(serverIdx int, approval_chan chan *AppendEntryOutput) error {
	//log.Printf("ApproveEntry %dA. Term=%d.", s.serverId, s.term)

	// Connect to client
	var conn *grpc.ClientConn
	var raft_surfstore_client RaftSurfstoreClient
	var ctx context.Context
	var cancel context.CancelFunc
	var err error

	for {
		conn, raft_surfstore_client, ctx, cancel, err = StartRaftSurfstoreClient(s.ipList[serverIdx])
		if err == nil {
			break
		}
	}

	defer cancel()

	// Initialize AppendEntryInput
	prev_log_index := s.nextIndex[serverIdx] - 1
	prev_log_term := int64(0)
	entries := make([]*UpdateOperation, 0)

	if len(s.log) > 0 {
		//log.Printf("ApproveEntry %dB. Term=%d.", s.serverId, s.term)
		entries = s.log[s.nextIndex[serverIdx]:]
	}

	if prev_log_index >= 0 {
		//log.Printf("ApproveEntry %dC. Term=%d.", s.serverId, s.term)
		prev_log_term = s.log[prev_log_index].Term
	}

	input := &AppendEntryInput{
		Term:         s.term,
		PrevLogIndex: prev_log_index,
		PrevLogTerm:  prev_log_term,
		Entries:      entries,
		LeaderCommit: s.commitIndex,
	}

	output, err := raft_surfstore_client.AppendEntries(ctx, input)
	if err != nil {
		//log.Printf("ApproveEntry %dD. Term=%d.", s.serverId, s.term)

		// Server was crashed
		approval_chan <- nil
		return conn.Close()
	}

	if output.Success {
		//log.Printf("ApproveEntry %dE. Term=%d.", s.serverId, s.term)

		// If successful, update nextIndex and matchIndex
		approval_chan <- output
		s.nextIndex[serverIdx] += int64(len(entries))
		s.matchIndex[serverIdx] = output.MatchedIndex
		return conn.Close()
	} else {
		//log.Printf("ApproveEntry %dF. Term=%d.", s.serverId, s.term)

		// If AppendEntries fails, decrement nextIndex and try again
		s.nextIndex[serverIdx]--
		s.ApproveEntry(serverIdx, approval_chan)
		return conn.Close()
	}
}

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
	//log.Printf("AppendEntries %dA. Term=%d. Input Term=%d", s.serverId, s.term, input.Term)

	// Initial error checking
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	// Step 0: Handle term conflict
	if s.term < input.Term {
		//log.Printf("AppendEntries %dB. Term=%d. Input Term=%d", s.serverId, s.term, input.Term)
		s.term = input.Term
		s.StepDown()
	}

	//log.Printf("AppendEntries %dC. Term=%d. Input Term=%d", s.serverId, s.term, input.Term)

	// Step 1 & 2
	if s.term > input.Term || s.isLeader || input.PrevLogIndex >= int64(len(s.log)) || (input.PrevLogIndex >= 0 && s.log[input.PrevLogIndex].Term != input.PrevLogTerm) {
		//log.Printf("AppendEntries %dD. Term=%d. Input Term=%d", s.serverId, s.term, input.Term)
		output := &AppendEntryOutput{
			ServerId:     s.serverId,
			Term:         s.term,
			Success:      false,
			MatchedIndex: s.lastApplied,
		}

		return output, nil
	}

	//log.Printf("AppendEntries %dE. Term=%d. Input Term=%d", s.serverId, s.term, input.Term)

	// Step 3
	for i := range input.Entries {
		cur_idx := int(input.PrevLogIndex) + i + 1
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
		s.commitIndex = int64(Min(int(input.LeaderCommit), int(input.PrevLogIndex)+len(input.Entries)))

		// Apply log
		for s.commitIndex > s.lastApplied {
			s.lastApplied += 1
			s.metaStore.UpdateFile(ctx, s.log[s.lastApplied].FileMetaData)
		}
	}

	output := &AppendEntryOutput{
		ServerId:     s.serverId,
		Term:         s.term,
		Success:      true,
		MatchedIndex: s.lastApplied,
	}

	return output, nil
}

////////////////////////
// SetLeader & SendHeartbeat
////////////////////////

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// Check if isCrashed
	if s.isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.Lock()

	// Increase term
	s.term += 1

	// Set as leader
	s.isLeader = true

	// Initialize leader variables
	for i := range s.ipList {
		s.nextIndex[i] = int64(len(s.log))
		s.matchIndex[i] = 0
	}

	s.isLeaderMutex.Unlock()

	// Send heartbeat
	s.SendHeartbeat(ctx, &emptypb.Empty{})

	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	//log.Printf("SendHeartbeat %dA. Term=%d", s.serverId, s.term)

	// Check if isCrashed
	if s.isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	// Check if not isLeader
	if !s.isLeader {
		return &Success{Flag: false}, nil
	}

	//log.Printf("SendHeartbeat %dB. Term=%d", s.serverId, s.term)

	// Update commitIndex
	for {
		N := s.commitIndex + 1
		if int64(len(s.log)) <= N {
			break
		}

		match_count := 1
		for i := range s.ipList {
			if s.matchIndex[i] >= N {
				match_count += 1
			}
		}

		if match_count > len(s.ipList)/2 && s.log[N].Term == s.term {
			s.commitIndex = N
		} else {
			break
		}
	}

	// Send heartbeat
	approval_chan := make(chan *AppendEntryOutput, len(s.ipList)-1)
	return_count := 1

	for i := range s.ipList {
		if i != int(s.serverId) {
			go s.ApproveEntry(i, approval_chan)
		}
	}

	for {
		<-approval_chan
		return_count += 1

		if return_count >= len(s.ipList) {
			return &Success{Flag: true}, nil
		}
	}
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
	s.isCrashedMutex.RLock()
	defer s.isCrashedMutex.RUnlock()
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
