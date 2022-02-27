package surfstore

import (
	"bufio"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc"
)

func LoadRaftConfigFile(filename string) (ipList []string) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	serverCount := 0

	for index := 0; ; index++ {
		lineContent, _, e := configReader.ReadLine()
		if e != nil && e != io.EOF {
			log.Fatal("Error During Reading Config", e)
		}

		if e == io.EOF {
			return
		}

		lineString := string(lineContent)
		splitRes := strings.Split(lineString, ": ")
		if index == 0 {
			serverCount, _ = strconv.Atoi(splitRes[1])
			ipList = make([]string, serverCount, serverCount)
		} else {
			ipList[index-1] = splitRes[1]
		}
	}

	return
}

func NewRaftServer(id int64, ips []string, blockStoreAddr string) (*RaftSurfstore, error) {
	isLeaderMutex := sync.RWMutex{}
	isCrashedMutex := sync.RWMutex{}

	server := RaftSurfstore{
		// TODO: initialize any fields you add here
		// Server Info
		ip:          ips[id],
		ipList:      ips,
		serverId:    id,
		surfClients: make([]*RaftSurfstoreClient, len(ips)),

		// General
		term:      0,
		metaStore: NewMetaStore(blockStoreAddr),
		log:       make([]*UpdateOperation, 0),

		// Log
		commitIndex: -1,
		lastApplied: -1,

		// Leader
		isLeader:      false,
		nextIndex:     make([]int64, len(ips)),
		matchIndex:    make([]int64, len(ips)),
		isLeaderMutex: isLeaderMutex,
		isLeaderCond:  sync.NewCond(&isLeaderMutex),

		// Chaos Monkey
		isCrashed:      false,
		isCrashedMutex: isCrashedMutex,
		notCrashedCond: sync.NewCond(&isCrashedMutex),
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	s := grpc.NewServer()
	RegisterRaftSurfstoreServer(s, server)

	l, e := net.Listen("tcp", server.ip)
	if e != nil {
		return e
	}

	// Create connections to other nodes
	for i, addr := range server.ipList {
		for {
			if int64(i) == server.serverId {
				continue
			}

			_, raft_surfstore_client, _, _, err := StartRaftSurfstoreClient(addr)
			if err != nil {
				continue
			}

			server.surfClients[i] = &raft_surfstore_client
		}
	}

	// go s.commitWorker()

	return s.Serve(l)
}
