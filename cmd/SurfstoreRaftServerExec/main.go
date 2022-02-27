package main

import (
	"cse224/proj5/pkg/surfstore"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"strconv"
	"strings"

	"google.golang.org/grpc"
)

func main() {
	service := flag.String("s", "meta", "(default='meta') Service Type of the Server: meta, block, both")

	// RaftServer
	serverId := flag.Int64("i", -1, "(default=-1) Server ID")
	configFile := flag.String("f", "", "(default='') Config file, absolute path")
	blockStoreAddr := flag.String("b", "", "(default='') BlockStore address")

	// Blockstore
	port := flag.Int("p", 8080, "(default = 8080) Port to accept connections")
	localOnly := flag.Bool("l", false, "Only listen on localhost")

	// Logging
	debug := flag.Bool("d", false, "Output log statements")
	flag.Parse()

	// Disable log outputs if debug flag is missing
	if !(*debug) {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	// Valid service type argument
	service_str := strings.ToLower(*service)
	if service_str != "meta" && service_str != "block" {
		flag.Usage()
		log.Fatal("service type not recognized")
	}

	if service_str == "meta" {
		addrs := surfstore.LoadRaftConfigFile(*configFile)

		log.Fatal(startRaftServer(*serverId, addrs, *blockStoreAddr))
	}

	if service_str == "block" {
		// Add localhost if necessary
		addr := ""
		if *localOnly {
			addr += "localhost"
		}
		addr += ":" + strconv.Itoa(*port)

		log.Fatal(startBlockServer(addr))
	}
}

func startRaftServer(id int64, addrs []string, blockStoreAddr string) error {
	raftServer, err := surfstore.NewRaftServer(id, addrs, blockStoreAddr)
	if err != nil {
		log.Fatal("Error creating servers")
	}

	return surfstore.ServeRaftServer(raftServer)
}

func startBlockServer(hostAddr string) error {
	// Create a new RPC server
	grpcServer := grpc.NewServer()

	// Register RPC services
	blockStore := surfstore.NewBlockStore()
	surfstore.RegisterBlockStoreServer(grpcServer, blockStore)

	// Start listening and serving
	listener, err := net.Listen("tcp", hostAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	if err := grpcServer.Serve(listener); err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}
	return nil
}
