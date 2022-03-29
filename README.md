# SurfStore
File sync service based on UCSD CSE224 Winter 2022 project spec by Professor George Porter. Written in [Go](https://go.dev/) using [gRPC](https://grpc.io/) and [RAFT](https://raft.github.io/).

## Instructions

Run BlockStore server:
```console
$ make run-blockstore
```

Run RaftSurfstore servers on separate terminals:
```console
$ make IDX=0 run-raft
$ make IDX=1 run-raft
$ make IDX=2 run-raft
```

Create file and sync dataA:
```console
$ mkdir dataA
$ touch testA.txt dataA/ 
$ go run cmd/SurfstoreClientExec/main.go localhost:8081 dataA 4096
```

Sync dataB and check file:
```console
$ mkdir dataB
$ go run cmd/SurfstoreClientExec/main.go localhost:8081 dataB 4096
$ ls dataB
```
