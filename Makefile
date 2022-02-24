.PHONY: install
install:
	rm -rf bin
	GOBIN=/Users/anshuman/Google\ Drive/Documents/Masters/Classes/2022\ Winter/CSE\ 224/Homework/project5/bin go install ./...

.PHONY: run-blockstore
run-blockstore:
	go run cmd/SurfstoreRaftServerExec/main.go -s block -p 8081 -l

.PHONY: run-raft
run-raft:
	go run cmd/SurfstoreRaftServerExec/main.go -b localhost:8081 -f example_config.txt -i $(IDX)

.PHONY: test
test:
	rm -rf test/_bin
	GOBIN=/Users/anshuman/Google\ Drive/Documents/Masters/Classes/2022\ Winter/CSE\ 224/Homework/project5/test/_bin go install ./...
	go test -v ./test/...

.PHONY: specific-test
specific-test:
	rm -rf test/_bin
	GOBIN=/Users/anshuman/Google\ Drive/Documents/Masters/Classes/2022\ Winter/CSE\ 224/Homework/project5/test/_bin go install ./...
	go test -v -run $(TEST_REGEX) -count=1 ./test/...

.PHONY: clean
clean:
	rm -rf bin/ test/_bin
