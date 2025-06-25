default: test

gen:
	go mod tidy
	go generate ./...
	goimports -local=github.com/dynoinc/starflow -w .

lint: gen
	go vet ./...
	staticcheck ./...
	govulncheck ./...

test: lint
	go mod verify
	go build ./...
	go test -v -race ./...

example:
	@echo "Extracting Go code from README.md and running it..."
	@perl -0777 -ne 'print "$1\n" if /```go\s*\n(.*?)```/sm' README.md > /tmp/starflow_example.go
	@rm -f /tmp/go.mod /tmp/go.sum
	@cd /tmp && go mod init starflow_example
	@cd /tmp && go get github.com/dynoinc/starflow
	@cd /tmp && go run starflow_example.go
	@echo "Example completed successfully!"
