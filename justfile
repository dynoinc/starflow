default: test

gen:
	# Generate code for all modules
	just gen-root
	just gen-backend-dynamodb
	just gen-backend-postgres

gen-root:
	go mod tidy
	go generate ./...
	goimports -local=github.com/dynoinc/starflow -w .

gen-backend-dynamodb:
	cd backends/dynamodb
	go mod tidy
	go generate ./...
	goimports -local=github.com/dynoinc/starflow -w .
	cd ../..

gen-backend-postgres:
	cd backends/postgres
	go mod tidy
	go generate ./...
	goimports -local=github.com/dynoinc/starflow -w .
	cd ../..

lint: gen
	# Lint all modules
	just lint-root
	just lint-backend-dynamodb
	just lint-backend-postgres

lint-root:
	go vet ./...
	staticcheck ./...
	govulncheck

lint-backend-dynamodb:
	cd backends/dynamodb
	go vet ./...
	staticcheck ./...
	govulncheck
	cd ../..

lint-backend-postgres:
	cd backends/postgres
	go vet ./...
	staticcheck ./...
	govulncheck
	cd ../..

test: lint
	# Test all modules
	just test-root
	just test-backend-dynamodb
	just test-backend-postgres

test-root:
	go mod verify
	go build ./...
	go test -v -race ./...

test-backend-dynamodb:
	cd backends/dynamodb
	go mod verify
	go build ./...
	go test -v -race ./...
	cd ../..

test-backend-postgres:
	cd backends/postgres
	go mod verify
	go build ./...
	go test -v -race ./...
	cd ../.. 