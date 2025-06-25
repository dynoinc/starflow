default: test

gen:
	#!/usr/bin/env bash
	set -e
	set -x

	for dir in $(find . -name "go.mod" -type f -exec dirname {} \;); do
		(cd "$dir" && go mod tidy && go generate ./... && goimports -local=github.com/dynoinc/starflow -w .)
	done

lint: gen
	#!/usr/bin/env bash
	set -e
	set -x
	
	for dir in $(find . -name "go.mod" -type f -exec dirname {} \;); do
		(cd "$dir" && go vet ./... && staticcheck ./... && govulncheck ./...)
	done

test: lint
	#!/usr/bin/env bash
	set -e
	set -x

	for dir in $(find . -name "go.mod" -type f -exec dirname {} \;); do
		(cd "$dir" && go mod verify && go build ./... && go test -v -race ./...)
	done