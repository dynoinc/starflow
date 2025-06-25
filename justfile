set shell := ["bash", "-cu"]

default: test

gen:
	@echo "🔧 Generating code for all modules..."
	@for dir in $(find . -name "go.mod" -type f -exec dirname {} \;); do \
		echo "📁 Processing module: $$dir"; \
		(cd "$$dir" && go mod tidy && go generate ./... && goimports -local=github.com/dynoinc/starflow -w .); \
	done
	@echo "✅ Code generation completed for all modules"

lint: gen
	@echo "🔍 Linting all modules..."
	@for dir in $(find . -name "go.mod" -type f -exec dirname {} \;); do \
		echo "📁 Linting module: $$dir"; \
		(cd "$$dir" && go vet ./... && staticcheck ./... && govulncheck ./...); \
	done
	@echo "✅ Linting completed for all modules"

test: lint
	@echo "🧪 Testing all modules..."
	@for dir in $(find . -name "go.mod" -type f -exec dirname {} \;); do \
		echo "📁 Testing module: $$dir"; \
		(cd "$$dir" && go mod verify && go build ./... && go test -v -race ./...); \
	done
	@echo "✅ Testing completed for all modules"